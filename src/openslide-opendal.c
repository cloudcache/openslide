/*
 *  OpenSlide, a library for reading whole slide image files
 *
 *  Copyright (c) 2007-2024 Carnegie Mellon University
 *  All rights reserved.
 *
 *  OpenSlide is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Lesser General Public License as
 *  published by the Free Software Foundation, version 2.1.
 *
 *  OpenSlide is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with OpenSlide. If not, see
 *  <http://www.gnu.org/licenses/>.
 *
 */

/*
 * OpenSlide remote file access via Apache OpenDAL
 *
 * This implementation uses OpenDAL's C binding to provide unified access to:
 * - Cloud storage: S3, GCS, Azure Blob, etc.
 * - HTTP/HTTPS URLs
 * - Local files (as fallback)
 *
 * Key design principles (following fsspec/parquet.net patterns):
 * 1. seek() is pure memory - only updates position, no I/O
 * 2. Block-aligned caching with extent-based prefetch
 * 3. Single operator instance per URI scheme for connection reuse
 * 4. Zero-copy reads via pread_ptr() when data is in cache
 */

#include "config.h"
#include "openslide-opendal.h"
#include "openslide-private.h"

#include <string.h>
#include <stdlib.h>
#include <inttypes.h>

/* OpenDAL C binding - controlled by HAVE_OPENDAL from config.h */
#ifdef HAVE_OPENDAL
#include <opendal.h>
#define OPENDAL_ENABLED 1
#else
#define OPENDAL_ENABLED 0
#endif

/* ==============================
 * Configuration
 * ============================== */

#define OPENDAL_BLOCK_SIZE        (256 * 1024)       /* 256KB blocks */
#define OPENDAL_MAX_CACHE_BLOCKS  256                /* 64MB max cache */
#define OPENDAL_FRONT_EXTENT_SIZE (8 * 1024 * 1024)  /* 8MB front prefetch */
#define OPENDAL_TAIL_EXTENT_SIZE  (4 * 1024 * 1024)  /* 4MB tail prefetch */

/* ==============================
 * Data Structures
 * ============================== */

/* Extent buffer - contiguous cached region */
typedef struct {
  uint8_t *data;
  uint64_t offset;      /* Start offset in file */
  size_t len;           /* Length of cached data */
  volatile gint ref_count;
} OpendalExtent;

/* Block entry pointing into an extent */
typedef struct {
  OpendalExtent *extent;  /* Owning extent (ref counted) */
  uint8_t *data;          /* Pointer into extent->data */
  size_t len;
  uint64_t block_idx;
  /* LRU links */
  gpointer lru_prev;
  gpointer lru_next;
} OpendalBlock;

/* File handle */
struct _openslide_opendal_file {
#if OPENDAL_ENABLED
  opendal_operator *op;      /* OpenDAL operator (shared per scheme) */
#else
  void *op;                  /* Placeholder */
#endif
  char *uri;                 /* Original URI */
  char *path;                /* Path within the storage (after scheme://) */
  char *scheme;              /* Storage scheme (s3, gs, http, etc.) */
  
  int64_t position;          /* Current seek position */
  int64_t file_size;         /* Total file size (-1 if unknown) */
  
  /* Extent cache */
  OpendalExtent *front_extent;  /* Cached front of file */
  OpendalExtent *tail_extent;   /* Cached tail of file */
  
  /* Block cache with LRU */
  GHashTable *block_map;     /* block_idx -> OpendalBlock* */
  OpendalBlock *lru_head;    /* Most recently used */
  OpendalBlock *lru_tail;    /* Least recently used */
  guint cache_count;
  
  GMutex mutex;
};

/* Global operator cache - one operator per scheme for connection reuse */
static GHashTable *g_operator_cache = NULL;
static GMutex g_operator_mutex;
static gboolean g_opendal_initialized = FALSE;

/* ==============================
 * Stub implementations when OpenDAL not available
 * ============================== */

#if !OPENDAL_ENABLED

bool _openslide_opendal_available(void) {
  return false;
}

bool _openslide_is_opendal_path(const char *path) {
  (void)path;
  return false;
}

struct _openslide_opendal_file *_openslide_opendal_open(const char *uri,
                                                         GError **err) {
  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "OpenDAL support not compiled in");
  (void)uri;
  return NULL;
}

void _openslide_opendal_close(struct _openslide_opendal_file *file) {
  (void)file;
}

size_t _openslide_opendal_read(struct _openslide_opendal_file *file,
                                void *buf, size_t size, GError **err) {
  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "OpenDAL support not compiled in");
  (void)file; (void)buf; (void)size;
  return 0;
}

bool _openslide_opendal_read_exact(struct _openslide_opendal_file *file,
                                    void *buf, size_t size, GError **err) {
  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "OpenDAL support not compiled in");
  (void)file; (void)buf; (void)size;
  return false;
}

bool _openslide_opendal_seek(struct _openslide_opendal_file *file,
                              int64_t offset, int whence, GError **err) {
  (void)file; (void)offset; (void)whence; (void)err;
  return false;
}

int64_t _openslide_opendal_tell(struct _openslide_opendal_file *file) {
  (void)file;
  return -1;
}

int64_t _openslide_opendal_size(struct _openslide_opendal_file *file,
                                 GError **err) {
  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "OpenDAL support not compiled in");
  (void)file;
  return -1;
}

bool _openslide_opendal_pread_ptr(struct _openslide_opendal_file *file,
                                   uint64_t offset,
                                   const uint8_t **out_ptr,
                                   size_t *out_avail,
                                   GError **err) {
  (void)file; (void)offset; (void)out_ptr; (void)out_avail; (void)err;
  return false;
}

#else /* OPENDAL_ENABLED */

/* ==============================
 * Helper Functions
 * ============================== */

static void extent_unref(OpendalExtent *ext) {
  if (!ext) return;
  if (g_atomic_int_dec_and_test(&ext->ref_count)) {
    g_free(ext->data);
    g_free(ext);
  }
}

static OpendalExtent *extent_ref(OpendalExtent *ext) {
  if (ext) {
    g_atomic_int_inc(&ext->ref_count);
  }
  return ext;
}

static void block_free(OpendalBlock *b) {
  if (!b) return;
  extent_unref(b->extent);
  g_free(b);
}

/* Parse URI into scheme and path */
static bool parse_uri(const char *uri, char **scheme, char **path) {
  const char *colon = strchr(uri, ':');
  if (!colon || colon[1] != '/' || colon[2] != '/') {
    return false;
  }
  
  size_t scheme_len = colon - uri;
  *scheme = g_strndup(uri, scheme_len);
  *path = g_strdup(colon + 3);  /* Skip :// */
  
  return true;
}

/* Get or create operator for a scheme */
static opendal_operator *get_operator(const char *scheme, const char *uri,
                                       GError **err) {
  g_mutex_lock(&g_operator_mutex);
  
  if (!g_opendal_initialized) {
    g_operator_cache = g_hash_table_new_full(g_str_hash, g_str_equal,
                                              g_free, NULL);
    g_opendal_initialized = TRUE;
  }
  
  opendal_operator *op = g_hash_table_lookup(g_operator_cache, scheme);
  if (op) {
    g_mutex_unlock(&g_operator_mutex);
    return op;
  }
  
  /* Create new operator based on scheme */
  opendal_operator_options *options = opendal_operator_options_new();
  
  if (g_strcmp0(scheme, "s3") == 0) {
    /* S3: extract bucket from path, use env vars for credentials */
    const char *bucket_end = strchr(uri + strlen("s3://"), '/');
    if (bucket_end) {
      char *bucket = g_strndup(uri + strlen("s3://"), bucket_end - (uri + strlen("s3://")));
      opendal_operator_options_set(options, "bucket", bucket);
      g_free(bucket);
    }
    /* AWS credentials from environment */
    const char *region = g_getenv("AWS_REGION");
    if (region) {
      opendal_operator_options_set(options, "region", region);
    }
  } else if (g_strcmp0(scheme, "gs") == 0 || g_strcmp0(scheme, "gcs") == 0) {
    /* Google Cloud Storage */
    const char *bucket_end = strchr(uri + strlen("gs://"), '/');
    if (bucket_end) {
      char *bucket = g_strndup(uri + strlen("gs://"), bucket_end - (uri + strlen("gs://")));
      opendal_operator_options_set(options, "bucket", bucket);
      g_free(bucket);
    }
  } else if (g_strcmp0(scheme, "az") == 0 || g_strcmp0(scheme, "azure") == 0) {
    /* Azure Blob Storage */
    /* Container and account from environment or URI */
  } else if (g_strcmp0(scheme, "http") == 0 || g_strcmp0(scheme, "https") == 0) {
    /* HTTP/HTTPS - extract endpoint */
    opendal_operator_options_set(options, "endpoint", uri);
  }
  
  opendal_result_operator_new result = opendal_operator_new(scheme, options);
  opendal_operator_options_free(options);
  
  if (result.error) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Failed to create OpenDAL operator for %s: %.*s",
                scheme, (int)result.error->message.len,
                (char*)result.error->message.data);
    opendal_error_free(result.error);
    g_mutex_unlock(&g_operator_mutex);
    return NULL;
  }
  
  g_hash_table_insert(g_operator_cache, g_strdup(scheme), result.op);
  g_mutex_unlock(&g_operator_mutex);
  
  return result.op;
}

/* Fetch data range from remote */
static OpendalExtent *fetch_extent(struct _openslide_opendal_file *file,
                                    uint64_t offset, size_t len,
                                    GError **err) {
  /* Use opendal_operator_read for now - could optimize with range reads */
  opendal_result_read result = opendal_operator_read(file->op, file->path);
  
  if (result.error) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "OpenDAL read failed: %.*s",
                (int)result.error->message.len,
                (char*)result.error->message.data);
    opendal_error_free(result.error);
    return NULL;
  }
  
  /* For now, cache the entire file (OpenDAL doesn't have range read in C binding yet) */
  OpendalExtent *ext = g_new0(OpendalExtent, 1);
  ext->data = g_malloc(result.data.len);
  memcpy(ext->data, result.data.data, result.data.len);
  ext->offset = 0;
  ext->len = result.data.len;
  g_atomic_int_set(&ext->ref_count, 1);
  
  opendal_bytes_free(&result.data);
  
  return ext;
}

/* Check if extent contains the requested range */
static bool extent_contains(OpendalExtent *ext, uint64_t offset, size_t size) {
  if (!ext || !ext->data) return false;
  return offset >= ext->offset && 
         offset + size <= ext->offset + ext->len;
}

/* Get pointer into extent if range is covered */
static bool extent_get_ptr(OpendalExtent *ext, uint64_t offset,
                           const uint8_t **out_ptr, size_t *out_avail) {
  if (!ext || !ext->data || offset < ext->offset) {
    return false;
  }
  uint64_t end = ext->offset + ext->len;
  if (offset >= end) {
    return false;
  }
  size_t off_in_ext = (size_t)(offset - ext->offset);
  *out_ptr = ext->data + off_in_ext;
  *out_avail = ext->len - off_in_ext;
  return true;
}

/* ==============================
 * Public API
 * ============================== */

bool _openslide_opendal_available(void) {
  return true;
}

bool _openslide_is_opendal_path(const char *path) {
  if (!path) return false;
  
  /* Cloud storage schemes */
  if (g_str_has_prefix(path, "s3://")) return true;
  if (g_str_has_prefix(path, "gs://")) return true;
  if (g_str_has_prefix(path, "gcs://")) return true;
  if (g_str_has_prefix(path, "az://")) return true;
  if (g_str_has_prefix(path, "azure://")) return true;
  if (g_str_has_prefix(path, "abfs://")) return true;
  if (g_str_has_prefix(path, "oss://")) return true;  /* Aliyun OSS */
  if (g_str_has_prefix(path, "cos://")) return true;  /* Tencent COS */
  
  /* HTTP with env var override */
  if ((g_str_has_prefix(path, "http://") || g_str_has_prefix(path, "https://")) &&
      g_getenv("OPENSLIDE_USE_OPENDAL")) {
    return true;
  }
  
  return false;
}

struct _openslide_opendal_file *_openslide_opendal_open(const char *uri,
                                                         GError **err) {
  char *scheme = NULL;
  char *path = NULL;
  
  if (!parse_uri(uri, &scheme, &path)) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Invalid URI: %s", uri);
    return NULL;
  }
  
  opendal_operator *op = get_operator(scheme, uri, err);
  if (!op) {
    g_free(scheme);
    g_free(path);
    return NULL;
  }
  
  /* Get file metadata to determine size */
  opendal_result_stat stat_result = opendal_operator_stat(op, path);
  if (stat_result.error) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Failed to stat %s: %.*s", uri,
                (int)stat_result.error->message.len,
                (char*)stat_result.error->message.data);
    opendal_error_free(stat_result.error);
    g_free(scheme);
    g_free(path);
    return NULL;
  }
  
  int64_t file_size = opendal_metadata_content_length(stat_result.meta);
  opendal_metadata_free(stat_result.meta);
  
  /* Create file handle */
  struct _openslide_opendal_file *file = g_new0(struct _openslide_opendal_file, 1);
  file->op = op;
  file->uri = g_strdup(uri);
  file->scheme = scheme;
  file->path = path;
  file->position = 0;
  file->file_size = file_size;
  file->block_map = g_hash_table_new_full(g_direct_hash, g_direct_equal,
                                           NULL, (GDestroyNotify)block_free);
  g_mutex_init(&file->mutex);
  
  /* Prefetch front extent */
  size_t front_size = MIN((size_t)file_size, OPENDAL_FRONT_EXTENT_SIZE);
  file->front_extent = fetch_extent(file, 0, front_size, NULL);
  
  /* Prefetch tail extent if file is large enough */
  if (file_size > OPENDAL_FRONT_EXTENT_SIZE + OPENDAL_TAIL_EXTENT_SIZE) {
    uint64_t tail_offset = file_size - OPENDAL_TAIL_EXTENT_SIZE;
    file->tail_extent = fetch_extent(file, tail_offset, OPENDAL_TAIL_EXTENT_SIZE, NULL);
  }
  
  return file;
}

void _openslide_opendal_close(struct _openslide_opendal_file *file) {
  if (!file) return;
  
  g_mutex_lock(&file->mutex);
  
  extent_unref(file->front_extent);
  extent_unref(file->tail_extent);
  g_hash_table_destroy(file->block_map);
  
  g_mutex_unlock(&file->mutex);
  g_mutex_clear(&file->mutex);
  
  g_free(file->uri);
  g_free(file->scheme);
  g_free(file->path);
  g_free(file);
}

/* seek() is pure memory - no network I/O */
bool _openslide_opendal_seek(struct _openslide_opendal_file *file,
                              int64_t offset, int whence, GError **err) {
  if (!file) return false;
  
  int64_t new_pos;
  switch (whence) {
    case SEEK_SET:
      new_pos = offset;
      break;
    case SEEK_CUR:
      new_pos = file->position + offset;
      break;
    case SEEK_END:
      if (file->file_size < 0) {
        g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                    "Cannot seek from end: file size unknown");
        return false;
      }
      new_pos = file->file_size + offset;
      break;
    default:
      g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                  "Invalid whence: %d", whence);
      return false;
  }
  
  if (new_pos < 0) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Seek to negative position: %" PRId64, new_pos);
    return false;
  }
  
  file->position = new_pos;
  return true;
}

int64_t _openslide_opendal_tell(struct _openslide_opendal_file *file) {
  return file ? file->position : -1;
}

int64_t _openslide_opendal_size(struct _openslide_opendal_file *file,
                                 GError **err) {
  if (!file) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "NULL file handle");
    return -1;
  }
  return file->file_size;
}

size_t _openslide_opendal_read(struct _openslide_opendal_file *file,
                                void *buf, size_t size, GError **err) {
  if (!file || !buf || size == 0) return 0;
  
  g_mutex_lock(&file->mutex);
  
  uint64_t offset = (uint64_t)file->position;
  size_t to_read = size;
  
  /* Clamp to file size */
  if (file->file_size >= 0 && offset + to_read > (uint64_t)file->file_size) {
    if (offset >= (uint64_t)file->file_size) {
      g_mutex_unlock(&file->mutex);
      return 0;  /* EOF */
    }
    to_read = (size_t)(file->file_size - offset);
  }
  
  /* Try front extent first */
  const uint8_t *ptr = NULL;
  size_t avail = 0;
  if (extent_get_ptr(file->front_extent, offset, &ptr, &avail) && avail >= to_read) {
    memcpy(buf, ptr, to_read);
    file->position += to_read;
    g_mutex_unlock(&file->mutex);
    return to_read;
  }
  
  /* Try tail extent */
  if (extent_get_ptr(file->tail_extent, offset, &ptr, &avail) && avail >= to_read) {
    memcpy(buf, ptr, to_read);
    file->position += to_read;
    g_mutex_unlock(&file->mutex);
    return to_read;
  }
  
  g_mutex_unlock(&file->mutex);
  
  /* Need to fetch from remote - for now, re-fetch front extent which has all data */
  /* TODO: Implement proper range-based block fetching */
  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "Read beyond cached extents not yet implemented");
  return 0;
}

bool _openslide_opendal_read_exact(struct _openslide_opendal_file *file,
                                    void *buf, size_t size, GError **err) {
  size_t read = _openslide_opendal_read(file, buf, size, err);
  if (read != size && !*err) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Short read: got %zu, expected %zu", read, size);
    return false;
  }
  return read == size;
}

bool _openslide_opendal_pread_ptr(struct _openslide_opendal_file *file,
                                   uint64_t offset,
                                   const uint8_t **out_ptr,
                                   size_t *out_avail,
                                   GError **err) {
  if (!file || !out_ptr || !out_avail) return false;
  
  g_mutex_lock(&file->mutex);
  
  /* Try front extent */
  if (extent_get_ptr(file->front_extent, offset, out_ptr, out_avail)) {
    g_mutex_unlock(&file->mutex);
    return true;
  }
  
  /* Try tail extent */
  if (extent_get_ptr(file->tail_extent, offset, out_ptr, out_avail)) {
    g_mutex_unlock(&file->mutex);
    return true;
  }
  
  g_mutex_unlock(&file->mutex);
  
  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "Offset %" PRIu64 " not in cached extents", offset);
  return false;
}

#endif /* OPENDAL_ENABLED */
