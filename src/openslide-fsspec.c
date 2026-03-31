/*
 *  OpenSlide, a library for reading whole slide image files
 *
 *  Copyright (c) 2024
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
 * fsspec-based remote file I/O for OpenSlide.
 *
 * This is a separate backend from openslide-http.c, using Python's fsspec
 * via fsspec_cpp for file I/O. It supports:
 *   - Cloud storage: s3://, gs://, az://
 *   - HTTP/HTTPS: http://, https:// (when OPENSLIDE_USE_FSSPEC=1)
 *
 * Key design principles (following fsspec):
 *   - seek() is pure memory operation, no network I/O
 *   - read() delegates to fsspec which handles block caching
 *   - Connection pooling managed by aiohttp/boto3/etc.
 */

#include <config.h>
#include "openslide-fsspec.h"
#include "openslide-private.h"

#include <string.h>
#include <inttypes.h>

/* fsspec_cpp C API - controlled by HAVE_FSSPEC_CPP from config.h */
#ifdef HAVE_FSSPEC_CPP
#include <fsspec_cpp/fsspec_c.h>
#define FSSPEC_ENABLED 1
#else
#define FSSPEC_ENABLED 0
/* Stub declarations */
typedef void fsspec_file_t;
typedef struct { int64_t size; } fsspec_stat_t;
static inline int fsspec_init(void) { return -1; }
static inline void fsspec_cleanup(void) {}
static inline const char* fsspec_last_error(void) { return "fsspec not compiled"; }
static inline int fsspec_stat(const char* url, fsspec_stat_t* st) { (void)url; (void)st; return -1; }
static inline fsspec_file_t* fsspec_open(const char* url, const char* mode) { (void)url; (void)mode; return NULL; }
static inline int fsspec_file_close(fsspec_file_t* f) { (void)f; return -1; }
static inline size_t fsspec_file_read(fsspec_file_t* f, void* buf, size_t size) { (void)f; (void)buf; (void)size; return 0; }
static inline int64_t fsspec_file_seek(fsspec_file_t* f, int64_t off, int whence) { (void)f; (void)off; (void)whence; return -1; }
#endif

/* Debug logging */
#define FSSPEC_DEBUG 1
#if FSSPEC_DEBUG
#define FSSPEC_LOG(...) fprintf(stderr, __VA_ARGS__)
#else
#define FSSPEC_LOG(...) ((void)0)
#endif

/* ==============================
 * Global State
 * ============================== */

static gboolean g_initialized = FALSE;
static GMutex g_init_mutex;

/* Statistics */
static volatile gint g_stat_opens = 0;
static volatile gint g_stat_reads = 0;
static volatile gint g_stat_seeks = 0;
static volatile gint g_stat_bytes_kb = 0;

/* ==============================
 * File Handle
 * ============================== */

struct _openslide_fsspec_file {
#if FSSPEC_ENABLED
  fsspec_file_t *handle;
#else
  void *handle;
#endif
  char *uri;
  int64_t size;
  int64_t position;  /* Tracked in memory, seek() does not touch network */
};

/* ==============================
 * Initialization
 * ============================== */

static bool ensure_initialized(void) {
#if FSSPEC_ENABLED
  g_mutex_lock(&g_init_mutex);
  if (!g_initialized) {
    if (fsspec_init() == 0) {
      g_initialized = TRUE;
      FSSPEC_LOG("[FSSPEC] Python/fsspec initialized\n");
    } else {
      FSSPEC_LOG("[FSSPEC] Init failed: %s\n", fsspec_last_error());
    }
  }
  g_mutex_unlock(&g_init_mutex);
  return g_initialized;
#else
  return false;
#endif
}

/* ==============================
 * Availability Check
 * ============================== */

bool _openslide_fsspec_available(void) {
#if FSSPEC_ENABLED
  return ensure_initialized();
#else
  return false;
#endif
}

/* ==============================
 * Path Detection
 * ============================== */

bool _openslide_is_fsspec_path(const char *path) {
  if (!path) return false;
  
  /* Cloud storage protocols - always use fsspec */
  if (g_str_has_prefix(path, "s3://") ||
      g_str_has_prefix(path, "gs://") ||
      g_str_has_prefix(path, "gcs://") ||
      g_str_has_prefix(path, "az://") ||
      g_str_has_prefix(path, "abfs://") ||
      g_str_has_prefix(path, "abfss://")) {
    return true;
  }
  
  /* HTTP/HTTPS - only use fsspec if explicitly requested */
  if (g_str_has_prefix(path, "http://") ||
      g_str_has_prefix(path, "https://")) {
    const char *use_fsspec = g_getenv("OPENSLIDE_USE_FSSPEC");
    if (use_fsspec && (g_ascii_strcasecmp(use_fsspec, "1") == 0 ||
                       g_ascii_strcasecmp(use_fsspec, "true") == 0 ||
                       g_ascii_strcasecmp(use_fsspec, "yes") == 0)) {
      return true;
    }
  }
  
  return false;
}

/* ==============================
 * Open
 * ============================== */

struct _openslide_fsspec_file *_openslide_fsspec_open(const char *uri, GError **err) {
#if FSSPEC_ENABLED
  if (!ensure_initialized()) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "fsspec backend not available: %s", fsspec_last_error());
    return NULL;
  }
  
  FSSPEC_LOG("[FSSPEC] Opening: %s\n", uri);
  
  /* Get file info */
  fsspec_stat_t st;
  if (fsspec_stat(uri, &st) != 0) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "fsspec stat failed for %s: %s", uri, fsspec_last_error());
    return NULL;
  }
  
  /* Open file */
  fsspec_file_t *h = fsspec_open(uri, "rb");
  if (!h) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "fsspec open failed for %s: %s", uri, fsspec_last_error());
    return NULL;
  }
  
  struct _openslide_fsspec_file *file = g_new0(struct _openslide_fsspec_file, 1);
  file->handle = h;
  file->uri = g_strdup(uri);
  file->size = st.size;
  file->position = 0;
  
  g_atomic_int_inc(&g_stat_opens);
  FSSPEC_LOG("[FSSPEC] Opened %s (size=%" PRId64 ")\n", uri, file->size);
  
  return file;
#else
  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "fsspec support not compiled in");
  return NULL;
#endif
}

/* ==============================
 * Close
 * ============================== */

void _openslide_fsspec_close(struct _openslide_fsspec_file *file) {
  if (!file) return;
  
#if FSSPEC_ENABLED
  if (file->handle) {
    fsspec_file_close(file->handle);
  }
#endif
  
  FSSPEC_LOG("[FSSPEC] Closed %s\n", file->uri);
  g_free(file->uri);
  g_free(file);
}

/* ==============================
 * Seek - PURE MEMORY OPERATION
 * ============================== */

bool _openslide_fsspec_seek(struct _openslide_fsspec_file *file,
                            int64_t offset, int whence, GError **err) {
  if (!file) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED, "NULL file");
    return false;
  }
  
  int64_t new_pos;
  switch (whence) {
    case SEEK_SET:
      new_pos = offset;
      break;
    case SEEK_CUR:
      new_pos = file->position + offset;
      break;
    case SEEK_END:
      new_pos = file->size + offset;
      break;
    default:
      g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                  "Invalid whence: %d", whence);
      return false;
  }
  
  if (new_pos < 0) new_pos = 0;
  if (new_pos > file->size) new_pos = file->size;
  
  /* PURE MEMORY OPERATION - no network I/O */
  file->position = new_pos;
  g_atomic_int_inc(&g_stat_seeks);
  
  return true;
}

/* ==============================
 * Tell
 * ============================== */

int64_t _openslide_fsspec_tell(struct _openslide_fsspec_file *file) {
  return file ? file->position : -1;
}

/* ==============================
 * Size
 * ============================== */

int64_t _openslide_fsspec_size(struct _openslide_fsspec_file *file, GError **err) {
  if (!file) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED, "NULL file");
    return -1;
  }
  return file->size;
}

/* ==============================
 * Read - delegates to fsspec (handles caching)
 * ============================== */

size_t _openslide_fsspec_read(struct _openslide_fsspec_file *file,
                              void *buf, size_t size, GError **err) {
  if (!file || !buf || size == 0) return 0;
  
#if FSSPEC_ENABLED
  if (file->position >= file->size) {
    return 0;  /* EOF */
  }
  
  /* Clamp to remaining size */
  size_t to_read = size;
  if (file->position + to_read > (uint64_t)file->size) {
    to_read = (size_t)(file->size - file->position);
  }
  
  /* Seek to position - fsspec will use cache if available */
  if (fsspec_file_seek(file->handle, file->position, SEEK_SET) < 0) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "fsspec seek failed: %s", fsspec_last_error());
    return 0;
  }
  
  /* Read - fsspec handles caching internally */
  size_t nread = fsspec_file_read(file->handle, buf, to_read);
  if (nread == 0 && to_read > 0) {
    const char *errmsg = fsspec_last_error();
    if (errmsg && *errmsg) {
      g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                  "fsspec read failed: %s", errmsg);
    }
    return 0;
  }
  
  file->position += nread;
  g_atomic_int_inc(&g_stat_reads);
  g_atomic_int_add(&g_stat_bytes_kb, (gint)(nread / 1024));
  
  return nread;
#else
  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED, "fsspec not available");
  return 0;
#endif
}

bool _openslide_fsspec_read_exact(struct _openslide_fsspec_file *file,
                                  void *buf, size_t size, GError **err) {
  GError *tmp_err = NULL;
  size_t nread = _openslide_fsspec_read(file, buf, size, &tmp_err);
  
  if (tmp_err) {
    g_propagate_error(err, tmp_err);
    return false;
  }
  
  if (nread < size) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Short read from %s: %zu < %zu", 
                file ? file->uri : "(null)", nread, size);
    return false;
  }
  
  return true;
}

/* ==============================
 * Zero-copy (if fsspec supports it)
 * ============================== */

bool _openslide_fsspec_pread_ptr(struct _openslide_fsspec_file *file,
                                 uint64_t offset,
                                 const uint8_t **out_ptr,
                                 size_t *out_avail,
                                 GError **err G_GNUC_UNUSED) {
  /* fsspec_cpp may not support zero-copy, return false to fall back */
  (void)file;
  (void)offset;
  *out_ptr = NULL;
  *out_avail = 0;
  return false;
}

/* ==============================
 * Statistics
 * ============================== */

void _openslide_fsspec_print_stats(void) {
  fprintf(stderr, "[FSSPEC-STATS] opens=%d reads=%d seeks=%d bytes=%dKB\n",
          g_atomic_int_get(&g_stat_opens),
          g_atomic_int_get(&g_stat_reads),
          g_atomic_int_get(&g_stat_seeks),
          g_atomic_int_get(&g_stat_bytes_kb));
}
