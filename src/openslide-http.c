/*
 *  OpenSlide, a library for reading whole slide image files
 *
 *  Copyright (c) 2024 OpenSlide project contributors
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

#include <config.h>

#include "openslide-http.h"
#include "openslide-private.h"

#include <curl/curl.h>
#include <errno.h>
#include <glib.h>
#include <inttypes.h>
#include <string.h>

/* ==============================
 * Global Configuration (thread-safe)
 * ============================== */

static GMutex config_mutex;
static gboolean config_mutex_initialized = FALSE;

static OpenslideHTTPConfig http_config = {
  .block_size = 256 * 1024,
  .max_cache_blocks = 256,
  .retry_max = 3,
  .retry_delay_ms = 200,
  .connect_timeout_ms = 5000,
  .transfer_timeout_ms = 15000,
  .low_speed_limit = 10240,
  .low_speed_time = 5,
  .pool_ttl_sec = 300,
};

static void ensure_config_mutex_init(void) {
  static gsize init_once = 0;
  if (g_once_init_enter(&init_once)) {
    g_mutex_init(&config_mutex);
    config_mutex_initialized = TRUE;
    g_once_init_leave(&init_once, 1);
  }
}

void _openslide_http_set_config(const OpenslideHTTPConfig *cfg) {
  ensure_config_mutex_init();
  g_mutex_lock(&config_mutex);
  if (cfg) {
    http_config = *cfg;
  }
  g_mutex_unlock(&config_mutex);
}

const OpenslideHTTPConfig *_openslide_http_get_config(void) {
  return &http_config;
}

/* ==============================
 * Internal Data Structures
 * ============================== */

typedef struct {
  uint8_t *data;
  size_t len;
} HttpBlock;

struct _openslide_http_file {
  CURL *curl;
  gchar *uri;
  uint64_t file_size;
  int64_t position;       /* current read position */

  GHashTable *block_map;  /* block_idx -> HttpBlock* */
  GList *lru_list;        /* LRU list of block indices */
  guint cache_count;

  gint ref_count;
  gint64 last_access;
  GMutex mutex;
};

typedef struct {
  uint8_t *dst;
  size_t remain;
  size_t written;
} WriteCtx;

/* ==============================
 * Global Connection Pool
 * ============================== */

static GMutex pool_mutex;
static gboolean pool_mutex_initialized = FALSE;
static GHashTable *file_pool = NULL;  /* uri -> _openslide_http_file* */
static CURLSH *curl_share = NULL;
static gboolean http_initialized = FALSE;

/* CURL share lock mutexes - properly initialized array */
static GMutex curl_lock_mutexes[CURL_LOCK_DATA_LAST];
static gboolean curl_locks_initialized = FALSE;

static void ensure_pool_mutex_init(void) {
  static gsize init_once = 0;
  if (g_once_init_enter(&init_once)) {
    g_mutex_init(&pool_mutex);
    pool_mutex_initialized = TRUE;
    g_once_init_leave(&init_once, 1);
  }
}

static void ensure_curl_locks_init(void) {
  static gsize init_once = 0;
  if (g_once_init_enter(&init_once)) {
    for (int i = 0; i < CURL_LOCK_DATA_LAST; i++) {
      g_mutex_init(&curl_lock_mutexes[i]);
    }
    curl_locks_initialized = TRUE;
    g_once_init_leave(&init_once, 1);
  }
}

static void curl_lock_cb(CURL *handle G_GNUC_UNUSED,
                         curl_lock_data data,
                         curl_lock_access access G_GNUC_UNUSED,
                         void *userptr G_GNUC_UNUSED) {
  if (data < CURL_LOCK_DATA_LAST) {
    g_mutex_lock(&curl_lock_mutexes[data]);
  }
}

static void curl_unlock_cb(CURL *handle G_GNUC_UNUSED,
                           curl_lock_data data,
                           void *userptr G_GNUC_UNUSED) {
  if (data < CURL_LOCK_DATA_LAST) {
    g_mutex_unlock(&curl_lock_mutexes[data]);
  }
}

/* ==============================
 * URL Detection
 * ============================== */

bool _openslide_is_remote_path(const char *path) {
  if (path == NULL) {
    return false;
  }
  return g_str_has_prefix(path, "http://") ||
         g_str_has_prefix(path, "https://") ||
         g_str_has_prefix(path, "s3://");
}

/* ==============================
 * HTTP Subsystem Init/Cleanup
 * ============================== */

void _openslide_http_init(void) {
  ensure_pool_mutex_init();
  ensure_curl_locks_init();

  g_mutex_lock(&pool_mutex);
  if (!http_initialized) {
    curl_global_init(CURL_GLOBAL_ALL);

    curl_share = curl_share_init();
    if (curl_share) {
      curl_share_setopt(curl_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
      curl_share_setopt(curl_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_SSL_SESSION);
      curl_share_setopt(curl_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_CONNECT);
      curl_share_setopt(curl_share, CURLSHOPT_LOCKFUNC, curl_lock_cb);
      curl_share_setopt(curl_share, CURLSHOPT_UNLOCKFUNC, curl_unlock_cb);
    }

    file_pool = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
    http_initialized = TRUE;
  }
  g_mutex_unlock(&pool_mutex);
}

void _openslide_http_cleanup(void) {
  ensure_pool_mutex_init();

  g_mutex_lock(&pool_mutex);
  if (http_initialized) {
    if (file_pool) {
      /* Close all remaining files */
      GList *values = g_hash_table_get_values(file_pool);
      for (GList *l = values; l; l = l->next) {
        struct _openslide_http_file *f = l->data;
        /* Force cleanup regardless of ref count */
        f->ref_count = 1;
      }
      g_list_free(values);
      g_hash_table_destroy(file_pool);
      file_pool = NULL;
    }

    if (curl_share) {
      curl_share_cleanup(curl_share);
      curl_share = NULL;
    }

    curl_global_cleanup();
    http_initialized = FALSE;
  }
  g_mutex_unlock(&pool_mutex);
}

/* ==============================
 * CURL Setup Helper
 * ============================== */

static void http_curl_setup_common(CURL *curl, const char *uri) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();

  curl_easy_setopt(curl, CURLOPT_URL, uri);
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
  curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT, 60L);
  
  /* Disable Accept-Encoding to ensure we get raw uncompressed data.
     This is important for Range requests on binary files. */
  curl_easy_setopt(curl, CURLOPT_ACCEPT_ENCODING, "identity");
  /* Also disable HTTP/2 server push which can cause issues */
  curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);

  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, (long)cfg->connect_timeout_ms);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)cfg->transfer_timeout_ms);
  curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, (long)cfg->low_speed_limit);
  curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, (long)cfg->low_speed_time);

  if (curl_share) {
    curl_easy_setopt(curl, CURLOPT_SHARE, curl_share);
  }
}

/* ==============================
 * Write Callback (bounds-checked)
 * ============================== */

static size_t http_write_callback(void *data, size_t sz, size_t nmemb,
                                  void *user) {
  WriteCtx *ctx = (WriteCtx *)user;
  size_t total = sz * nmemb;

  /* If we have no more room, still return the full size to indicate success
     to CURL (otherwise it treats it as an error). We just won't copy data. */
  if (ctx->remain == 0) {
    return total;
  }

  size_t to_copy = total;
  if (to_copy > ctx->remain) {
    to_copy = ctx->remain;
  }

  memcpy(ctx->dst, data, to_copy);
  ctx->dst += to_copy;
  ctx->written += to_copy;
  ctx->remain -= to_copy;

  return total;  /* Always return total to signal success to CURL */
}

/* ==============================
 * Range Fetch with Retry
 * ============================== */

static bool http_fetch_range(struct _openslide_http_file *f,
                             uint64_t offset, size_t len,
                             uint8_t *dst, size_t *out_written,
                             GError **err) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();
  *out_written = 0;

  WriteCtx ctx = {.dst = dst, .remain = len, .written = 0};

  for (int retry = 0; retry <= (int)cfg->retry_max; retry++) {
    g_autofree gchar *range = g_strdup_printf(
        "bytes=%" PRIu64 "-%" PRIu64,
        offset + ctx.written,
        offset + len - 1);

    fprintf(stderr, "[HTTP-FETCH] Range: %s\n", range);

    curl_easy_reset(f->curl);
    http_curl_setup_common(f->curl, f->uri);
    curl_easy_setopt(f->curl, CURLOPT_RANGE, range);
    curl_easy_setopt(f->curl, CURLOPT_WRITEFUNCTION, http_write_callback);
    curl_easy_setopt(f->curl, CURLOPT_WRITEDATA, &ctx);

    CURLcode code = curl_easy_perform(f->curl);
    fprintf(stderr, "[HTTP-FETCH] CURL code=%d, written=%zu\n", code, ctx.written);
    
    if (code == CURLE_OK) {
      long http_code = 0;
      curl_easy_getinfo(f->curl, CURLINFO_RESPONSE_CODE, &http_code);
      fprintf(stderr, "[HTTP-FETCH] HTTP code=%ld\n", http_code);
      /* Accept 200 (full content) and 206 (partial content) */
      if (http_code == 200 || http_code == 206) {
        *out_written = ctx.written;
        return true;
      }
    }

    /* Check if error is retryable */
    gboolean retryable = (code == CURLE_COULDNT_CONNECT ||
                          code == CURLE_OPERATION_TIMEDOUT ||
                          code == CURLE_RECV_ERROR ||
                          code == CURLE_SEND_ERROR ||
                          code == CURLE_GOT_NOTHING ||
                          code == CURLE_PARTIAL_FILE);
    if (!retryable) {
      break;
    }

    if (retry < cfg->retry_max) {
      g_usleep((gulong)cfg->retry_delay_ms * 1000 * (1u << retry));
    }
  }

  if (ctx.written > 0) {
    *out_written = ctx.written;
    return true;
  }

  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "HTTP range request failed for %s", f->uri);
  return false;
}

/* ==============================
 * LRU Cache Management
 * ============================== */

static void lru_promote(struct _openslide_http_file *f, guint64 idx) {
  /* Remove from current position and add to front */
  f->lru_list = g_list_remove(f->lru_list, GUINT_TO_POINTER(idx));
  f->lru_list = g_list_prepend(f->lru_list, GUINT_TO_POINTER(idx));
}

static void lru_evict(struct _openslide_http_file *f) {
  if (f->cache_count == 0 || f->lru_list == NULL) {
    return;
  }

  GList *last = g_list_last(f->lru_list);
  if (!last) {
    return;
  }

  guint64 last_idx = GPOINTER_TO_UINT(last->data);
  f->lru_list = g_list_delete_link(f->lru_list, last);

  HttpBlock *b = g_hash_table_lookup(f->block_map, GUINT_TO_POINTER(last_idx));
  if (b) {
    g_free(b->data);
    g_free(b);
  }
  g_hash_table_remove(f->block_map, GUINT_TO_POINTER(last_idx));
  f->cache_count--;
}

/* ==============================
 * Block Fetch
 * ============================== */

static HttpBlock *http_get_block(struct _openslide_http_file *f,
                                 guint64 block_idx, GError **err) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();

  g_mutex_lock(&f->mutex);

  /* Check cache first */
  HttpBlock *b = g_hash_table_lookup(f->block_map, GUINT_TO_POINTER(block_idx));
  if (b) {
    lru_promote(f, block_idx);
    g_mutex_unlock(&f->mutex);
    return b;
  }

  /* Evict if necessary */
  while (f->cache_count >= cfg->max_cache_blocks) {
    lru_evict(f);
  }

  /* Calculate range */
  uint64_t offset = block_idx * cfg->block_size;
  size_t len = cfg->block_size;

  if (offset >= f->file_size) {
    g_mutex_unlock(&f->mutex);
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Block offset %" PRIu64 " beyond file size %" PRIu64,
                offset, f->file_size);
    return NULL;
  }
  if (offset + len > f->file_size) {
    len = (size_t)(f->file_size - offset);
  }

  /* Allocate block */
  b = g_new0(HttpBlock, 1);
  b->data = g_malloc(len);
  b->len = len;

  /* Fetch data (release lock during network I/O) */
  g_mutex_unlock(&f->mutex);

  size_t written;
  if (!http_fetch_range(f, offset, len, b->data, &written, err) ||
      written == 0) {
    g_free(b->data);
    g_free(b);
    return NULL;
  }

  /* Re-acquire lock and insert into cache */
  g_mutex_lock(&f->mutex);

  /* Check if another thread already cached this block */
  HttpBlock *existing = g_hash_table_lookup(f->block_map,
                                            GUINT_TO_POINTER(block_idx));
  if (existing) {
    g_free(b->data);
    g_free(b);
    lru_promote(f, block_idx);
    g_mutex_unlock(&f->mutex);
    return existing;
  }

  b->len = written;
  g_hash_table_insert(f->block_map, GUINT_TO_POINTER(block_idx), b);
  f->lru_list = g_list_prepend(f->lru_list, GUINT_TO_POINTER(block_idx));
  f->cache_count++;

  g_mutex_unlock(&f->mutex);
  return b;
}

/* ==============================
 * Pool Cleanup (TTL expiration)
 * ============================== */

static void pool_cleanup_expired(void) {
  /* Must be called with pool_mutex held */
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();
  gint64 now = g_get_monotonic_time() / G_USEC_PER_SEC;

  /* Build list of expired URIs (don't modify hash table while iterating) */
  GPtrArray *expired = g_ptr_array_new();

  GHashTableIter iter;
  gpointer key, value;
  g_hash_table_iter_init(&iter, file_pool);
  while (g_hash_table_iter_next(&iter, &key, &value)) {
    struct _openslide_http_file *f = value;
    /* Only expire if ref_count is 1 (only pool holds reference) */
    if (g_atomic_int_get(&f->ref_count) == 1 &&
        now - f->last_access > cfg->pool_ttl_sec) {
      g_ptr_array_add(expired, g_strdup(key));
    }
  }

  /* Remove expired entries */
  for (guint i = 0; i < expired->len; i++) {
    const gchar *uri = g_ptr_array_index(expired, i);
    struct _openslide_http_file *f = g_hash_table_lookup(file_pool, uri);
    if (f && g_atomic_int_get(&f->ref_count) == 1) {
      g_hash_table_remove(file_pool, uri);
      /* Actual cleanup happens when ref drops to 0 */
    }
  }

  g_ptr_array_free(expired, TRUE);
}

/* ==============================
 * File Operations
 * ============================== */

static void http_file_destroy(struct _openslide_http_file *f) {
  if (!f) {
    return;
  }

  /* Free all cached blocks */
  if (f->block_map) {
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, f->block_map);
    while (g_hash_table_iter_next(&iter, &key, &value)) {
      HttpBlock *b = value;
      g_free(b->data);
      g_free(b);
    }
    g_hash_table_destroy(f->block_map);
  }

  g_list_free(f->lru_list);
  g_free(f->uri);

  if (f->curl) {
    curl_easy_cleanup(f->curl);
  }

  g_mutex_clear(&f->mutex);
  g_free(f);
}

struct _openslide_http_file *_openslide_http_open(const char *uri,
                                                   GError **err) {
  _openslide_http_init();

  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();

  g_mutex_lock(&pool_mutex);

  /* Cleanup expired entries */
  pool_cleanup_expired();

  /* Check if already in pool */
  struct _openslide_http_file *f = g_hash_table_lookup(file_pool, uri);
  if (f) {
    g_atomic_int_inc(&f->ref_count);
    f->last_access = g_get_monotonic_time() / G_USEC_PER_SEC;
    g_mutex_unlock(&pool_mutex);
    return f;
  }

  /* Create new CURL handle */
  CURL *curl = curl_easy_init();
  if (!curl) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Failed to initialize CURL");
    g_mutex_unlock(&pool_mutex);
    return NULL;
  }

  /* Get file size with HEAD request */
  curl_easy_setopt(curl, CURLOPT_URL, uri);
  curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS,
                   (long)cfg->connect_timeout_ms);
  if (curl_share) {
    curl_easy_setopt(curl, CURLOPT_SHARE, curl_share);
  }

  CURLcode code = curl_easy_perform(curl);
  if (code != CURLE_OK) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "HEAD request failed for %s: %s", uri,
                curl_easy_strerror(code));
    curl_easy_cleanup(curl);
    g_mutex_unlock(&pool_mutex);
    return NULL;
  }

  long http_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
  if (http_code != 200) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "HEAD request returned HTTP %ld for %s", http_code, uri);
    curl_easy_cleanup(curl);
    g_mutex_unlock(&pool_mutex);
    return NULL;
  }

  /* Use CURLINFO_CONTENT_LENGTH_DOWNLOAD (double) for compatibility with
     older libcurl versions. CURLINFO_CONTENT_LENGTH_DOWNLOAD_T requires
     libcurl >= 7.55.0 */
  double cl_double = -1;
  curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl_double);
  if (cl_double <= 0) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Cannot determine file size for %s", uri);
    curl_easy_cleanup(curl);
    g_mutex_unlock(&pool_mutex);
    return NULL;
  }

  /* Create file handle */
  f = g_new0(struct _openslide_http_file, 1);
  f->uri = g_strdup(uri);
  f->curl = curl;
  f->file_size = (uint64_t)cl_double;
  f->position = 0;
  f->block_map = g_hash_table_new(g_direct_hash, g_direct_equal);
  f->lru_list = NULL;
  f->cache_count = 0;
  f->ref_count = 1;
  f->last_access = g_get_monotonic_time() / G_USEC_PER_SEC;
  g_mutex_init(&f->mutex);

  /* Add to pool */
  g_hash_table_insert(file_pool, g_strdup(uri), f);

  g_mutex_unlock(&pool_mutex);
  return f;
}

size_t _openslide_http_read(struct _openslide_http_file *file,
                            void *buf, size_t size, GError **err) {
  if (!file || !buf || size == 0) {
    return 0;
  }

  uint8_t *dst = (uint8_t *)buf;
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();

  g_mutex_lock(&file->mutex);
  int64_t pos = file->position;
  uint64_t file_size = file->file_size;
  g_mutex_unlock(&file->mutex);

  if ((uint64_t)pos >= file_size) {
    return 0;  /* EOF */
  }

  /* Clamp to available data */
  uint64_t avail = file_size - (uint64_t)pos;
  size_t to_read = MIN(size, (size_t)avail);
  size_t total = 0;

  while (total < to_read) {
    guint64 block_idx = (uint64_t)(pos + (int64_t)total) / cfg->block_size;
    uint32_t block_off = (uint64_t)(pos + (int64_t)total) % cfg->block_size;

    HttpBlock *b = http_get_block(file, block_idx, err);
    if (!b) {
      break;
    }

    if (block_off >= b->len) {
      break;  /* Shouldn't happen, but be safe */
    }

    size_t block_avail = b->len - block_off;
    size_t copy_len = MIN(to_read - total, block_avail);

    memcpy(dst + total, b->data + block_off, copy_len);
    total += copy_len;
  }

  /* Update position */
  g_mutex_lock(&file->mutex);
  file->position += (int64_t)total;
  file->last_access = g_get_monotonic_time() / G_USEC_PER_SEC;
  g_mutex_unlock(&file->mutex);

  return total;
}

bool _openslide_http_read_exact(struct _openslide_http_file *file,
                                void *buf, size_t size, GError **err) {
  GError *tmp_err = NULL;
  size_t count = _openslide_http_read(file, buf, size, &tmp_err);

  if (tmp_err) {
    g_propagate_error(err, tmp_err);
    return false;
  }
  if (count < size) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Short read from %s: %" PRIu64 " < %" PRIu64,
                file->uri, (uint64_t)count, (uint64_t)size);
    return false;
  }
  return true;
}

bool _openslide_http_seek(struct _openslide_http_file *file,
                          int64_t offset, int whence, GError **err) {
  if (!file) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "NULL file handle");
    return false;
  }

  g_mutex_lock(&file->mutex);

  int64_t new_pos;
  switch (whence) {
    case SEEK_SET:
      new_pos = offset;
      break;
    case SEEK_CUR:
      new_pos = file->position + offset;
      break;
    case SEEK_END:
      new_pos = (int64_t)file->file_size + offset;
      break;
    default:
      g_mutex_unlock(&file->mutex);
      g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                  "Invalid whence value: %d", whence);
      return false;
  }

  if (new_pos < 0) {
    g_mutex_unlock(&file->mutex);
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Seek to negative position: %" PRId64, new_pos);
    return false;
  }

  file->position = new_pos;
  g_mutex_unlock(&file->mutex);
  return true;
}

int64_t _openslide_http_tell(struct _openslide_http_file *file,
                             GError **err G_GNUC_UNUSED) {
  if (!file) {
    return -1;
  }

  g_mutex_lock(&file->mutex);
  int64_t pos = file->position;
  g_mutex_unlock(&file->mutex);

  return pos;
}

int64_t _openslide_http_size(struct _openslide_http_file *file,
                             GError **err G_GNUC_UNUSED) {
  if (!file) {
    return -1;
  }
  return (int64_t)file->file_size;
}

void _openslide_http_close(struct _openslide_http_file *file) {
  if (!file) {
    return;
  }

  ensure_pool_mutex_init();

  g_mutex_lock(&pool_mutex);

  if (g_atomic_int_dec_and_test(&file->ref_count)) {
    /* Last reference - remove from pool and destroy */
    if (file_pool) {
      g_hash_table_remove(file_pool, file->uri);
    }
    g_mutex_unlock(&pool_mutex);
    http_file_destroy(file);
  } else {
    /* Still has references */
    g_mutex_unlock(&pool_mutex);
  }
}
