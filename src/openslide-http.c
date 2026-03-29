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

/* Shared connection info - cached in pool, shared by multiple file handles */
typedef struct _http_connection {
  gchar *uri;
  uint64_t file_size;

  GHashTable *block_map;  /* block_idx -> HttpBlock* */
  GList *lru_list;        /* LRU list of block indices */
  guint cache_count;

  gint ref_count;         /* Number of file handles using this connection */
  gint64 last_access;
  GMutex mutex;
} HttpConnection;

/* Per-open file handle - each caller gets their own with independent position */
struct _openslide_http_file {
  HttpConnection *conn;   /* Shared connection (has its own refcount) */
  int64_t position;       /* This handle's read position - NOT shared */
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
static GHashTable *conn_pool = NULL;  /* uri -> HttpConnection* */
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

    conn_pool = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
    http_initialized = TRUE;
  }
  g_mutex_unlock(&pool_mutex);
}

void _openslide_http_cleanup(void) {
  ensure_pool_mutex_init();

  g_mutex_lock(&pool_mutex);
  if (http_initialized) {
    if (conn_pool) {
      g_hash_table_destroy(conn_pool);
      conn_pool = NULL;
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
  
  /* Set a proper User-Agent - some servers require this */
  curl_easy_setopt(curl, CURLOPT_USERAGENT, "OpenSlide/4.0 libcurl");

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

  return total;
}

/* ==============================
 * Range Fetch with Retry
 * ============================== */

static bool http_fetch_range(HttpConnection *conn,
                             uint64_t offset, size_t len,
                             uint8_t *dst, size_t *out_written,
                             GError **err) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();
  *out_written = 0;

  WriteCtx ctx = {.dst = dst, .remain = len, .written = 0};

  for (int retry = 0; retry <= (int)cfg->retry_max; retry++) {
    CURL *curl = curl_easy_init();
    if (!curl) {
      g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                  "Failed to create CURL handle");
      return false;
    }

    char range_header[128];
    snprintf(range_header, sizeof(range_header),
             "Range: bytes=%" PRIu64 "-%" PRIu64,
             offset + ctx.written,
             offset + len - 1);

    fprintf(stderr, "[HTTP-FETCH] %s\n", range_header);

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, range_header);

    http_curl_setup_common(curl, conn->uri);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, http_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ctx);

    CURLcode code = curl_easy_perform(curl);
    fprintf(stderr, "[HTTP-FETCH] CURL code=%d, written=%zu\n", code, ctx.written);

    long http_code = 0;
    if (code == CURLE_OK) {
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
      fprintf(stderr, "[HTTP-FETCH] HTTP code=%ld\n", http_code);
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (code == CURLE_OK && (http_code == 200 || http_code == 206)) {
      *out_written = ctx.written;
      return true;
    }

    gboolean retryable = (code == CURLE_COULDNT_CONNECT ||
                          code == CURLE_OPERATION_TIMEDOUT ||
                          code == CURLE_RECV_ERROR ||
                          code == CURLE_SEND_ERROR ||
                          code == CURLE_GOT_NOTHING ||
                          code == CURLE_PARTIAL_FILE);
    if (!retryable && code != CURLE_OK) {
      break;
    }

    if (retry < (int)cfg->retry_max) {
      g_usleep((gulong)cfg->retry_delay_ms * 1000 * (1u << retry));
    }
  }

  if (ctx.written > 0) {
    *out_written = ctx.written;
    return true;
  }

  g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
              "HTTP range request failed for %s", conn->uri);
  return false;
}

/* ==============================
 * LRU Cache Management
 * ============================== */

static void lru_promote(HttpConnection *conn, guint64 idx) {
  conn->lru_list = g_list_remove(conn->lru_list, GUINT_TO_POINTER(idx));
  conn->lru_list = g_list_prepend(conn->lru_list, GUINT_TO_POINTER(idx));
}

static void lru_evict(HttpConnection *conn) {
  if (conn->cache_count == 0 || conn->lru_list == NULL) {
    return;
  }

  GList *last = g_list_last(conn->lru_list);
  if (!last) {
    return;
  }

  guint64 last_idx = GPOINTER_TO_UINT(last->data);
  conn->lru_list = g_list_delete_link(conn->lru_list, last);

  HttpBlock *b = g_hash_table_lookup(conn->block_map, GUINT_TO_POINTER(last_idx));
  if (b) {
    g_free(b->data);
    g_free(b);
  }
  g_hash_table_remove(conn->block_map, GUINT_TO_POINTER(last_idx));
  conn->cache_count--;
}

/* ==============================
 * Block Fetch
 * ============================== */

static HttpBlock *http_get_block(HttpConnection *conn,
                                 guint64 block_idx, GError **err) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();

  g_mutex_lock(&conn->mutex);

  HttpBlock *b = g_hash_table_lookup(conn->block_map, GUINT_TO_POINTER(block_idx));
  if (b) {
    lru_promote(conn, block_idx);
    g_mutex_unlock(&conn->mutex);
    return b;
  }

  while (conn->cache_count >= cfg->max_cache_blocks) {
    lru_evict(conn);
  }

  uint64_t offset = block_idx * cfg->block_size;
  size_t len = cfg->block_size;

  if (offset >= conn->file_size) {
    g_mutex_unlock(&conn->mutex);
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Block offset %" PRIu64 " beyond file size %" PRIu64,
                offset, conn->file_size);
    return NULL;
  }
  if (offset + len > conn->file_size) {
    len = (size_t)(conn->file_size - offset);
  }

  b = g_new0(HttpBlock, 1);
  b->data = g_malloc(len);
  b->len = len;

  g_mutex_unlock(&conn->mutex);

  size_t written;
  if (!http_fetch_range(conn, offset, len, b->data, &written, err) ||
      written == 0) {
    g_free(b->data);
    g_free(b);
    return NULL;
  }

  g_mutex_lock(&conn->mutex);

  HttpBlock *existing = g_hash_table_lookup(conn->block_map,
                                            GUINT_TO_POINTER(block_idx));
  if (existing) {
    g_free(b->data);
    g_free(b);
    lru_promote(conn, block_idx);
    g_mutex_unlock(&conn->mutex);
    return existing;
  }

  b->len = written;
  g_hash_table_insert(conn->block_map, GUINT_TO_POINTER(block_idx), b);
  conn->lru_list = g_list_prepend(conn->lru_list, GUINT_TO_POINTER(block_idx));
  conn->cache_count++;

  g_mutex_unlock(&conn->mutex);
  return b;
}

/* ==============================
 * Connection Management
 * ============================== */

static void http_connection_destroy(HttpConnection *conn) {
  if (!conn) {
    return;
  }

  if (conn->block_map) {
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, conn->block_map);
    while (g_hash_table_iter_next(&iter, &key, &value)) {
      HttpBlock *b = value;
      g_free(b->data);
      g_free(b);
    }
    g_hash_table_destroy(conn->block_map);
  }

  g_list_free(conn->lru_list);
  g_free(conn->uri);
  g_mutex_clear(&conn->mutex);
  g_free(conn);
}

static void http_connection_unref(HttpConnection *conn) {
  if (!conn) {
    return;
  }

  if (g_atomic_int_dec_and_test(&conn->ref_count)) {
    ensure_pool_mutex_init();
    g_mutex_lock(&pool_mutex);
    if (conn_pool) {
      g_hash_table_remove(conn_pool, conn->uri);
    }
    g_mutex_unlock(&pool_mutex);
    http_connection_destroy(conn);
  }
}

static HttpConnection *http_connection_get_or_create(const char *uri,
                                                      GError **err) {
  _openslide_http_init();

  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();

  g_mutex_lock(&pool_mutex);

  HttpConnection *conn = g_hash_table_lookup(conn_pool, uri);
  if (conn) {
    g_atomic_int_inc(&conn->ref_count);
    conn->last_access = g_get_monotonic_time() / G_USEC_PER_SEC;
    g_mutex_unlock(&pool_mutex);
    return conn;
  }

  /* Get file size with HEAD request */
  CURL *curl = curl_easy_init();
  if (!curl) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Failed to initialize CURL");
    g_mutex_unlock(&pool_mutex);
    return NULL;
  }

  curl_easy_setopt(curl, CURLOPT_URL, uri);
  curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_USERAGENT, "OpenSlide/4.0 libcurl");
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

  double cl_double = -1;
  curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl_double);
  curl_easy_cleanup(curl);

  if (cl_double <= 0) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Cannot determine file size for %s", uri);
    g_mutex_unlock(&pool_mutex);
    return NULL;
  }

  conn = g_new0(HttpConnection, 1);
  conn->uri = g_strdup(uri);
  conn->file_size = (uint64_t)cl_double;
  conn->block_map = g_hash_table_new(g_direct_hash, g_direct_equal);
  conn->lru_list = NULL;
  conn->cache_count = 0;
  conn->ref_count = 1;
  conn->last_access = g_get_monotonic_time() / G_USEC_PER_SEC;
  g_mutex_init(&conn->mutex);

  g_hash_table_insert(conn_pool, g_strdup(uri), conn);

  g_mutex_unlock(&pool_mutex);
  return conn;
}

/* ==============================
 * File Handle Operations
 * ============================== */

struct _openslide_http_file *_openslide_http_open(const char *uri,
                                                   GError **err) {
  fprintf(stderr, "[OPENSLIDE-HTTP] Opening: %s\n", uri);
  
  HttpConnection *conn = http_connection_get_or_create(uri, err);
  if (!conn) {
    fprintf(stderr, "[OPENSLIDE-HTTP] FAILED to open: %s\n", uri);
    return NULL;
  }

  /* Create new file handle with its own position */
  struct _openslide_http_file *file = g_new0(struct _openslide_http_file, 1);
  file->conn = conn;
  file->position = 0;

  fprintf(stderr, "[OPENSLIDE-HTTP] SUCCESS opened: %s, size=%" PRIu64 "\n", 
          uri, conn->file_size);
  return file;
}

size_t _openslide_http_read(struct _openslide_http_file *file,
                            void *buf, size_t size, GError **err) {
  if (!file || !buf || size == 0) {
    return 0;
  }

  HttpConnection *conn = file->conn;
  uint8_t *dst = (uint8_t *)buf;
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();

  int64_t pos = file->position;
  uint64_t file_size = conn->file_size;

  if ((uint64_t)pos >= file_size) {
    return 0;
  }

  uint64_t avail = file_size - (uint64_t)pos;
  size_t to_read = MIN(size, (size_t)avail);
  size_t total = 0;

  while (total < to_read) {
    guint64 block_idx = (uint64_t)(pos + (int64_t)total) / cfg->block_size;
    uint32_t block_off = (uint64_t)(pos + (int64_t)total) % cfg->block_size;

    HttpBlock *b = http_get_block(conn, block_idx, err);
    if (!b) {
      break;
    }

    if (block_off >= b->len) {
      break;
    }

    size_t block_avail = b->len - block_off;
    size_t copy_len = MIN(to_read - total, block_avail);

    memcpy(dst + total, b->data + block_off, copy_len);
    total += copy_len;
  }

  file->position += (int64_t)total;

  /* Debug: show first bytes read */
  static int dbg_read_count = 0;
  if (dbg_read_count < 5 && total > 0) {
    dbg_read_count++;
    unsigned char *p = (unsigned char *)buf;
    fprintf(stderr, "[HTTP-READ] %zu bytes at pos %" PRId64 ", magic: %02x%02x%02x%02x\n",
            total, pos, p[0], p[1], p[2], p[3]);
  }

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
                file->conn->uri, (uint64_t)count, (uint64_t)size);
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

  int64_t new_pos;
  switch (whence) {
    case SEEK_SET:
      new_pos = offset;
      break;
    case SEEK_CUR:
      new_pos = file->position + offset;
      break;
    case SEEK_END:
      new_pos = (int64_t)file->conn->file_size + offset;
      break;
    default:
      g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                  "Invalid whence value: %d", whence);
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

int64_t _openslide_http_tell(struct _openslide_http_file *file) {
  if (!file) {
    return -1;
  }
  return file->position;
}

uint64_t _openslide_http_size(struct _openslide_http_file *file,
                              GError **err G_GNUC_UNUSED) {
  if (!file) {
    return 0;
  }
  return file->conn->file_size;
}

void _openslide_http_close(struct _openslide_http_file *file) {
  if (!file) {
    return;
  }

  http_connection_unref(file->conn);
  g_free(file);
}

const char *_openslide_http_get_uri(struct _openslide_http_file *file) {
  if (!file || !file->conn) {
    return NULL;
  }
  return file->conn->uri;
}
