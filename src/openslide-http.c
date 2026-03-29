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
#include <sys/time.h>

/* ==============================
 * Statistics Tracking
 * ============================== */

typedef struct {
  gint64 total_requests;
  gint64 total_bytes;
  gint64 total_time_us;       /* Microseconds */
  gint64 cache_hits;
  gint64 cache_misses;
  gint64 connection_reuses;
  gint64 new_connections;
} HttpStats;

static HttpStats global_stats = {0};
static GMutex stats_mutex;
static gboolean stats_mutex_initialized = FALSE;

static void ensure_stats_mutex_init(void) {
  static gsize init_once = 0;
  if (g_once_init_enter(&init_once)) {
    g_mutex_init(&stats_mutex);
    stats_mutex_initialized = TRUE;
    g_once_init_leave(&init_once, 1);
  }
}

static void stats_record_request(size_t bytes, gint64 time_us, gboolean reused) {
  ensure_stats_mutex_init();
  g_mutex_lock(&stats_mutex);
  global_stats.total_requests++;
  global_stats.total_bytes += bytes;
  global_stats.total_time_us += time_us;
  if (reused) {
    global_stats.connection_reuses++;
  } else {
    global_stats.new_connections++;
  }
  g_mutex_unlock(&stats_mutex);
}

static void stats_record_cache(gboolean hit) {
  ensure_stats_mutex_init();
  g_mutex_lock(&stats_mutex);
  if (hit) {
    global_stats.cache_hits++;
  } else {
    global_stats.cache_misses++;
  }
  g_mutex_unlock(&stats_mutex);
}

/* ==============================
 * Global Configuration (thread-safe)
 * ============================== */

static GMutex config_mutex;
static gboolean config_mutex_initialized = FALSE;

static OpenslideHTTPConfig http_config = {
  .block_size = 512 * 1024,        /* 512KB blocks - balance between request count and data size */
  .max_cache_blocks = 128,         /* 64MB max cache */
  .retry_max = 3,
  .retry_delay_ms = 100,
  .connect_timeout_ms = 10000,
  .transfer_timeout_ms = 30000,
  .low_speed_limit = 1024,
  .low_speed_time = 10,
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

/* CURL handle pool entry */
typedef struct {
  CURL *curl;
  gboolean in_use;
  gint64 last_used;
} CurlHandle;

#define MAX_CURL_HANDLES 16

/* Shared connection info - cached in pool, shared by multiple file handles */
typedef struct _http_connection {
  gchar *uri;
  uint64_t file_size;

  GHashTable *block_map;  /* block_idx -> HttpBlock* */
  GList *lru_list;        /* LRU list of block indices */
  guint cache_count;

  /* CURL handle pool for connection reuse */
  CurlHandle curl_pool[MAX_CURL_HANDLES];
  GMutex curl_mutex;

  gint ref_count;         /* Number of file handles using this connection */
  gint64 last_access;
  GMutex mutex;

  /* Per-connection stats */
  gint64 range_requests;
  gint64 range_bytes;
  gint64 range_time_us;
} HttpConnection;

/* Per-open file handle - each caller gets their own with independent position */
struct _openslide_http_file {
  HttpConnection *conn;   /* Shared connection (has its own refcount) */
  int64_t position;       /* This handle's read position - NOT shared */
  
  /* Read-ahead tracking for sequential access optimization */
  int64_t last_read_end;  /* End position of last read (for detecting sequential) */
  guint64 last_block_idx; /* Last accessed block index */
  guint32 sequential_hits;/* Count of sequential reads */
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
  ensure_stats_mutex_init();

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
 * CURL Handle Pool Management
 * ============================== */

static CURL *curl_pool_acquire(HttpConnection *conn, gboolean *was_reused) {
  g_mutex_lock(&conn->curl_mutex);

  *was_reused = FALSE;
  gint64 now = g_get_monotonic_time();

  /* Try to find an idle handle */
  for (int i = 0; i < MAX_CURL_HANDLES; i++) {
    if (conn->curl_pool[i].curl && !conn->curl_pool[i].in_use) {
      conn->curl_pool[i].in_use = TRUE;
      conn->curl_pool[i].last_used = now;
      *was_reused = TRUE;
      g_mutex_unlock(&conn->curl_mutex);
      return conn->curl_pool[i].curl;
    }
  }

  /* No idle handle, try to create a new one in an empty slot */
  for (int i = 0; i < MAX_CURL_HANDLES; i++) {
    if (!conn->curl_pool[i].curl) {
      CURL *curl = curl_easy_init();
      if (curl) {
        conn->curl_pool[i].curl = curl;
        conn->curl_pool[i].in_use = TRUE;
        conn->curl_pool[i].last_used = now;
        g_mutex_unlock(&conn->curl_mutex);
        return curl;
      }
    }
  }

  g_mutex_unlock(&conn->curl_mutex);

  /* Pool is full and all handles are in use - create a temporary one */
  return curl_easy_init();
}

static void curl_pool_release(HttpConnection *conn, CURL *curl) {
  g_mutex_lock(&conn->curl_mutex);

  for (int i = 0; i < MAX_CURL_HANDLES; i++) {
    if (conn->curl_pool[i].curl == curl) {
      conn->curl_pool[i].in_use = FALSE;
      conn->curl_pool[i].last_used = g_get_monotonic_time();
      g_mutex_unlock(&conn->curl_mutex);
      return;
    }
  }

  g_mutex_unlock(&conn->curl_mutex);

  /* This was a temporary handle, clean it up */
  curl_easy_cleanup(curl);
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
  curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT, 300L);
  
  /* Enable connection reuse */
  curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
  curl_easy_setopt(curl, CURLOPT_FRESH_CONNECT, 0L);
  curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
  
  /* Try HTTP/2 for multiplexing if available */
  curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
  
  /* Enable TCP Fast Open if supported */
#ifdef CURLOPT_TCP_FASTOPEN
  curl_easy_setopt(curl, CURLOPT_TCP_FASTOPEN, 1L);
#endif
  
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
 * Range Fetch with Retry and Stats
 * ============================== */

static gint64 get_time_us(void) {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (gint64)tv.tv_sec * 1000000 + tv.tv_usec;
}

static bool http_fetch_range(HttpConnection *conn,
                             uint64_t offset, size_t len,
                             uint8_t *dst, size_t *out_written,
                             GError **err) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();
  *out_written = 0;

  WriteCtx ctx = {.dst = dst, .remain = len, .written = 0};
  gint64 start_time = get_time_us();

  for (int retry = 0; retry <= (int)cfg->retry_max; retry++) {
    gboolean was_reused = FALSE;
    CURL *curl = curl_pool_acquire(conn, &was_reused);
    if (!curl) {
      g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                  "Failed to acquire CURL handle");
      return false;
    }

    /* Reset handle for new request but keep connection alive */
    curl_easy_reset(curl);

    char range_header[128];
    snprintf(range_header, sizeof(range_header),
             "Range: bytes=%" PRIu64 "-%" PRIu64,
             offset + ctx.written,
             offset + len - 1);

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, range_header);

    http_curl_setup_common(curl, conn->uri);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, http_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ctx);

    gint64 req_start = get_time_us();
    CURLcode code = curl_easy_perform(curl);
    gint64 req_time = get_time_us() - req_start;

    long http_code = 0;
    double total_time = 0, connect_time = 0, namelookup_time = 0;
    if (code == CURLE_OK) {
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
      curl_easy_getinfo(curl, CURLINFO_TOTAL_TIME, &total_time);
      curl_easy_getinfo(curl, CURLINFO_CONNECT_TIME, &connect_time);
      curl_easy_getinfo(curl, CURLINFO_NAMELOOKUP_TIME, &namelookup_time);
    }

    /* Detailed logging */
    fprintf(stderr, "[HTTP-RANGE] %s | offset=%" PRIu64 " len=%zu | "
            "reused=%d retry=%d code=%d http=%ld | "
            "dns=%.1fms conn=%.1fms total=%.1fms written=%zu\n",
            conn->uri + (strlen(conn->uri) > 60 ? strlen(conn->uri) - 60 : 0),
            offset, len, was_reused, retry, code, http_code,
            namelookup_time * 1000, connect_time * 1000, total_time * 1000,
            ctx.written);

    curl_slist_free_all(headers);
    curl_pool_release(conn, curl);

    if (code == CURLE_OK && (http_code == 200 || http_code == 206)) {
      gint64 elapsed = get_time_us() - start_time;
      
      /* Update stats */
      stats_record_request(ctx.written, elapsed, was_reused);
      
      g_mutex_lock(&conn->mutex);
      conn->range_requests++;
      conn->range_bytes += ctx.written;
      conn->range_time_us += elapsed;
      g_mutex_unlock(&conn->mutex);

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

/* Fetch multiple consecutive blocks in a single request (like fsspec) */
static bool http_fetch_blocks_range(HttpConnection *conn,
                                    guint64 start_block, guint64 end_block,
                                    GError **err) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();
  
  uint64_t start_offset = start_block * cfg->block_size;
  uint64_t end_offset = (end_block + 1) * cfg->block_size;
  
  if (end_offset > conn->file_size) {
    end_offset = conn->file_size;
  }
  
  size_t total_len = (size_t)(end_offset - start_offset);
  if (total_len == 0) {
    return true;
  }
  
  uint8_t *buffer = g_malloc(total_len);
  size_t written = 0;
  
  if (!http_fetch_range(conn, start_offset, total_len, buffer, &written, err)) {
    g_free(buffer);
    return false;
  }
  
  /* Split into individual blocks and cache them */
  g_mutex_lock(&conn->mutex);
  
  for (guint64 idx = start_block; idx <= end_block; idx++) {
    /* Check if already cached (another thread may have added it) */
    if (g_hash_table_lookup(conn->block_map, GUINT_TO_POINTER(idx))) {
      continue;
    }
    
    uint64_t block_start = idx * cfg->block_size - start_offset;
    uint64_t block_end = (idx + 1) * cfg->block_size - start_offset;
    if (block_end > written) {
      block_end = written;
    }
    if (block_start >= written) {
      break;
    }
    
    size_t block_len = (size_t)(block_end - block_start);
    
    HttpBlock *b = g_new0(HttpBlock, 1);
    b->data = g_malloc(block_len);
    b->len = block_len;
    memcpy(b->data, buffer + block_start, block_len);
    
    /* Evict if needed */
    while (conn->cache_count >= cfg->max_cache_blocks) {
      lru_evict(conn);
    }
    
    g_hash_table_insert(conn->block_map, GUINT_TO_POINTER(idx), b);
    conn->lru_list = g_list_prepend(conn->lru_list, GUINT_TO_POINTER(idx));
    conn->cache_count++;
  }
  
  g_mutex_unlock(&conn->mutex);
  g_free(buffer);
  return true;
}

static HttpBlock *http_get_block(HttpConnection *conn,
                                 guint64 block_idx, GError **err) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();

  g_mutex_lock(&conn->mutex);

  HttpBlock *b = g_hash_table_lookup(conn->block_map, GUINT_TO_POINTER(block_idx));
  if (b) {
    lru_promote(conn, block_idx);
    stats_record_cache(TRUE);
    g_mutex_unlock(&conn->mutex);
    return b;
  }

  stats_record_cache(FALSE);

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

  /* Print connection stats before cleanup */
  fprintf(stderr, "[HTTP-STATS] Connection %s | requests=%" PRId64 
          " bytes=%" PRId64 " time=%" PRId64 "ms avg=%.2fms/req\n",
          conn->uri,
          conn->range_requests,
          conn->range_bytes,
          conn->range_time_us / 1000,
          conn->range_requests > 0 ? 
            (double)conn->range_time_us / conn->range_requests / 1000.0 : 0);

  /* Cleanup CURL handles */
  g_mutex_lock(&conn->curl_mutex);
  for (int i = 0; i < MAX_CURL_HANDLES; i++) {
    if (conn->curl_pool[i].curl) {
      curl_easy_cleanup(conn->curl_pool[i].curl);
      conn->curl_pool[i].curl = NULL;
    }
  }
  g_mutex_unlock(&conn->curl_mutex);
  g_mutex_clear(&conn->curl_mutex);

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
    fprintf(stderr, "[HTTP-POOL] Reusing connection: %s (refs=%d)\n",
            uri, g_atomic_int_get(&conn->ref_count));
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

  gint64 head_start = get_time_us();
  CURLcode code = curl_easy_perform(curl);
  gint64 head_time = get_time_us() - head_start;

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
  conn->range_requests = 0;
  conn->range_bytes = 0;
  conn->range_time_us = 0;
  g_mutex_init(&conn->mutex);
  g_mutex_init(&conn->curl_mutex);
  memset(conn->curl_pool, 0, sizeof(conn->curl_pool));

  g_hash_table_insert(conn_pool, g_strdup(uri), conn);

  fprintf(stderr, "[HTTP-POOL] New connection: %s | size=%" PRIu64 " | HEAD took %.1fms\n",
          uri, conn->file_size, head_time / 1000.0);

  g_mutex_unlock(&pool_mutex);
  return conn;
}

/* ==============================
 * File Handle Operations
 * ============================== */

struct _openslide_http_file *_openslide_http_open(const char *uri,
                                                   GError **err) {
  fprintf(stderr, "[HTTP-OPEN] Opening: %s\n", uri);
  
  HttpConnection *conn = http_connection_get_or_create(uri, err);
  if (!conn) {
    fprintf(stderr, "[HTTP-OPEN] FAILED: %s\n", uri);
    return NULL;
  }

  /* Create new file handle with its own position */
  struct _openslide_http_file *file = g_new0(struct _openslide_http_file, 1);
  file->conn = conn;
  file->position = 0;
  file->last_read_end = -1;
  file->last_block_idx = G_MAXUINT64;
  file->sequential_hits = 0;

  fprintf(stderr, "[HTTP-OPEN] SUCCESS: %s (size=%" PRIu64 ", handle=%p, pos=0)\n", 
          uri, conn->file_size, (void*)file);
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

  /* Detect sequential access pattern (like fsspec ReadAheadCache) */
  gboolean is_sequential = (file->last_read_end >= 0 && pos == file->last_read_end);
  if (is_sequential) {
    file->sequential_hits++;
  } else {
    file->sequential_hits = 0;
  }

  uint64_t avail = file_size - (uint64_t)pos;
  size_t to_read = MIN(size, (size_t)avail);
  size_t total = 0;

  /* Calculate block range needed */
  guint64 start_block = (uint64_t)pos / cfg->block_size;
  guint64 end_block = (uint64_t)(pos + (int64_t)to_read - 1) / cfg->block_size;
  
  /* Read-ahead: if sequential pattern detected, prefetch next blocks */
  guint64 prefetch_blocks = 0;
  if (file->sequential_hits >= 2) {
    /* Prefetch up to 4 blocks ahead for sequential reads */
    prefetch_blocks = MIN(4, (file_size / cfg->block_size) - end_block);
  }

  /* Check which blocks are missing and need to be fetched */
  g_mutex_lock(&conn->mutex);
  guint64 fetch_start = G_MAXUINT64;
  guint64 fetch_end = 0;
  
  for (guint64 idx = start_block; idx <= end_block + prefetch_blocks; idx++) {
    if (!g_hash_table_lookup(conn->block_map, GUINT_TO_POINTER(idx))) {
      if (fetch_start == G_MAXUINT64) {
        fetch_start = idx;
      }
      fetch_end = idx;
    }
  }
  g_mutex_unlock(&conn->mutex);
  
  /* Batch fetch missing blocks in a single request */
  if (fetch_start != G_MAXUINT64) {
    if (!http_fetch_blocks_range(conn, fetch_start, fetch_end, err)) {
      /* Fall back to individual block fetching on error */
    }
  }

  /* Now read from cache */
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
    
    file->last_block_idx = block_idx;
  }

  file->position += (int64_t)total;
  file->last_read_end = file->position;

  /* Debug first few reads */
  static int dbg_count = 0;
  if (dbg_count < 5 && total > 0) {
    dbg_count++;
    uint8_t *p = (uint8_t *)buf;
    fprintf(stderr, "[HTTP-READ] #%d: pos=%" PRId64 " size=%zu read=%zu magic=%02x%02x%02x%02x newpos=%" PRId64 "\n",
            dbg_count, pos, size, total, 
            total >= 4 ? p[0] : 0, total >= 4 ? p[1] : 0,
            total >= 4 ? p[2] : 0, total >= 4 ? p[3] : 0,
            file->position);
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

/* ==============================
 * Global Stats API
 * ============================== */

void _openslide_http_print_stats(void) {
  ensure_stats_mutex_init();
  g_mutex_lock(&stats_mutex);
  
  fprintf(stderr, "\n[HTTP-GLOBAL-STATS]\n");
  fprintf(stderr, "  Total requests: %" PRId64 "\n", global_stats.total_requests);
  fprintf(stderr, "  Total bytes: %" PRId64 " (%.2f MB)\n", 
          global_stats.total_bytes, global_stats.total_bytes / 1048576.0);
  fprintf(stderr, "  Total time: %" PRId64 " ms\n", global_stats.total_time_us / 1000);
  fprintf(stderr, "  Avg time/request: %.2f ms\n",
          global_stats.total_requests > 0 ? 
            (double)global_stats.total_time_us / global_stats.total_requests / 1000.0 : 0);
  fprintf(stderr, "  Cache hits: %" PRId64 " (%.1f%%)\n", 
          global_stats.cache_hits,
          (global_stats.cache_hits + global_stats.cache_misses) > 0 ?
            100.0 * global_stats.cache_hits / (global_stats.cache_hits + global_stats.cache_misses) : 0);
  fprintf(stderr, "  Cache misses: %" PRId64 "\n", global_stats.cache_misses);
  fprintf(stderr, "  Connection reuses: %" PRId64 " (%.1f%%)\n",
          global_stats.connection_reuses,
          (global_stats.connection_reuses + global_stats.new_connections) > 0 ?
            100.0 * global_stats.connection_reuses / 
              (global_stats.connection_reuses + global_stats.new_connections) : 0);
  fprintf(stderr, "  New connections: %" PRId64 "\n", global_stats.new_connections);
  fprintf(stderr, "\n");

  g_mutex_unlock(&stats_mutex);
}
