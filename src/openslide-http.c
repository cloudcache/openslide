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
 * Debug Control - set to 0 for production
 * ============================== */
#define HTTP_DEBUG 0

#if HTTP_DEBUG
  #define HTTP_LOG(...) fprintf(stderr, __VA_ARGS__)
#else
  #define HTTP_LOG(...) ((void)0)
#endif

/* ==============================
 * Statistics Tracking (atomic counters - no lock needed)
 * ============================== */

typedef struct {
  volatile gint64 total_requests;
  volatile gint64 total_bytes;
  volatile gint64 total_time_us;
  volatile gint64 cache_hits;
  volatile gint64 cache_misses;
  volatile gint64 connection_reuses;
  volatile gint64 new_connections;
} HttpStats;

static HttpStats global_stats = {0};

static inline void stats_record_request(size_t bytes, gint64 time_us, gboolean reused) {
  g_atomic_int_add(&global_stats.total_requests, 1);
  g_atomic_int_add(&global_stats.total_bytes, (gint)bytes);
  g_atomic_int_add(&global_stats.total_time_us, (gint)time_us);
  if (reused) {
    g_atomic_int_add(&global_stats.connection_reuses, 1);
  } else {
    g_atomic_int_add(&global_stats.new_connections, 1);
  }
}

static inline void stats_record_cache(gboolean hit) {
  if (hit) {
    g_atomic_int_add(&global_stats.cache_hits, 1);
  } else {
    g_atomic_int_add(&global_stats.cache_misses, 1);
  }
}

/* ==============================
 * Global Configuration
 * ============================== */

static OpenslideHTTPConfig http_config = {
  .block_size = 1024 * 1024,       /* 1MB blocks */
  .max_cache_blocks = 0,           /* 0 = unlimited */
  .retry_max = 3,
  .retry_delay_ms = 100,
  .connect_timeout_ms = 10000,
  .transfer_timeout_ms = 60000,
  .low_speed_limit = 1024,
  .low_speed_time = 30,
  .pool_ttl_sec = 300,
};

void _openslide_http_set_config(const OpenslideHTTPConfig *cfg) {
  if (cfg) {
    http_config = *cfg;
  }
}

const OpenslideHTTPConfig *_openslide_http_get_config(void) {
  return &http_config;
}

/* ==============================
 * Internal Data Structures
 * ============================== */

/* Block state for in-flight tracking */
typedef enum {
  BLOCK_STATE_EMPTY = 0,
  BLOCK_STATE_FETCHING,  /* Being downloaded - other threads wait */
  BLOCK_STATE_READY      /* Data available */
} BlockState;

/* Cache entry with O(1) LRU support */
typedef struct {
  uint8_t *data;
  size_t len;
  GList *lru_node;       /* Direct pointer to LRU list node - O(1) promote */
  BlockState state;
  GCond fetch_cond;      /* Condition variable for in-flight waiting */
} HttpBlock;

/* CURL handle pool entry */
typedef struct {
  CURL *curl;
  gboolean in_use;
  gint64 last_used;
} CurlHandle;

#define MAX_CURL_HANDLES 16

/* Connection state */
typedef enum {
  CONN_STATE_INIT = 0,
  CONN_STATE_CONNECTING,  /* HEAD request in progress */
  CONN_STATE_READY,       /* Ready for use */
  CONN_STATE_ERROR
} ConnState;

/* Shared connection info */
typedef struct _http_connection {
  gchar *uri;
  uint64_t file_size;
  ConnState state;
  GCond state_cond;      /* Wait for connection ready */

  GHashTable *block_map;  /* block_idx -> HttpBlock* */
  GList *lru_list;        /* LRU list head (most recent first) */
  guint cache_count;
  GMutex cache_mutex;     /* Separate lock for cache only */

  /* CURL handle pool */
  CurlHandle curl_pool[MAX_CURL_HANDLES];
  GMutex curl_mutex;

  gint ref_count;
  GMutex mutex;           /* Protects ref_count and state */

  /* Per-connection stats (atomic) */
  volatile gint64 range_requests;
  volatile gint64 range_bytes;
  volatile gint64 range_time_us;
} HttpConnection;

/* Per-open file handle */
struct _openslide_http_file {
  HttpConnection *conn;
  int64_t position;
  
  /* Read-ahead tracking */
  int64_t last_read_end;
  guint64 last_block_idx;
  guint32 sequential_hits;
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
static GHashTable *conn_pool = NULL;
static CURLSH *curl_share = NULL;
static gboolean http_initialized = FALSE;
static gboolean pool_mutex_initialized = FALSE;

/* CURL share lock mutexes */
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

  /* Create new handle in empty slot */
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
  return curl_easy_init();  /* Temporary handle */
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
  curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
  curl_easy_setopt(curl, CURLOPT_FRESH_CONNECT, 0L);
  curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
  
#ifdef CURL_HTTP_VERSION_2_0
  curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
#endif
#ifdef CURLOPT_TCP_FASTOPEN
  curl_easy_setopt(curl, CURLOPT_TCP_FASTOPEN, 1L);
#endif
  
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
 * Write Callback
 * ============================== */

static size_t http_write_callback(void *data, size_t sz, size_t nmemb,
                                  void *user) {
  WriteCtx *ctx = (WriteCtx *)user;
  size_t total = sz * nmemb;

  if (ctx->remain == 0) {
    return total;
  }

  size_t to_copy = MIN(total, ctx->remain);
  memcpy(ctx->dst, data, to_copy);
  ctx->dst += to_copy;
  ctx->written += to_copy;
  ctx->remain -= to_copy;

  return total;
}

/* ==============================
 * Range Fetch
 * ============================== */

static gint64 get_time_us(void) {
  return g_get_monotonic_time();
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

    curl_easy_reset(curl);

    char range_header[128];
    snprintf(range_header, sizeof(range_header),
             "Range: bytes=%" PRIu64 "-%" PRIu64,
             offset + ctx.written, offset + len - 1);

    struct curl_slist *headers = curl_slist_append(NULL, range_header);

    http_curl_setup_common(curl, conn->uri);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, http_write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ctx);

    CURLcode code = curl_easy_perform(curl);

    long http_code = 0;
    if (code == CURLE_OK) {
      curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    }

    HTTP_LOG("[HTTP-RANGE] offset=%" PRIu64 " len=%zu reused=%d http=%ld written=%zu\n",
             offset, len, was_reused, http_code, ctx.written);

    curl_slist_free_all(headers);
    curl_pool_release(conn, curl);

    if (code == CURLE_OK && (http_code == 200 || http_code == 206)) {
      gint64 elapsed = get_time_us() - start_time;
      stats_record_request(ctx.written, elapsed, was_reused);
      g_atomic_int_add(&conn->range_requests, 1);
      g_atomic_int_add(&conn->range_bytes, (gint)ctx.written);
      g_atomic_int_add(&conn->range_time_us, (gint)elapsed);
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
 * LRU Cache Management (O(1) operations)
 * ============================== */

/* O(1) promote - we store direct pointer to GList node */
static void lru_promote(HttpConnection *conn, HttpBlock *block) {
  if (!block->lru_node) {
    return;
  }
  /* Move node to front - O(1) with GList */
  conn->lru_list = g_list_remove_link(conn->lru_list, block->lru_node);
  conn->lru_list = g_list_concat(block->lru_node, conn->lru_list);
}

/* O(1) evict from tail */
static void lru_evict(HttpConnection *conn) {
  if (!conn->lru_list) {
    return;
  }
  GList *last = g_list_last(conn->lru_list);
  if (!last) {
    return;
  }
  guint64 idx = GPOINTER_TO_UINT(last->data);
  HttpBlock *b = g_hash_table_lookup(conn->block_map, GUINT_TO_POINTER(idx));
  if (b) {
    g_free(b->data);
    b->data = NULL;
    g_cond_clear(&b->fetch_cond);
    g_free(b);
  }
  g_hash_table_remove(conn->block_map, GUINT_TO_POINTER(idx));
  conn->lru_list = g_list_delete_link(conn->lru_list, last);
  conn->cache_count--;
}

/* ==============================
 * Block Fetch with In-Flight Deduplication
 * ============================== */

static HttpBlock *http_get_block(HttpConnection *conn,
                                 guint64 block_idx, GError **err) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();

  g_mutex_lock(&conn->cache_mutex);

  /* Check if block exists */
  HttpBlock *b = g_hash_table_lookup(conn->block_map, GUINT_TO_POINTER(block_idx));
  
  if (b) {
    if (b->state == BLOCK_STATE_READY) {
      /* Cache hit - promote and return */
      stats_record_cache(TRUE);
      lru_promote(conn, b);
      g_mutex_unlock(&conn->cache_mutex);
      return b;
    } else if (b->state == BLOCK_STATE_FETCHING) {
      /* Another thread is fetching - wait for it */
      while (b->state == BLOCK_STATE_FETCHING) {
        g_cond_wait(&b->fetch_cond, &conn->cache_mutex);
      }
      if (b->state == BLOCK_STATE_READY) {
        stats_record_cache(TRUE);
        g_mutex_unlock(&conn->cache_mutex);
        return b;
      }
      /* Fetch failed, we'll retry */
    }
  }

  /* Cache miss - create placeholder and mark as fetching */
  stats_record_cache(FALSE);
  
  if (!b) {
    b = g_new0(HttpBlock, 1);
    g_cond_init(&b->fetch_cond);
    b->lru_node = g_list_alloc();
    b->lru_node->data = GUINT_TO_POINTER(block_idx);
    
    /* Evict if needed */
    while (cfg->max_cache_blocks > 0 && conn->cache_count >= cfg->max_cache_blocks) {
      lru_evict(conn);
    }
    
    g_hash_table_insert(conn->block_map, GUINT_TO_POINTER(block_idx), b);
    conn->lru_list = g_list_concat(b->lru_node, conn->lru_list);
    conn->cache_count++;
  }
  
  b->state = BLOCK_STATE_FETCHING;
  g_mutex_unlock(&conn->cache_mutex);

  /* Fetch data outside lock */
  uint64_t offset = block_idx * cfg->block_size;
  size_t len = cfg->block_size;
  
  if (offset >= conn->file_size) {
    g_mutex_lock(&conn->cache_mutex);
    b->state = BLOCK_STATE_EMPTY;
    g_cond_broadcast(&b->fetch_cond);
    g_mutex_unlock(&conn->cache_mutex);
    return NULL;
  }
  if (offset + len > conn->file_size) {
    len = (size_t)(conn->file_size - offset);
  }

  uint8_t *data = g_malloc(len);
  size_t written = 0;
  
  if (!http_fetch_range(conn, offset, len, data, &written, err) || written == 0) {
    g_free(data);
    g_mutex_lock(&conn->cache_mutex);
    b->state = BLOCK_STATE_EMPTY;
    g_cond_broadcast(&b->fetch_cond);
    g_mutex_unlock(&conn->cache_mutex);
    return NULL;
  }

  /* Store data and signal waiters */
  g_mutex_lock(&conn->cache_mutex);
  b->data = data;
  b->len = written;
  b->state = BLOCK_STATE_READY;
  g_cond_broadcast(&b->fetch_cond);
  g_mutex_unlock(&conn->cache_mutex);

  return b;
}

/* ==============================
 * Connection Management
 * ============================== */

static void http_connection_destroy(HttpConnection *conn) {
  if (!conn) {
    return;
  }

  /* Free CURL handles */
  g_mutex_lock(&conn->curl_mutex);
  for (int i = 0; i < MAX_CURL_HANDLES; i++) {
    if (conn->curl_pool[i].curl) {
      curl_easy_cleanup(conn->curl_pool[i].curl);
      conn->curl_pool[i].curl = NULL;
    }
  }
  g_mutex_unlock(&conn->curl_mutex);

  /* Free cache */
  g_mutex_lock(&conn->cache_mutex);
  if (conn->block_map) {
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, conn->block_map);
    while (g_hash_table_iter_next(&iter, &key, &value)) {
      HttpBlock *b = value;
      if (b) {
        g_free(b->data);
        b->data = NULL;
        g_cond_clear(&b->fetch_cond);
        g_free(b);
      }
    }
    g_hash_table_destroy(conn->block_map);
    conn->block_map = NULL;
  }
  if (conn->lru_list) {
    g_list_free(conn->lru_list);
    conn->lru_list = NULL;
  }
  g_mutex_unlock(&conn->cache_mutex);

  g_free(conn->uri);
  conn->uri = NULL;
  g_mutex_clear(&conn->mutex);
  g_mutex_clear(&conn->cache_mutex);
  g_mutex_clear(&conn->curl_mutex);
  g_cond_clear(&conn->state_cond);
  g_free(conn);
}

static void http_connection_unref(HttpConnection *conn) {
  if (!conn) {
    return;
  }

  g_mutex_lock(&conn->mutex);
  conn->ref_count--;
  int count = conn->ref_count;
  g_mutex_unlock(&conn->mutex);

  if (count <= 0) {
    /* Print per-connection stats */
    gint64 reqs = g_atomic_int_get(&conn->range_requests);
    gint64 bytes = g_atomic_int_get(&conn->range_bytes);
    gint64 time_us = g_atomic_int_get(&conn->range_time_us);
    if (reqs > 0) {
      HTTP_LOG("[HTTP-STATS] %s | requests=%" PRId64 " bytes=%" PRId64 " time=%" PRId64 "ms avg=%.2fms/req\n",
               conn->uri, reqs, bytes, time_us / 1000, (double)time_us / reqs / 1000.0);
    }

    ensure_pool_mutex_init();
    g_mutex_lock(&pool_mutex);
    if (conn_pool) {
      g_hash_table_remove(conn_pool, conn->uri);
    }
    g_mutex_unlock(&pool_mutex);

    http_connection_destroy(conn);
  }
}

/* Fetch file size using first Range request (avoids separate HEAD) */
static bool http_get_file_size(const char *uri, uint64_t *out_size, GError **err) {
  CURL *curl = curl_easy_init();
  if (!curl) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Failed to init CURL");
    return false;
  }

  http_curl_setup_common(curl, uri);
  
  /* Use Range: bytes=0-0 to get Content-Range header */
  struct curl_slist *headers = curl_slist_append(NULL, "Range: bytes=0-0");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
  curl_easy_setopt(curl, CURLOPT_NOBODY, 0L);
  
  /* Discard body */
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, 
                   (size_t (*)(void*, size_t, size_t, void*))
                   [](void *d, size_t s, size_t n, void *u) { return s*n; });

  CURLcode code = curl_easy_perform(curl);
  
  long http_code = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

  bool success = false;
  
  if (code == CURLE_OK && http_code == 206) {
    /* Parse Content-Range: bytes 0-0/12345 */
    double cl = 0;
    curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
    
    /* Try to get from header */
    char *content_range = NULL;
    curl_easy_getinfo(curl, CURLINFO_CONTENT_TYPE, &content_range);
    
    /* Fallback: try HEAD */
    curl_easy_reset(curl);
    http_curl_setup_common(curl, uri);
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    code = curl_easy_perform(curl);
    
    if (code == CURLE_OK) {
      curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
      if (cl > 0) {
        *out_size = (uint64_t)cl;
        success = true;
      }
    }
  } else if (code == CURLE_OK && http_code == 200) {
    /* Server doesn't support Range, get full size */
    double cl = 0;
    curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
    if (cl > 0) {
      *out_size = (uint64_t)cl;
      success = true;
    }
  }

  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  if (!success) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Failed to get file size for %s", uri);
  }
  return success;
}

static HttpConnection *http_connection_create(const char *uri, GError **err) {
  HttpConnection *conn = g_new0(HttpConnection, 1);
  conn->uri = g_strdup(uri);
  conn->ref_count = 1;
  conn->state = CONN_STATE_INIT;
  
  g_mutex_init(&conn->mutex);
  g_mutex_init(&conn->cache_mutex);
  g_mutex_init(&conn->curl_mutex);
  g_cond_init(&conn->state_cond);
  
  conn->block_map = g_hash_table_new(g_direct_hash, g_direct_equal);
  
  return conn;
}

static HttpConnection *http_connection_get_or_create(const char *uri, GError **err) {
  ensure_pool_mutex_init();
  _openslide_http_init();

  /* Quick check with pool lock */
  g_mutex_lock(&pool_mutex);
  
  HttpConnection *conn = conn_pool ? g_hash_table_lookup(conn_pool, uri) : NULL;
  
  if (conn) {
    g_mutex_lock(&conn->mutex);
    
    /* Wait if another thread is connecting */
    while (conn->state == CONN_STATE_CONNECTING) {
      g_mutex_unlock(&pool_mutex);
      g_cond_wait(&conn->state_cond, &conn->mutex);
      g_mutex_lock(&pool_mutex);
    }
    
    if (conn->state == CONN_STATE_READY) {
      conn->ref_count++;
      g_mutex_unlock(&conn->mutex);
      g_mutex_unlock(&pool_mutex);
      return conn;
    }
    
    /* Error state - remove and retry */
    g_mutex_unlock(&conn->mutex);
    g_hash_table_remove(conn_pool, uri);
    conn = NULL;
  }

  /* Create new connection placeholder */
  conn = http_connection_create(uri, err);
  if (!conn) {
    g_mutex_unlock(&pool_mutex);
    return NULL;
  }
  
  conn->state = CONN_STATE_CONNECTING;
  g_hash_table_insert(conn_pool, g_strdup(uri), conn);
  
  g_mutex_unlock(&pool_mutex);

  /* Do network operation OUTSIDE pool_mutex */
  uint64_t file_size = 0;
  bool size_ok = http_get_file_size(uri, &file_size, err);

  g_mutex_lock(&conn->mutex);
  if (size_ok && file_size > 0) {
    conn->file_size = file_size;
    conn->state = CONN_STATE_READY;
    HTTP_LOG("[HTTP-OPEN] %s size=%" PRIu64 "\n", uri, file_size);
  } else {
    conn->state = CONN_STATE_ERROR;
  }
  g_cond_broadcast(&conn->state_cond);
  g_mutex_unlock(&conn->mutex);

  if (conn->state != CONN_STATE_READY) {
    http_connection_unref(conn);
    return NULL;
  }

  return conn;
}

/* ==============================
 * Public File API
 * ============================== */

struct _openslide_http_file *_openslide_http_open(const char *uri,
                                                   GError **err) {
  HttpConnection *conn = http_connection_get_or_create(uri, err);
  if (!conn) {
    return NULL;
  }

  struct _openslide_http_file *file = g_new0(struct _openslide_http_file, 1);
  file->conn = conn;
  file->position = 0;
  file->last_read_end = -1;
  file->last_block_idx = G_MAXUINT64;
  file->sequential_hits = 0;

  return file;
}

void _openslide_http_close(struct _openslide_http_file *file) {
  if (!file) {
    return;
  }
  http_connection_unref(file->conn);
  file->conn = NULL;
  g_free(file);
}

bool _openslide_http_seek(struct _openslide_http_file *file,
                          int64_t offset, int whence,
                          GError **err G_GNUC_UNUSED) {
  if (!file) {
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
      return false;
  }

  if (new_pos < 0) {
    new_pos = 0;
  }
  file->position = new_pos;
  return true;
}

int64_t _openslide_http_tell(struct _openslide_http_file *file) {
  return file ? file->position : -1;
}

uint64_t _openslide_http_size(struct _openslide_http_file *file,
                              GError **err G_GNUC_UNUSED) {
  return file ? file->conn->file_size : 0;
}

const char *_openslide_http_get_uri(struct _openslide_http_file *file) {
  return file ? file->conn->uri : NULL;
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
    if (!b || !b->data) {
      break;
    }

    if (block_off >= b->len) {
      break;
    }

    size_t copy_len = MIN(to_read - total, b->len - block_off);
    memcpy(dst + total, b->data + block_off, copy_len);
    total += copy_len;
  }

  file->position += (int64_t)total;
  file->last_read_end = file->position;

  return total;
}

/* Zero-copy pread */
size_t _openslide_http_pread(struct _openslide_http_file *file,
                             uint64_t offset, void *buf, size_t size,
                             GError **err) {
  if (!file || !buf || size == 0) {
    return 0;
  }

  HttpConnection *conn = file->conn;
  uint8_t *dst = (uint8_t *)buf;
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();

  if (offset >= conn->file_size) {
    return 0;
  }

  size_t to_read = MIN(size, (size_t)(conn->file_size - offset));
  size_t total = 0;

  while (total < to_read) {
    guint64 block_idx = (offset + total) / cfg->block_size;
    uint32_t block_off = (offset + total) % cfg->block_size;

    HttpBlock *b = http_get_block(conn, block_idx, err);
    if (!b || !b->data) {
      break;
    }

    if (block_off >= b->len) {
      break;
    }

    size_t copy_len = MIN(to_read - total, b->len - block_off);
    memcpy(dst + total, b->data + block_off, copy_len);
    total += copy_len;
  }

  return total;
}

/* Zero-copy pointer access */
bool _openslide_http_pread_ptr(struct _openslide_http_file *file,
                               uint64_t offset,
                               const uint8_t **out_ptr,
                               size_t *out_avail,
                               GError **err) {
  if (!file || !out_ptr || !out_avail) {
    return false;
  }

  HttpConnection *conn = file->conn;
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();

  if (offset >= conn->file_size) {
    *out_ptr = NULL;
    *out_avail = 0;
    return true;
  }

  guint64 block_idx = offset / cfg->block_size;
  uint32_t block_off = offset % cfg->block_size;

  HttpBlock *b = http_get_block(conn, block_idx, err);
  if (!b || !b->data) {
    return false;
  }

  if (block_off >= b->len) {
    *out_ptr = NULL;
    *out_avail = 0;
    return true;
  }

  *out_ptr = b->data + block_off;
  *out_avail = b->len - block_off;
  return true;
}

/* ==============================
 * Statistics API
 * ============================== */

void _openslide_http_print_stats(void) {
  gint64 reqs = g_atomic_int_get(&global_stats.total_requests);
  gint64 bytes = g_atomic_int_get(&global_stats.total_bytes);
  gint64 time_us = g_atomic_int_get(&global_stats.total_time_us);
  gint64 hits = g_atomic_int_get(&global_stats.cache_hits);
  gint64 misses = g_atomic_int_get(&global_stats.cache_misses);
  gint64 reused = g_atomic_int_get(&global_stats.connection_reuses);
  gint64 newconn = g_atomic_int_get(&global_stats.new_connections);

  fprintf(stderr, "[HTTP-GLOBAL-STATS] requests=%" PRId64 " bytes=%" PRId64 
          " time=%" PRId64 "ms avg=%.2fms/req\n",
          reqs, bytes, time_us / 1000, 
          reqs > 0 ? (double)time_us / reqs / 1000.0 : 0.0);
  fprintf(stderr, "[HTTP-GLOBAL-STATS] cache_hits=%" PRId64 " cache_misses=%" PRId64 
          " hit_rate=%.1f%%\n",
          hits, misses, 
          (hits + misses) > 0 ? 100.0 * hits / (hits + misses) : 0.0);
  fprintf(stderr, "[HTTP-GLOBAL-STATS] conn_reused=%" PRId64 " conn_new=%" PRId64 
          " reuse_rate=%.1f%%\n",
          reused, newconn,
          (reused + newconn) > 0 ? 100.0 * reused / (reused + newconn) : 0.0);
}
