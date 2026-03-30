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
 * Debug/Stats Control - set to 0 for production
 * ============================== */
#define HTTP_DEBUG 0
#define HTTP_STATS 0

#if HTTP_DEBUG
  #define HTTP_LOG(...) fprintf(stderr, __VA_ARGS__)
#else
  #define HTTP_LOG(...) ((void)0)
#endif

/* ==============================
 * Statistics Tracking (only when HTTP_STATS enabled)
 * ============================== */

#if HTTP_STATS

typedef struct {
  volatile gint total_requests;
  volatile gint total_bytes_kb;
  volatile gint total_time_ms;
  volatile gint cache_hits;
  volatile gint cache_misses;
  volatile gint connection_reuses;
  volatile gint new_connections;
} HttpStats;

static HttpStats global_stats = {0};

static inline void stats_record_request(size_t bytes, gint64 time_us, gboolean reused) {
  g_atomic_int_add(&global_stats.total_requests, 1);
  g_atomic_int_add(&global_stats.total_bytes_kb, (gint)(bytes / 1024));
  g_atomic_int_add(&global_stats.total_time_ms, (gint)(time_us / 1000));
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

#else
  #define stats_record_request(bytes, time_us, reused) ((void)0)
  #define stats_record_cache(hit) ((void)0)
#endif

/* ==============================
 * Global Configuration
 * ============================== */

static OpenslideHTTPConfig http_config = {
  .block_size = 256 * 1024,        /* 256KB blocks - balance latency vs requests */
  .max_cache_blocks = 0,           /* 0 = unlimited cache */
  .retry_max = 3,
  .retry_delay_ms = 100,
  .connect_timeout_ms = 10000,
  .transfer_timeout_ms = 30000,    /* 30s per request */
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

/* Tunables for remote open / metadata-heavy access paths.
 * Keep these local so we don't need to change the public config struct yet. */
#define HTTP_OPEN_PREFETCH_BYTES        (1024 * 1024)   /* 1MB on first open */
#define HTTP_METADATA_WINDOW_BYTES      (1024 * 1024)   /* Treat first 1MB as metadata-heavy */
#define HTTP_NORMAL_MAX_EXTENT_BYTES    (1024 * 1024)   /* 1MB for general reads */
#define HTTP_METADATA_MAX_EXTENT_BYTES  (2 * 1024 * 1024) /* 2MB for header/index/small images */
#define HTTP_SMALL_READ_THRESHOLD_BYTES (64 * 1024)     /* Tiny reads benefit from modest neighbor prefetch */
#define HTTP_SMALL_READ_AHEAD_BYTES     (256 * 1024)    /* One extra 256KB window for tiny non-metadata reads */

#define HTTP_FRONT_WINDOW_BYTES         (4 * 1024 * 1024)  /* Grab 4MB up front to collapse metadata chatter */
#define HTTP_FRONT_WINDOW_MAX_BYTES     (8 * 1024 * 1024)  /* Allow metadata/DZI window to grow to 8MB */


/* ==============================
 * Internal Data Structures
 * ============================== */

/* Block state for in-flight tracking */
typedef enum {
  BLOCK_STATE_EMPTY = 0,
  BLOCK_STATE_FETCHING,  /* Being downloaded - other threads wait */
  BLOCK_STATE_READY      /* Data available */
} BlockState;

/* Intrusive LRU node for O(1) operations */
typedef struct _lru_node {
  guint64 block_idx;
  struct _lru_node *prev;
  struct _lru_node *next;
} LruNode;

/* Cache entry with O(1) LRU support */
typedef struct {
  uint8_t *data;
  size_t len;
  LruNode *lru_node;     /* Intrusive O(1) LRU node */
  BlockState state;
  GCond fetch_cond;      /* Condition variable for in-flight waiting */
} HttpBlock;

/* Connection state */
typedef enum {
  CONN_STATE_INIT = 0,
  CONN_STATE_CONNECTING,  /* HEAD request in progress */
  CONN_STATE_READY,       /* Ready for use */
  CONN_STATE_ERROR,
  CONN_STATE_CLOSING
} ConnState;

/* Shared connection info */
typedef struct _http_connection {
  gchar *uri;
  uint64_t file_size;

  /* Front window for metadata-heavy reads.  This is separate from block cache
   * so that dozens of tiny TIFF/IFD reads do not turn into dozens of HTTP
   * transactions. */
  uint8_t *front_window_data;
  size_t front_window_len;

  ConnState state;
  GCond state_cond;      /* Wait for connection ready */

  GHashTable *block_map;  /* block_idx -> HttpBlock* */
  LruNode *lru_head;      /* Most recently used */
  LruNode *lru_tail;      /* Least recently used */
  guint cache_count;
  GMutex cache_mutex;     /* Separate lock for cache only */

  gint ref_count;
  gint64 last_used_us;    /* Last time this pooled connection was borrowed/released */
  GMutex mutex;           /* Protects ref_count, state, and last_used_us */

#if HTTP_STATS
  /* Per-connection stats (atomic, using gint for compatibility) */
  volatile gint range_requests;
  volatile gint range_bytes_kb;    /* in KB */
  volatile gint range_time_ms;     /* in ms */
#endif
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

static void async_downloader_init(void);
static void async_downloader_shutdown(void);
static HttpBlock *http_get_block(HttpConnection *conn,
                                 guint64 block_idx, GError **err);
static bool http_fill_front_window(HttpConnection *conn, size_t want_bytes, GError **err);
static bool http_front_window_contains(HttpConnection *conn, uint64_t offset, size_t size);
static bool http_front_window_view(HttpConnection *conn, uint64_t offset, const uint8_t **out_ptr, size_t *out_avail);
static void http_connection_destroy(HttpConnection *conn);
static GList *http_pool_collect_expired_locked(void);
static void http_destroy_connection_list(GList *list);

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
    async_downloader_init();
    http_initialized = TRUE;
  }
  g_mutex_unlock(&pool_mutex);
}

void _openslide_http_cleanup(void) {
  ensure_pool_mutex_init();

  g_mutex_lock(&pool_mutex);
  if (http_initialized) {
    GList *to_destroy = NULL;
    if (conn_pool) {
      GHashTableIter iter;
      gpointer key, value;
      g_hash_table_iter_init(&iter, conn_pool);
      while (g_hash_table_iter_next(&iter, &key, &value)) {
        to_destroy = g_list_prepend(to_destroy, value);
      }
      g_hash_table_destroy(conn_pool);
      conn_pool = NULL;
    }

    async_downloader_shutdown();

    if (curl_share) {
      curl_share_cleanup(curl_share);
      curl_share = NULL;
    }

    curl_global_cleanup();
    http_initialized = FALSE;

    g_mutex_unlock(&pool_mutex);
    http_destroy_connection_list(to_destroy);
    return;
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
  curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT, 300L);
  curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
  curl_easy_setopt(curl, CURLOPT_FRESH_CONNECT, 0L);
  curl_easy_setopt(curl, CURLOPT_TCP_NODELAY, 1L);
  
#ifdef CURL_HTTP_VERSION_2_0
  curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
#endif
#ifdef CURLOPT_PIPEWAIT
  /* Wait for HTTP/2 multiplexing instead of opening new connection */
  curl_easy_setopt(curl, CURLOPT_PIPEWAIT, 1L);
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
 * Write / Header Callbacks
 * ============================== */

typedef struct {
  uint64_t total_size;
  uint64_t content_length;
  gboolean found_content_range;
  gboolean found_content_length;
} HeaderCtx;

/* Discard body callback - used when we only need headers */
static size_t http_discard_callback(void *data G_GNUC_UNUSED,
                                    size_t sz, size_t nmemb,
                                    void *user G_GNUC_UNUSED) {
  return sz * nmemb;
}

static size_t http_write_callback(void *data, size_t sz, size_t nmemb,
                                  void *user) {
  WriteCtx *ctx = (WriteCtx *) user;
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

static size_t header_callback(char *buffer, size_t size, size_t nitems, void *userdata) {
  HeaderCtx *ctx = (HeaderCtx *) userdata;
  size_t total = size * nitems;

  if (g_ascii_strncasecmp(buffer, "Content-Range:", 14) == 0) {
    char *slash = strchr(buffer, '/');
    if (slash && slash[1] != '*') {
      ctx->total_size = (uint64_t) g_ascii_strtoull(slash + 1, NULL, 10);
      if (ctx->total_size > 0) {
        ctx->found_content_range = TRUE;
      }
    }
  } else if (g_ascii_strncasecmp(buffer, "Content-Length:", 15) == 0) {
    char *p = buffer + 15;
    while (*p == ' ' || *p == '\t') {
      p++;
    }
    ctx->content_length = (uint64_t) g_ascii_strtoull(p, NULL, 10);
    if (ctx->content_length > 0) {
      ctx->found_content_length = TRUE;
    }
  }
  return total;
}

/* ==============================
 * Async Download Manager using curl_multi
 * ============================== */

#define HTTP_ASYNC_SLOTS 8
#define HTTP_COALESCE_SMALL_MAX_BYTES   (128 * 1024)
#define HTTP_COALESCE_MAX_SPAN_BYTES    (512 * 1024)
#define HTTP_COALESCE_MAX_GAP_BYTES     (64 * 1024)
#define HTTP_COALESCE_MAX_JOBS          8

typedef struct _http_request_job {
  HttpConnection *conn;
  uint64_t offset;
  size_t len;
  uint8_t *dst;

  gboolean capture_size;
  gboolean was_reused;

  WriteCtx write_ctx;
  HeaderCtx header_ctx;

  size_t out_written;
  uint64_t discovered_size;

  CURLcode curl_code;
  long http_code;
  int attempt;

  gboolean completed;
  gboolean success;
  GError *error;

  GMutex mutex;
  GCond cond;

  struct _http_request_job *next;
} HttpRequestJob;

typedef struct {
  HttpRequestJob *jobs[HTTP_COALESCE_MAX_JOBS];
  guint job_count;
  uint64_t merged_offset;
  size_t merged_len;
  uint8_t *merged_buffer;
  WriteCtx write_ctx;
  HeaderCtx header_ctx;
  int attempt;
} AsyncBatch;

typedef struct {
  CURL *easy;
  HttpRequestJob *job;
  AsyncBatch *batch;
  struct curl_slist *headers;
  gboolean busy;
  gboolean ever_used;
} AsyncEasySlot;

typedef struct {
  CURLM *multi_handle;
  GThread *thread;
  GMutex mutex;
  GCond cond;
  HttpRequestJob *pending_head;
  HttpRequestJob *pending_tail;
  guint active_jobs;
  gboolean initialized;
  gboolean stop;
  AsyncEasySlot slots[HTTP_ASYNC_SLOTS];

  /* Transport metrics - for debugging connection reuse */
  volatile gint total_requests;       /* Total HTTP requests issued */
  volatile gint reused_connections;   /* Requests that reused an existing connection */
  volatile gint new_connections;      /* Requests that opened a new TCP connection */
  volatile gint http2_streams;        /* HTTP/2 multiplexed streams */
} AsyncDownloader;

static AsyncDownloader g_downloader = {0};

static gboolean http_is_retryable_code(CURLcode code) {
  return code == CURLE_COULDNT_CONNECT ||
         code == CURLE_OPERATION_TIMEDOUT ||
         code == CURLE_RECV_ERROR ||
         code == CURLE_SEND_ERROR ||
         code == CURLE_GOT_NOTHING ||
         code == CURLE_PARTIAL_FILE;
}

static void async_complete_job(HttpRequestJob *job, gboolean success, GError *err) {
  g_mutex_lock(&job->mutex);
  job->success = success;
  job->completed = TRUE;
  if (err) {
    job->error = err;
  }
  g_cond_signal(&job->cond);
  g_mutex_unlock(&job->mutex);
}

static void async_queue_job_locked(HttpRequestJob *job) {
  job->next = NULL;
  if (g_downloader.pending_tail) {
    g_downloader.pending_tail->next = job;
  } else {
    g_downloader.pending_head = job;
  }
  g_downloader.pending_tail = job;
}

static HttpRequestJob *async_pop_job_locked(void) {
  HttpRequestJob *job = g_downloader.pending_head;
  if (!job) {
    return NULL;
  }
  g_downloader.pending_head = job->next;
  if (!g_downloader.pending_head) {
    g_downloader.pending_tail = NULL;
  }
  job->next = NULL;
  return job;
}

static AsyncEasySlot *async_find_free_slot_locked(void) {
  for (int i = 0; i < HTTP_ASYNC_SLOTS; i++) {
    if (!g_downloader.slots[i].busy && g_downloader.slots[i].easy) {
      return &g_downloader.slots[i];
    }
  }
  return NULL;
}

static AsyncEasySlot *async_find_slot_by_easy(CURL *easy) {
  for (int i = 0; i < HTTP_ASYNC_SLOTS; i++) {
    if (g_downloader.slots[i].easy == easy) {
      return &g_downloader.slots[i];
    }
  }
  return NULL;
}

static gboolean async_job_is_coalescible(HttpRequestJob *job) {
  return job && !job->capture_size && job->write_ctx.written == 0 &&
         job->len > 0 && job->len <= HTTP_COALESCE_SMALL_MAX_BYTES;
}

static AsyncBatch *async_batch_new(HttpRequestJob *first) {
  AsyncBatch *batch = g_new0(AsyncBatch, 1);
  batch->jobs[0] = first;
  batch->job_count = 1;
  batch->merged_offset = first->offset;
  batch->merged_len = first->len;
  return batch;
}

static gboolean async_can_merge_job(const AsyncBatch *batch, HttpRequestJob *job) {
  if (!batch || !job || batch->job_count >= HTTP_COALESCE_MAX_JOBS) {
    return FALSE;
  }
  HttpRequestJob *first = batch->jobs[0];
  if (job->conn != first->conn) {
    return FALSE;
  }
  if (!async_job_is_coalescible(first) || !async_job_is_coalescible(job)) {
    return FALSE;
  }
  uint64_t cur_start = batch->merged_offset;
  uint64_t cur_end = batch->merged_offset + batch->merged_len;
  uint64_t job_start = job->offset;
  uint64_t job_end = job->offset + job->len;
  uint64_t new_start = MIN(cur_start, job_start);
  uint64_t new_end = MAX(cur_end, job_end);
  if (new_end - new_start > HTTP_COALESCE_MAX_SPAN_BYTES) {
    return FALSE;
  }
  if (job_start > cur_end + HTTP_COALESCE_MAX_GAP_BYTES) {
    return FALSE;
  }
  if (cur_start > job_end + HTTP_COALESCE_MAX_GAP_BYTES) {
    return FALSE;
  }
  return TRUE;
}

static void async_batch_add_job(AsyncBatch *batch, HttpRequestJob *job) {
  uint64_t cur_end = batch->merged_offset + batch->merged_len;
  uint64_t job_end = job->offset + job->len;
  uint64_t new_start = MIN(batch->merged_offset, job->offset);
  uint64_t new_end = MAX(cur_end, job_end);
  batch->jobs[batch->job_count++] = job;
  batch->merged_offset = new_start;
  batch->merged_len = (size_t) (new_end - new_start);
}

static AsyncBatch *async_collect_batch_locked(HttpRequestJob *first) {
  AsyncBatch *batch = async_batch_new(first);
  HttpRequestJob *prev = NULL;
  HttpRequestJob *cur = g_downloader.pending_head;
  while (cur && batch->job_count < HTTP_COALESCE_MAX_JOBS) {
    HttpRequestJob *next = cur->next;
    if (async_can_merge_job(batch, cur)) {
      if (prev) {
        prev->next = next;
      } else {
        g_downloader.pending_head = next;
      }
      if (g_downloader.pending_tail == cur) {
        g_downloader.pending_tail = prev;
      }
      cur->next = NULL;
      async_batch_add_job(batch, cur);
    } else {
      prev = cur;
    }
    cur = next;
  }
  return batch;
}

static bool async_assign_batch_to_slot(AsyncEasySlot *slot, AsyncBatch *batch, GError **err) {
  char range_header[128];
  HttpRequestJob *first = batch ? batch->jobs[0] : NULL;

  if (!slot || !slot->easy || !batch || !first) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "No async slot available");
    return false;
  }

  batch->merged_buffer = g_malloc(batch->merged_len);
  batch->write_ctx.dst = batch->merged_buffer;
  batch->write_ctx.remain = batch->merged_len;
  batch->write_ctx.written = 0;
  memset(&batch->header_ctx, 0, sizeof(batch->header_ctx));

  snprintf(range_header, sizeof(range_header),
           "Range: bytes=%" PRIu64 "-%" PRIu64,
           batch->merged_offset,
           batch->merged_offset + batch->merged_len - 1);

  slot->headers = curl_slist_append(NULL, range_header);
  if (!slot->headers) {
    g_free(batch->merged_buffer);
    batch->merged_buffer = NULL;
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Failed to create request headers");
    return false;
  }

  curl_easy_reset(slot->easy);
  http_curl_setup_common(slot->easy, first->conn->uri);
  curl_easy_setopt(slot->easy, CURLOPT_HTTPHEADER, slot->headers);
  curl_easy_setopt(slot->easy, CURLOPT_WRITEFUNCTION, http_write_callback);
  curl_easy_setopt(slot->easy, CURLOPT_WRITEDATA, &batch->write_ctx);
  curl_easy_setopt(slot->easy, CURLOPT_HEADERFUNCTION, header_callback);
  curl_easy_setopt(slot->easy, CURLOPT_HEADERDATA, &batch->header_ctx);
  curl_easy_setopt(slot->easy, CURLOPT_PRIVATE, slot);

  for (guint i = 0; i < batch->job_count; i++) {
    batch->jobs[i]->was_reused = slot->ever_used;
  }
  batch->attempt = first->attempt;
  slot->job = NULL;
  slot->batch = batch;
  slot->busy = TRUE;
  return true;
}

static void async_batch_complete_success(AsyncBatch *batch, long http_code) {
  guint64 discovered_size = 0;
  if (batch->header_ctx.found_content_range) {
    discovered_size = batch->header_ctx.total_size;
  } else if (batch->header_ctx.found_content_length && http_code == 200) {
    discovered_size = batch->header_ctx.content_length;
  }

  for (guint i = 0; i < batch->job_count; i++) {
    HttpRequestJob *job = batch->jobs[i];
    if (!job) continue;
    size_t start = (size_t) (job->offset - batch->merged_offset);
    size_t available = 0;
    if (start < batch->write_ctx.written) {
      available = batch->write_ctx.written - start;
    }
    size_t copy_len = MIN(job->len, available);
    if (copy_len > 0) {
      memcpy(job->dst, batch->merged_buffer + start, copy_len);
    }
    job->out_written = copy_len;
    job->discovered_size = discovered_size;
    async_complete_job(job, TRUE, NULL);
  }
}

static void async_batch_requeue_or_fail(AsyncBatch *batch, CURLcode code, long http_code) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();
  gboolean retryable = http_is_retryable_code(code) && batch->attempt < (int) cfg->retry_max;
  if (retryable) {
    for (guint i = 0; i < batch->job_count; i++) {
      HttpRequestJob *job = batch->jobs[i];
      job->attempt = batch->attempt + 1;
      g_mutex_lock(&g_downloader.mutex);
      async_queue_job_locked(job);
      g_mutex_unlock(&g_downloader.mutex);
    }
    g_mutex_lock(&g_downloader.mutex);
    g_cond_signal(&g_downloader.cond);
    g_mutex_unlock(&g_downloader.mutex);
    return;
  }

  for (guint i = 0; i < batch->job_count; i++) {
    HttpRequestJob *job = batch->jobs[i];
    async_complete_job(job, FALSE,
                       g_error_new(OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                                   "HTTP request failed for %s (curl=%d http=%ld)",
                                   job->conn->uri, (int) code, http_code));
  }
}

static void async_release_slot(AsyncEasySlot *slot) {
  if (!slot) {
    return;
  }
  if (slot->headers) {
    curl_slist_free_all(slot->headers);
    slot->headers = NULL;
  }
  if (slot->batch) {
    g_free(slot->batch->merged_buffer);
    g_free(slot->batch);
    slot->batch = NULL;
  }
  slot->job = NULL;
  slot->busy = FALSE;
  slot->ever_used = TRUE;
}

static void async_schedule_pending_jobs(void) {
  while (true) {
    AsyncEasySlot *slot = NULL;
    HttpRequestJob *job = NULL;
    AsyncBatch *batch = NULL;

    g_mutex_lock(&g_downloader.mutex);
    slot = async_find_free_slot_locked();
    if (slot) {
      job = async_pop_job_locked();
      if (job) {
        batch = async_collect_batch_locked(job);
      }
    }
    g_mutex_unlock(&g_downloader.mutex);

    if (!slot || !job || !batch) {
      if (batch) {
        g_free(batch);
      }
      return;
    }

    GError *setup_err = NULL;
    if (!async_assign_batch_to_slot(slot, batch, &setup_err)) {
      async_release_slot(slot);
      for (guint i = 0; i < batch->job_count; i++) {
        GError *job_err = NULL;
        if (setup_err) {
          job_err = (i == 0) ? setup_err : g_error_copy(setup_err);
        }
        async_complete_job(batch->jobs[i], FALSE, job_err);
      }
      continue;
    }

    CURLMcode mc = curl_multi_add_handle(g_downloader.multi_handle, slot->easy);
    if (mc != CURLM_OK) {
      AsyncBatch *failed_batch = slot->batch;
      slot->batch = NULL;
      async_release_slot(slot);
      for (guint i = 0; i < failed_batch->job_count; i++) {
        async_complete_job(failed_batch->jobs[i], FALSE,
                           g_error_new(OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                                       "curl_multi_add_handle failed: %s",
                                       curl_multi_strerror(mc)));
      }
      g_free(failed_batch->merged_buffer);
      g_free(failed_batch);
      continue;
    }

    g_mutex_lock(&g_downloader.mutex);
    g_downloader.active_jobs++;
    g_mutex_unlock(&g_downloader.mutex);
  }
}

static void async_handle_completed_jobs(void) {
  int msgs_left = 0;
  CURLMsg *msg = NULL;

  while ((msg = curl_multi_info_read(g_downloader.multi_handle, &msgs_left)) != NULL) {
    if (msg->msg != CURLMSG_DONE) {
      continue;
    }

    AsyncEasySlot *slot = NULL;
    curl_easy_getinfo(msg->easy_handle, CURLINFO_PRIVATE, &slot);

    long http_code = 0;
    curl_easy_getinfo(msg->easy_handle, CURLINFO_RESPONSE_CODE, &http_code);

    /* Track connection reuse metrics */
    long num_connects = 0;
    curl_easy_getinfo(msg->easy_handle, CURLINFO_NUM_CONNECTS, &num_connects);
    g_atomic_int_add(&g_downloader.total_requests, 1);
    if (num_connects == 0) {
      g_atomic_int_add(&g_downloader.reused_connections, 1);
    } else {
      g_atomic_int_add(&g_downloader.new_connections, 1);
    }
#ifdef CURLINFO_HTTP_VERSION
    long http_version = 0;
    curl_easy_getinfo(msg->easy_handle, CURLINFO_HTTP_VERSION, &http_version);
    if (http_version >= CURL_HTTP_VERSION_2) {
      g_atomic_int_add(&g_downloader.http2_streams, 1);
    }
#endif

    curl_multi_remove_handle(g_downloader.multi_handle, msg->easy_handle);

    if (!slot || !slot->batch) {
      g_mutex_lock(&g_downloader.mutex);
      if (g_downloader.active_jobs > 0) {
        g_downloader.active_jobs--;
      }
      g_mutex_unlock(&g_downloader.mutex);
      continue;
    }

    AsyncBatch *batch = slot->batch;
    CURLcode curl_code = msg->data.result;

    g_mutex_lock(&g_downloader.mutex);
    if (g_downloader.active_jobs > 0) {
      g_downloader.active_jobs--;
    }
    g_mutex_unlock(&g_downloader.mutex);

    gboolean ok = (curl_code == CURLE_OK && (http_code == 200 || http_code == 206));
    if (ok) {
      async_batch_complete_success(batch, http_code);
    } else {
      async_batch_requeue_or_fail(batch, curl_code, http_code);
    }

    async_release_slot(slot);
  }
}

static gpointer async_downloader_thread_main(gpointer data G_GNUC_UNUSED) {
  while (true) {
    async_schedule_pending_jobs();

    g_mutex_lock(&g_downloader.mutex);
    gboolean should_stop = g_downloader.stop &&
                           g_downloader.active_jobs == 0 &&
                           g_downloader.pending_head == NULL;
    gboolean should_wait = !g_downloader.stop &&
                           g_downloader.active_jobs == 0 &&
                           g_downloader.pending_head == NULL;
    g_mutex_unlock(&g_downloader.mutex);

    if (should_stop) {
      break;
    }

    if (should_wait) {
      g_mutex_lock(&g_downloader.mutex);
      while (!g_downloader.stop &&
             g_downloader.active_jobs == 0 &&
             g_downloader.pending_head == NULL) {
        g_cond_wait(&g_downloader.cond, &g_downloader.mutex);
      }
      g_mutex_unlock(&g_downloader.mutex);
      continue;
    }

    int still_running = 0;
    CURLMcode mc = curl_multi_perform(g_downloader.multi_handle, &still_running);
    if (mc != CURLM_OK) {
      g_mutex_lock(&g_downloader.mutex);
      HttpRequestJob *job = g_downloader.pending_head;
      g_downloader.pending_head = NULL;
      g_downloader.pending_tail = NULL;
      g_mutex_unlock(&g_downloader.mutex);

      while (job) {
        HttpRequestJob *next = job->next;
        job->next = NULL;
        async_complete_job(job, FALSE,
                           g_error_new(OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                                       "curl_multi_perform failed: %s",
                                       curl_multi_strerror(mc)));
        job = next;
      }
      g_usleep(1000);
      continue;
    }

    async_handle_completed_jobs();

    long timeout_ms = 100;
    curl_multi_timeout(g_downloader.multi_handle, &timeout_ms);
    if (timeout_ms < 0 || timeout_ms > 100) {
      timeout_ms = 100;
    }

    int numfds = 0;
    mc = curl_multi_poll(g_downloader.multi_handle, NULL, 0, (int) timeout_ms, &numfds);
    if (mc != CURLM_OK) {
      g_usleep(1000);
    }
  }

  return NULL;
}

static void async_downloader_init(void) {
  static gsize init_once = 0;
  if (g_once_init_enter(&init_once)) {
    g_mutex_init(&g_downloader.mutex);
    g_cond_init(&g_downloader.cond);
    g_downloader.multi_handle = curl_multi_init();
    if (g_downloader.multi_handle) {
#ifdef CURLMOPT_PIPELINING
#ifdef CURLPIPE_MULTIPLEX
      curl_multi_setopt(g_downloader.multi_handle, CURLMOPT_PIPELINING, CURLPIPE_MULTIPLEX);
#endif
#endif
      /* CRITICAL: Limit connections per host to enable HTTP/2 multiplexing.
       * With HTTP/2, multiple requests can share a single TCP connection.
       * Setting this to 1-2 forces connection reuse instead of opening many TCP connections. */
#ifdef CURLMOPT_MAX_HOST_CONNECTIONS
      curl_multi_setopt(g_downloader.multi_handle, CURLMOPT_MAX_HOST_CONNECTIONS, 2L);
#endif
#ifdef CURLMOPT_MAX_TOTAL_CONNECTIONS
      curl_multi_setopt(g_downloader.multi_handle, CURLMOPT_MAX_TOTAL_CONNECTIONS, 4L);
#endif
      for (int i = 0; i < HTTP_ASYNC_SLOTS; i++) {
        CURL *easy = curl_easy_init();
        if (easy) {
          /* Pre-configure for HTTP/2 multiplexing */
#ifdef CURL_HTTP_VERSION_2_0
          curl_easy_setopt(easy, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
#endif
#ifdef CURLOPT_PIPEWAIT
          curl_easy_setopt(easy, CURLOPT_PIPEWAIT, 1L);
#endif
          curl_easy_setopt(easy, CURLOPT_TCP_KEEPALIVE, 1L);
          curl_easy_setopt(easy, CURLOPT_FORBID_REUSE, 0L);
          curl_easy_setopt(easy, CURLOPT_FRESH_CONNECT, 0L);
          if (curl_share) {
            curl_easy_setopt(easy, CURLOPT_SHARE, curl_share);
          }
        }
        g_downloader.slots[i].easy = easy;
        g_downloader.slots[i].job = NULL;
        g_downloader.slots[i].headers = NULL;
        g_downloader.slots[i].busy = FALSE;
        g_downloader.slots[i].ever_used = FALSE;
      }
      g_downloader.thread = g_thread_new("openslide-http-multi",
                                         async_downloader_thread_main, NULL);
      g_downloader.initialized = TRUE;
    }
    g_once_init_leave(&init_once, 1);
  }
}

static void async_downloader_shutdown(void) {
  if (!g_downloader.initialized) {
    return;
  }

  g_mutex_lock(&g_downloader.mutex);
  g_downloader.stop = TRUE;
  g_cond_signal(&g_downloader.cond);
  g_mutex_unlock(&g_downloader.mutex);

  if (g_downloader.thread) {
    g_thread_join(g_downloader.thread);
    g_downloader.thread = NULL;
  }
  if (g_downloader.multi_handle) {
    curl_multi_cleanup(g_downloader.multi_handle);
    g_downloader.multi_handle = NULL;
  }

  for (int i = 0; i < HTTP_ASYNC_SLOTS; i++) {
    if (g_downloader.slots[i].headers) {
      curl_slist_free_all(g_downloader.slots[i].headers);
      g_downloader.slots[i].headers = NULL;
    }
    if (g_downloader.slots[i].easy) {
      curl_easy_cleanup(g_downloader.slots[i].easy);
      g_downloader.slots[i].easy = NULL;
    }
    g_downloader.slots[i].job = NULL;
    g_downloader.slots[i].busy = FALSE;
    g_downloader.slots[i].ever_used = FALSE;
  }

  HttpRequestJob *job = g_downloader.pending_head;
  while (job) {
    HttpRequestJob *next = job->next;
    job->next = NULL;
    async_complete_job(job, FALSE,
                       g_error_new(OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                                   "Downloader stopped"));
    job = next;
  }
  g_downloader.pending_head = NULL;
  g_downloader.pending_tail = NULL;
  g_downloader.initialized = FALSE;
}

static gint64 get_time_us(void) {
  return g_get_monotonic_time();
}

static bool http_fetch_range(HttpConnection *conn,
                             uint64_t offset, size_t len,
                             uint8_t *dst, size_t *out_written,
                             uint64_t *out_size,
                             GError **err) {
  if (out_written) {
    *out_written = 0;
  }
  if (out_size) {
    *out_size = 0;
  }
  if (len == 0) {
    return true;
  }

  async_downloader_init();
  if (!g_downloader.initialized) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Async downloader initialization failed");
    return false;
  }

  gint64 start_time = get_time_us();

  HttpRequestJob job = {0};
  job.conn = conn;
  job.offset = offset;
  job.len = len;
  job.dst = dst;
  job.capture_size = (out_size != NULL);
  job.write_ctx.dst = dst;
  job.write_ctx.remain = len;
  job.write_ctx.written = 0;
  g_mutex_init(&job.mutex);
  g_cond_init(&job.cond);

  g_mutex_lock(&g_downloader.mutex);
  async_queue_job_locked(&job);
  g_cond_signal(&g_downloader.cond);
  g_mutex_unlock(&g_downloader.mutex);

  g_mutex_lock(&job.mutex);
  while (!job.completed) {
    g_cond_wait(&job.cond, &job.mutex);
  }
  g_mutex_unlock(&job.mutex);

  if (out_written) {
    *out_written = job.out_written;
  }
  if (out_size) {
    *out_size = job.discovered_size;
  }

#if HTTP_STATS
  if (job.success) {
    gint64 elapsed = get_time_us() - start_time;
    stats_record_request(job.out_written, elapsed, job.was_reused);
    g_atomic_int_add(&conn->range_requests, 1);
    g_atomic_int_add(&conn->range_bytes_kb, (gint) (job.out_written / 1024));
    g_atomic_int_add(&conn->range_time_ms, (gint) (elapsed / 1000));
  }
#else
  (void) start_time;
#endif

  if (!job.success) {
    if (job.error) {
      g_propagate_error(err, job.error);
      job.error = NULL;
    } else {
      g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                  "HTTP range request failed for %s", conn->uri);
    }
  }

  g_cond_clear(&job.cond);
  g_mutex_clear(&job.mutex);
  return job.success;
}

/* Fetch multiple ranges in parallel using the shared curl_multi downloader.
 * This is not on the hot path yet, but unlike the previous skeleton it now
 * runs through the same fully-asynchronous machinery as single-range fetches. */
G_GNUC_UNUSED
static bool http_fetch_parallel(HttpConnection *conn,
                                uint64_t *offsets, size_t *lens,
                                uint8_t **buffers, size_t count,
                                GError **err) {
  if (count == 0) {
    return true;
  }

  async_downloader_init();
  if (!g_downloader.initialized) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Async downloader initialization failed");
    return false;
  }

  HttpRequestJob *jobs = g_new0(HttpRequestJob, count);
  bool all_ok = true;

  for (size_t i = 0; i < count; i++) {
    jobs[i].conn = conn;
    jobs[i].offset = offsets[i];
    jobs[i].len = lens[i];
    jobs[i].dst = buffers[i];
    jobs[i].write_ctx.dst = buffers[i];
    jobs[i].write_ctx.remain = lens[i];
    g_mutex_init(&jobs[i].mutex);
    g_cond_init(&jobs[i].cond);
  }

  g_mutex_lock(&g_downloader.mutex);
  for (size_t i = 0; i < count; i++) {
    async_queue_job_locked(&jobs[i]);
  }
  g_cond_signal(&g_downloader.cond);
  g_mutex_unlock(&g_downloader.mutex);

  for (size_t i = 0; i < count; i++) {
    g_mutex_lock(&jobs[i].mutex);
    while (!jobs[i].completed) {
      g_cond_wait(&jobs[i].cond, &jobs[i].mutex);
    }
    g_mutex_unlock(&jobs[i].mutex);
    if (!jobs[i].success) {
      all_ok = false;
      if (err && !*err && jobs[i].error) {
        g_propagate_error(err, jobs[i].error);
        jobs[i].error = NULL;
      }
    }
    if (jobs[i].error) {
      g_error_free(jobs[i].error);
    }
    g_cond_clear(&jobs[i].cond);
    g_mutex_clear(&jobs[i].mutex);
  }

  g_free(jobs);
  return all_ok;
}
/* ==============================
 * LRU Cache Management (O(1) operations with intrusive linked list)
 * ============================== */

static void lru_insert_front(HttpConnection *conn, HttpBlock *block, guint64 block_idx) {
  LruNode *node = g_new0(LruNode, 1);
  node->block_idx = block_idx;
  node->next = conn->lru_head;
  if (conn->lru_head) {
    conn->lru_head->prev = node;
  } else {
    conn->lru_tail = node;
  }
  conn->lru_head = node;
  block->lru_node = node;
}

static void lru_remove_node(HttpConnection *conn, LruNode *node) {
  if (!node) {
    return;
  }
  if (node->prev) {
    node->prev->next = node->next;
  } else {
    conn->lru_head = node->next;
  }
  if (node->next) {
    node->next->prev = node->prev;
  } else {
    conn->lru_tail = node->prev;
  }
  node->prev = NULL;
  node->next = NULL;
}

static void lru_promote(HttpConnection *conn, HttpBlock *block) {
  LruNode *node = block ? block->lru_node : NULL;
  if (!node || conn->lru_head == node) {
    return;
  }
  lru_remove_node(conn, node);
  node->next = conn->lru_head;
  if (conn->lru_head) {
    conn->lru_head->prev = node;
  } else {
    conn->lru_tail = node;
  }
  conn->lru_head = node;
}

static bool http_front_window_contains(HttpConnection *conn, uint64_t offset, size_t size) {
  if (size == 0) {
    return true;
  }
  return conn->front_window_data != NULL &&
         offset < conn->front_window_len &&
         offset + size <= conn->front_window_len;
}

static bool http_front_window_view(HttpConnection *conn, uint64_t offset,
                                   const uint8_t **out_ptr, size_t *out_avail) {
  if (!http_front_window_contains(conn, offset, 1)) {
    return false;
  }
  *out_ptr = conn->front_window_data + offset;
  *out_avail = conn->front_window_len - (size_t) offset;
  return true;
}

static void http_front_window_replace_locked(HttpConnection *conn,
                                             uint8_t *buffer, size_t len) {
  g_free(conn->front_window_data);
  conn->front_window_data = buffer;
  conn->front_window_len = len;
}

static bool http_fill_front_window(HttpConnection *conn, size_t want_bytes, GError **err) {
  size_t capped = MIN((size_t) HTTP_FRONT_WINDOW_MAX_BYTES, want_bytes);
  if (conn->file_size > 0) {
    capped = MIN(capped, (size_t) conn->file_size);
  }
  if (capped == 0) {
    return true;
  }

  g_mutex_lock(&conn->cache_mutex);
  if (conn->front_window_len >= capped && conn->front_window_data != NULL) {
    g_mutex_unlock(&conn->cache_mutex);
    return true;
  }
  g_mutex_unlock(&conn->cache_mutex);

  uint8_t *buffer = g_malloc(capped);
  size_t written = 0;
  uint64_t discovered_size = 0;

  if (!http_fetch_range(conn, 0, capped, buffer, &written, &discovered_size, err) || written == 0) {
    g_free(buffer);
    return false;
  }

  if (discovered_size == 0) {
    discovered_size = written < capped ? written : conn->file_size;
  }
  if (discovered_size == 0) {
    discovered_size = written;
  }

  uint8_t *owned = g_realloc(buffer, written);

  g_mutex_lock(&conn->cache_mutex);
  if (discovered_size > 0) {
    conn->file_size = discovered_size;
  }
  if (written > conn->front_window_len || conn->front_window_data == NULL) {
    http_front_window_replace_locked(conn, owned, written);
    owned = NULL;
  }
  g_mutex_unlock(&conn->cache_mutex);

  g_free(owned);
  return true;
}

static void http_block_free(HttpBlock *b) {
  if (!b) {
    return;
  }
  g_free(b->data);
  b->data = NULL;
  if (b->lru_node) {
    g_free(b->lru_node);
    b->lru_node = NULL;
  }
  g_cond_clear(&b->fetch_cond);
  g_free(b);
}

static void block_remove_locked(HttpConnection *conn, guint64 block_idx, HttpBlock *b) {
  if (!b) {
    return;
  }
  if (b->lru_node) {
    lru_remove_node(conn, b->lru_node);
  }
  g_hash_table_remove(conn->block_map, GUINT_TO_POINTER(block_idx));
  if (conn->cache_count > 0) {
    conn->cache_count--;
  }
  http_block_free(b);
}

/* O(1) evict from tail */
static void lru_evict(HttpConnection *conn) {
  if (!conn->lru_tail) {
    return;
  }
  guint64 idx = conn->lru_tail->block_idx;
  HttpBlock *b = g_hash_table_lookup(conn->block_map, GUINT_TO_POINTER(idx));
  block_remove_locked(conn, idx, b);
}

static guint64 bytes_to_blocks(size_t bytes, const OpenslideHTTPConfig *cfg) {
  if (bytes == 0) {
    return 0;
  }
  return (bytes + cfg->block_size - 1) / cfg->block_size;
}

static guint64 extent_cap_end_block(HttpConnection *conn,
                                    guint64 start_block,
                                    guint64 requested_end_block) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();
  uint64_t start_offset = start_block * cfg->block_size;
  guint64 file_last_block = conn->file_size == 0 ? 0 :
    (guint64)((conn->file_size - 1) / cfg->block_size);

  size_t max_bytes = HTTP_NORMAL_MAX_EXTENT_BYTES;
  guint64 target_end = requested_end_block;

  if (start_offset < HTTP_METADATA_WINDOW_BYTES) {
    max_bytes = HTTP_METADATA_MAX_EXTENT_BYTES;
    uint64_t metadata_end_offset = MIN((uint64_t) HTTP_METADATA_WINDOW_BYTES,
                                       conn->file_size);
    if (metadata_end_offset > 0) {
      guint64 metadata_end_block = (metadata_end_offset - 1) / cfg->block_size;
      if (metadata_end_block > target_end) {
        target_end = metadata_end_block;
      }
    }
  }

  guint64 max_blocks = bytes_to_blocks(max_bytes, cfg);
  if (max_blocks > 0) {
    guint64 capped = start_block + max_blocks - 1;
    if (target_end > capped) {
      target_end = capped;
    }
  }

  if (target_end > file_last_block) {
    target_end = file_last_block;
  }
  return target_end;
}

static gboolean block_is_ready_locked(HttpConnection *conn G_GNUC_UNUSED,
                                      guint64 block_idx) {
  HttpBlock *b = g_hash_table_lookup(conn->block_map, GUINT_TO_POINTER(block_idx));
  return b && b->state == BLOCK_STATE_READY && b->data != NULL;
}

static bool http_prefetch_missing_blocks(HttpConnection *conn,
                                         guint64 start_block,
                                         guint64 end_block,
                                         GError **err);
static void http_plan_prefetch_blocks(HttpConnection *conn,
                                      uint64_t offset,
                                      size_t to_read,
                                      guint64 *io_start_block,
                                      guint64 *io_end_block);
static size_t http_copy_cached_bytes(HttpConnection *conn,
                                     uint64_t offset,
                                     uint8_t *dst,
                                     size_t to_read,
                                     GError **err);

/* ==============================
 * Extent Merge: Fetch multiple consecutive missing blocks in one request
 * ============================== */

/* Find missing blocks in range [start, end] and fetch them together */
static bool http_fetch_extent(HttpConnection *conn,
                              guint64 start_block, guint64 end_block,
                              GError **err) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();

  end_block = extent_cap_end_block(conn, start_block, end_block);
  if (end_block < start_block) {
    return true;
  }

  /* Calculate byte range */
  uint64_t start_offset = start_block * cfg->block_size;
  uint64_t end_offset = (end_block + 1) * cfg->block_size;

  if (start_offset >= conn->file_size) {
    return true;  /* Nothing to fetch */
  }
  if (end_offset > conn->file_size) {
    end_offset = conn->file_size;
  }

  size_t total_len = (size_t)(end_offset - start_offset);
  if (total_len == 0) {
    return true;
  }

  HTTP_LOG("[HTTP-EXTENT] Fetching blocks %" PRIu64 "-%" PRIu64 " (%zu bytes)\n",
           start_block, end_block, total_len);

  /* Reserve missing blocks up front so parallel readers wait instead of
   * issuing duplicate single-block fetches. */
  g_mutex_lock(&conn->cache_mutex);
  for (guint64 idx = start_block; idx <= end_block; idx++) {
    HttpBlock *b = g_hash_table_lookup(conn->block_map, GUINT_TO_POINTER(idx));
    if (!b) {
      while (cfg->max_cache_blocks > 0 && conn->cache_count >= cfg->max_cache_blocks) {
        lru_evict(conn);
      }
      b = g_new0(HttpBlock, 1);
      g_cond_init(&b->fetch_cond);
      lru_insert_front(conn, b, idx);
      g_hash_table_insert(conn->block_map, GUINT_TO_POINTER(idx), b);
      conn->cache_count++;
    }
    if (b->state != BLOCK_STATE_READY) {
      b->state = BLOCK_STATE_FETCHING;
    }
  }
  g_mutex_unlock(&conn->cache_mutex);

  /* Allocate buffer for entire extent */
  uint8_t *buffer = g_malloc(total_len);
  size_t written = 0;

  if (!http_fetch_range(conn, start_offset, total_len, buffer, &written, NULL, err)) {
    g_free(buffer);
    /* Mark reserved blocks as failed and remove them */
    g_mutex_lock(&conn->cache_mutex);
    for (guint64 idx = start_block; idx <= end_block; idx++) {
      HttpBlock *b = g_hash_table_lookup(conn->block_map, GUINT_TO_POINTER(idx));
      if (b && b->state == BLOCK_STATE_FETCHING) {
        g_cond_broadcast(&b->fetch_cond);
        block_remove_locked(conn, idx, b);
      }
    }
    g_mutex_unlock(&conn->cache_mutex);
    return false;
  }

  /* Split into blocks and cache them */
  g_mutex_lock(&conn->cache_mutex);

  for (guint64 idx = start_block; idx <= end_block; idx++) {
    HttpBlock *existing = g_hash_table_lookup(conn->block_map, GUINT_TO_POINTER(idx));
    if (existing && existing->state == BLOCK_STATE_READY && existing->data != NULL) {
      lru_promote(conn, existing);
      continue;
    }

    uint64_t block_start = (idx - start_block) * cfg->block_size;
    uint64_t block_end = block_start + cfg->block_size;
    if (block_end > written) {
      block_end = written;
    }
    if (block_start >= written) {
      break;
    }

    size_t block_len = (size_t)(block_end - block_start);

    HttpBlock *b = existing;
    if (!b) {
      b = g_new0(HttpBlock, 1);
      g_cond_init(&b->fetch_cond);

      while (cfg->max_cache_blocks > 0 && conn->cache_count >= cfg->max_cache_blocks) {
        lru_evict(conn);
      }

      lru_insert_front(conn, b, idx);
      g_hash_table_insert(conn->block_map, GUINT_TO_POINTER(idx), b);
      conn->cache_count++;
    }

    g_free(b->data);
    b->data = g_malloc(block_len);
    memcpy(b->data, buffer + block_start, block_len);
    b->len = block_len;
    b->state = BLOCK_STATE_READY;
    lru_promote(conn, b);
    g_cond_broadcast(&b->fetch_cond);
  }

  /* Any reserved blocks not populated by the returned range should be removed. */
  for (guint64 idx = start_block; idx <= end_block; idx++) {
    HttpBlock *b = g_hash_table_lookup(conn->block_map, GUINT_TO_POINTER(idx));
    if (!b) {
      continue;
    }
    if (b->state == BLOCK_STATE_FETCHING) {
      g_cond_broadcast(&b->fetch_cond);
      block_remove_locked(conn, idx, b);
    }
  }

  g_mutex_unlock(&conn->cache_mutex);
  g_free(buffer);

  return true;
}

static bool http_prefetch_missing_blocks(HttpConnection *conn,
                                         guint64 start_block,
                                         guint64 end_block,
                                         GError **err) {
  if (end_block < start_block) {
    return true;
  }

  guint64 idx = start_block;
  while (idx <= end_block) {
    guint64 miss_start = G_MAXUINT64;
    guint64 miss_end = 0;

    g_mutex_lock(&conn->cache_mutex);
    while (idx <= end_block && block_is_ready_locked(conn, idx)) {
      idx++;
    }
    if (idx <= end_block) {
      miss_start = idx;
      miss_end = idx;
      while (miss_end + 1 <= end_block && !block_is_ready_locked(conn, miss_end + 1)) {
        miss_end++;
      }
    }
    g_mutex_unlock(&conn->cache_mutex);

    if (miss_start == G_MAXUINT64) {
      break;
    }

    if (!http_fetch_extent(conn, miss_start, miss_end, err)) {
      return false;
    }

    idx = miss_end + 1;
  }

  return true;
}

static void http_plan_prefetch_blocks(HttpConnection *conn,
                                      uint64_t offset,
                                      size_t to_read,
                                      guint64 *io_start_block,
                                      guint64 *io_end_block) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();
  guint64 start_block = *io_start_block;
  guint64 end_block = *io_end_block;

  if (to_read == 0 || conn->file_size == 0) {
    return;
  }

  if (offset < HTTP_METADATA_WINDOW_BYTES) {
    guint64 metadata_end = MIN((uint64_t) HTTP_METADATA_WINDOW_BYTES,
                               conn->file_size);
    if (metadata_end > 0) {
      guint64 metadata_end_block = (metadata_end - 1) / cfg->block_size;
      if (end_block < metadata_end_block) {
        end_block = metadata_end_block;
      }
    }
  } else if (to_read <= HTTP_SMALL_READ_THRESHOLD_BYTES) {
    guint64 extra_end_offset = MIN(offset + to_read + HTTP_SMALL_READ_AHEAD_BYTES,
                                   conn->file_size);
    if (extra_end_offset > 0) {
      guint64 extra_end_block = (extra_end_offset - 1) / cfg->block_size;
      if (end_block < extra_end_block) {
        end_block = extra_end_block;
      }
    }
  }

  *io_start_block = start_block;
  *io_end_block = end_block;
}

static size_t http_copy_cached_bytes(HttpConnection *conn,
                                     uint64_t offset,
                                     uint8_t *dst,
                                     size_t to_read,
                                     GError **err) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();
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
        lru_promote(conn, b);
        g_mutex_unlock(&conn->cache_mutex);
        return b;
      }
      /* Fetch failed and the entry may already have been removed. */
      b = g_hash_table_lookup(conn->block_map, GUINT_TO_POINTER(block_idx));
      if (!b) {
        /* Fall through and recreate a clean placeholder. */
      }
    }
  }

  /* Cache miss - create placeholder and mark as fetching */
  stats_record_cache(FALSE);
  
  if (!b) {
    b = g_new0(HttpBlock, 1);
    g_cond_init(&b->fetch_cond);
    
    /* Evict if needed */
    while (cfg->max_cache_blocks > 0 && conn->cache_count >= cfg->max_cache_blocks) {
      lru_evict(conn);
    }
    
    lru_insert_front(conn, b, block_idx);
    g_hash_table_insert(conn->block_map, GUINT_TO_POINTER(block_idx), b);
    conn->cache_count++;
  }
  
  b->state = BLOCK_STATE_FETCHING;
  g_mutex_unlock(&conn->cache_mutex);

  /* Fetch data outside lock */
  uint64_t offset = block_idx * cfg->block_size;
  size_t len = cfg->block_size;
  
  if (offset >= conn->file_size) {
    g_mutex_lock(&conn->cache_mutex);
    g_cond_broadcast(&b->fetch_cond);
    block_remove_locked(conn, block_idx, b);
    g_mutex_unlock(&conn->cache_mutex);
    return NULL;
  }
  if (offset + len > conn->file_size) {
    len = (size_t)(conn->file_size - offset);
  }

  uint8_t *data = g_malloc(len);
  size_t written = 0;
  
  if (!http_fetch_range(conn, offset, len, data, &written, NULL, err) || written == 0) {
    g_free(data);
    g_mutex_lock(&conn->cache_mutex);
    g_cond_broadcast(&b->fetch_cond);
    block_remove_locked(conn, block_idx, b);
    g_mutex_unlock(&conn->cache_mutex);
    return NULL;
  }

  /* Store data and signal waiters */
  g_mutex_lock(&conn->cache_mutex);
  /* Free old data if any (shouldn't happen but be safe) */
  if (b->data) {
    g_free(b->data);
  }
  b->data = data;
  b->len = written;
  b->state = BLOCK_STATE_READY;
  lru_promote(conn, b);
  g_cond_broadcast(&b->fetch_cond);
  g_mutex_unlock(&conn->cache_mutex);

  return b;
}

/* ==============================
 * Connection Pool Helpers
 * ============================== */

static GList *http_pool_collect_expired_locked(void) {
  GList *to_destroy = NULL;
  if (!conn_pool) {
    return NULL;
  }

  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();
  if (cfg->pool_ttl_sec <= 0) {
    return NULL;
  }

  gint64 now = g_get_monotonic_time();
  gint64 ttl_us = (gint64) cfg->pool_ttl_sec * G_USEC_PER_SEC;

  GHashTableIter iter;
  gpointer key, value;
  g_hash_table_iter_init(&iter, conn_pool);
  while (g_hash_table_iter_next(&iter, &key, &value)) {
    HttpConnection *conn = value;
    gboolean should_remove = FALSE;

    g_mutex_lock(&conn->mutex);
    if (conn->ref_count == 0 && conn->state == CONN_STATE_READY &&
        now - conn->last_used_us >= ttl_us) {
      conn->state = CONN_STATE_CLOSING;
      should_remove = TRUE;
    }
    g_mutex_unlock(&conn->mutex);

    if (should_remove) {
      g_hash_table_iter_remove(&iter);
      to_destroy = g_list_prepend(to_destroy, conn);
    }
  }

  return to_destroy;
}

static void http_destroy_connection_list(GList *list) {
  for (GList *l = list; l != NULL; l = l->next) {
    http_connection_destroy((HttpConnection *) l->data);
  }
  g_list_free(list);
}

/* ==============================
 * Connection Management
 * ============================== */

static void http_connection_destroy(HttpConnection *conn) {
  if (!conn) {
    return;
  }

  /* Free cache */
  g_mutex_lock(&conn->cache_mutex);
  if (conn->block_map) {
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, conn->block_map);
    while (g_hash_table_iter_next(&iter, &key, &value)) {
      http_block_free((HttpBlock *)value);
    }
    g_hash_table_destroy(conn->block_map);
    conn->block_map = NULL;
  }
  g_free(conn->front_window_data);
  conn->front_window_data = NULL;
  conn->front_window_len = 0;
  conn->lru_head = NULL;
  conn->lru_tail = NULL;
  g_mutex_unlock(&conn->cache_mutex);

  g_free(conn->uri);
  conn->uri = NULL;
  g_mutex_clear(&conn->mutex);
  g_mutex_clear(&conn->cache_mutex);
  g_cond_clear(&conn->state_cond);
  g_free(conn);
}

static void http_connection_unref(HttpConnection *conn) {
  if (!conn) {
    return;
  }

  ensure_pool_mutex_init();
  g_mutex_lock(&pool_mutex);
  g_mutex_lock(&conn->mutex);

  if (conn->ref_count <= 0) {
    g_mutex_unlock(&conn->mutex);
    g_mutex_unlock(&pool_mutex);
    return;
  }

  conn->ref_count--;
  if (conn->ref_count == 0) {
    conn->last_used_us = g_get_monotonic_time();
    if (conn->state == CONN_STATE_CLOSING || conn->state == CONN_STATE_ERROR) {
      if (conn_pool) {
        HttpConnection *pooled = g_hash_table_lookup(conn_pool, conn->uri);
        if (pooled == conn) {
          g_hash_table_remove(conn_pool, conn->uri);
        }
      }
      g_mutex_unlock(&conn->mutex);
      g_mutex_unlock(&pool_mutex);

#if HTTP_STATS
      /* Print per-connection stats */
      gint reqs = g_atomic_int_get(&conn->range_requests);
      gint bytes_kb = g_atomic_int_get(&conn->range_bytes_kb);
      gint time_ms = g_atomic_int_get(&conn->range_time_ms);
      if (reqs > 0) {
        HTTP_LOG("[HTTP-STATS] %s | requests=%d bytes=%dKB time=%dms avg=%.2fms/req\n",
                 conn->uri, reqs, bytes_kb, time_ms, (double)time_ms / reqs);
      }
#endif
      http_connection_destroy(conn);
      return;
    }
  }

  g_mutex_unlock(&conn->mutex);
  g_mutex_unlock(&pool_mutex);
}


static void http_store_buffer_locked(HttpConnection *conn,
                                     guint64 start_block,
                                     const uint8_t *buffer,
                                     size_t written) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();
  if (written == 0) {
    return;
  }

  guint64 end_block = start_block + (guint64) ((written - 1) / cfg->block_size);
  for (guint64 idx = start_block; idx <= end_block; idx++) {
    size_t block_start = (size_t) ((idx - start_block) * cfg->block_size);
    size_t block_end = block_start + cfg->block_size;
    if (block_end > written) {
      block_end = written;
    }
    if (block_start >= written) {
      break;
    }

    HttpBlock *b = g_hash_table_lookup(conn->block_map, GUINT_TO_POINTER(idx));
    if (!b) {
      while (cfg->max_cache_blocks > 0 && conn->cache_count >= cfg->max_cache_blocks) {
        lru_evict(conn);
      }
      b = g_new0(HttpBlock, 1);
      g_cond_init(&b->fetch_cond);
      lru_insert_front(conn, b, idx);
      g_hash_table_insert(conn->block_map, GUINT_TO_POINTER(idx), b);
      conn->cache_count++;
    }

    size_t block_len = block_end - block_start;
    g_free(b->data);
    b->data = g_malloc(block_len);
    memcpy(b->data, buffer + block_start, block_len);
    b->len = block_len;
    b->state = BLOCK_STATE_READY;
    lru_promote(conn, b);
    g_cond_broadcast(&b->fetch_cond);
  }
}

/* Prime connection by issuing one real range GET for the first chunk.
 * This both discovers file size and warms the cache for metadata / DZI. */
static bool http_prime_connection(HttpConnection *conn, uint64_t *out_size, GError **err) {
  if (!http_fill_front_window(conn, HTTP_FRONT_WINDOW_BYTES, err)) {
    return false;
  }

  g_mutex_lock(&conn->cache_mutex);
  *out_size = conn->file_size > 0 ? conn->file_size : conn->front_window_len;
  g_mutex_unlock(&conn->cache_mutex);

  HTTP_LOG("[HTTP-PRIME] %s size=%" PRIu64 " front_window=%zuB\n",
           conn->uri, *out_size, conn->front_window_len);
  return *out_size > 0;
}

static HttpConnection *http_connection_create(const char *uri, GError **err G_GNUC_UNUSED) {
  HttpConnection *conn = g_new0(HttpConnection, 1);
  conn->uri = g_strdup(uri);
  conn->ref_count = 1;
  conn->last_used_us = g_get_monotonic_time();
  conn->state = CONN_STATE_INIT;
  
  g_mutex_init(&conn->mutex);
  g_mutex_init(&conn->cache_mutex);
  g_cond_init(&conn->state_cond);
  
  conn->block_map = g_hash_table_new(g_direct_hash, g_direct_equal);
  conn->lru_head = NULL;
  conn->lru_tail = NULL;
  
  return conn;
}

static HttpConnection *http_connection_get_or_create(const char *uri, GError **err) {
  ensure_pool_mutex_init();
  _openslide_http_init();

  /* Quick check with pool lock */
  g_mutex_lock(&pool_mutex);
  GList *expired = http_pool_collect_expired_locked();
  
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
      conn->last_used_us = g_get_monotonic_time();
      HTTP_LOG("[HTTP-POOL] Reuse conn=%p ref=%d uri=%s\n",
               (void *) conn, conn->ref_count, uri);
      g_mutex_unlock(&conn->mutex);
      g_mutex_unlock(&pool_mutex);
      http_destroy_connection_list(expired);
      return conn;
    }
    
    if (conn->state == CONN_STATE_CLOSING) {
      g_mutex_unlock(&conn->mutex);
      g_mutex_unlock(&pool_mutex);
      http_destroy_connection_list(expired);
      g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                  "Connection is closing for %s", uri);
      return NULL;
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
    http_destroy_connection_list(expired);
    return NULL;
  }
  
  conn->state = CONN_STATE_CONNECTING;
  conn->last_used_us = g_get_monotonic_time();
  g_hash_table_insert(conn_pool, g_strdup(uri), conn);
  
  g_mutex_unlock(&pool_mutex);
  http_destroy_connection_list(expired);

  /* Do network operation OUTSIDE pool_mutex */
  uint64_t file_size = 0;
  bool size_ok = http_prime_connection(conn, &file_size, err);

  g_mutex_lock(&conn->mutex);
  if (size_ok && file_size > 0) {
    conn->file_size = file_size;
    conn->last_used_us = g_get_monotonic_time();
    conn->state = CONN_STATE_READY;
    HTTP_LOG("[HTTP-POOL] New conn=%p size=%" PRIu64 " uri=%s\n",
             (void *) conn, file_size, uri);
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

static bool http_get_request_block_range(HttpConnection *conn,
                                         uint64_t offset,
                                         size_t size,
                                         guint64 *out_start_block,
                                         guint64 *out_end_block,
                                         size_t *out_to_read) {
  const OpenslideHTTPConfig *cfg = _openslide_http_get_config();
  if (offset >= conn->file_size) {
    if (out_to_read) {
      *out_to_read = 0;
    }
    return false;
  }

  size_t to_read = MIN(size, (size_t) (conn->file_size - offset));
  if (to_read == 0) {
    if (out_to_read) {
      *out_to_read = 0;
    }
    return false;
  }

  if (out_start_block) {
    *out_start_block = offset / cfg->block_size;
  }
  if (out_end_block) {
    *out_end_block = (offset + to_read - 1) / cfg->block_size;
  }
  if (out_to_read) {
    *out_to_read = to_read;
  }
  return true;
}

static bool http_ensure_cached_range(HttpConnection *conn,
                                     uint64_t offset,
                                     size_t size,
                                     GError **err,
                                     size_t *out_to_read) {
  guint64 start_block = 0, end_block = 0;
  size_t to_read = 0;
  if (!http_get_request_block_range(conn, offset, size,
                                    &start_block, &end_block, &to_read)) {
    if (out_to_read) {
      *out_to_read = 0;
    }
    return true;
  }

  if (offset < HTTP_FRONT_WINDOW_MAX_BYTES) {
    uint64_t request_end = offset + to_read;
    size_t want = HTTP_FRONT_WINDOW_BYTES;
    if (request_end > want) {
      want = (size_t) MIN((uint64_t) HTTP_FRONT_WINDOW_MAX_BYTES, request_end);
    }
    if (!http_fill_front_window(conn, want, err)) {
      return false;
    }
    g_mutex_lock(&conn->cache_mutex);
    gboolean covered = http_front_window_contains(conn, offset, to_read);
    g_mutex_unlock(&conn->cache_mutex);
    if (covered) {
      if (out_to_read) {
        *out_to_read = to_read;
      }
      return true;
    }
  }

  http_plan_prefetch_blocks(conn, offset, to_read, &start_block, &end_block);

  if (out_to_read) {
    *out_to_read = to_read;
  }
  return http_prefetch_missing_blocks(conn, start_block, end_block, err);
}

size_t _openslide_http_read(struct _openslide_http_file *file,
                            void *buf, size_t size, GError **err) {
  if (!file || !buf || size == 0) {
    return 0;
  }

  HttpConnection *conn = file->conn;
  uint8_t *dst = (uint8_t *) buf;

  int64_t pos = file->position;
  size_t to_read = 0;
  if (!http_ensure_cached_range(conn, (uint64_t) pos, size, err, &to_read)) {
    return 0;
  }
  if (to_read == 0) {
    return 0;
  }

  size_t total = 0;
  g_mutex_lock(&conn->cache_mutex);
  if (http_front_window_contains(conn, (uint64_t) pos, to_read)) {
    memcpy(dst, conn->front_window_data + pos, to_read);
    total = to_read;
  }
  g_mutex_unlock(&conn->cache_mutex);

  if (total == 0) {
    total = http_copy_cached_bytes(conn, (uint64_t) pos, dst, to_read, err);
  }

  file->position += (int64_t) total;
  file->last_read_end = file->position;
  return total;
}

/* Read exact number of bytes, error if less */
bool _openslide_http_read_exact(struct _openslide_http_file *file,
                                void *buf, size_t size, GError **err) {
  size_t n = _openslide_http_read(file, buf, size, err);
  if (n < size) {
    if (err && !*err) {
      g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                  "Short read: requested %zu bytes, got %zu", size, n);
    }
    return false;
  }
  return true;
}

/* Zero-copy pread */
size_t _openslide_http_pread(struct _openslide_http_file *file,
                             uint64_t offset, void *buf, size_t size,
                             GError **err) {
  if (!file || !buf || size == 0) {
    return 0;
  }

  HttpConnection *conn = file->conn;
  uint8_t *dst = (uint8_t *) buf;

  size_t to_read = 0;
  if (!http_ensure_cached_range(conn, offset, size, err, &to_read)) {
    return 0;
  }
  if (to_read == 0) {
    return 0;
  }

  g_mutex_lock(&conn->cache_mutex);
  if (http_front_window_contains(conn, offset, to_read)) {
    memcpy(dst, conn->front_window_data + offset, to_read);
    g_mutex_unlock(&conn->cache_mutex);
    return to_read;
  }
  g_mutex_unlock(&conn->cache_mutex);

  return http_copy_cached_bytes(conn, offset, dst, to_read, err);
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

  size_t to_read = 0;
  if (!http_ensure_cached_range(conn, offset, 1, err, &to_read)) {
    return false;
  }
  if (to_read == 0) {
    *out_ptr = NULL;
    *out_avail = 0;
    return true;
  }

  g_mutex_lock(&conn->cache_mutex);
  if (http_front_window_view(conn, offset, out_ptr, out_avail)) {
    g_mutex_unlock(&conn->cache_mutex);
    return true;
  }
  g_mutex_unlock(&conn->cache_mutex);

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
  /* Transport metrics - always available for debugging connection reuse */
  gint total = g_atomic_int_get(&g_downloader.total_requests);
  gint reused = g_atomic_int_get(&g_downloader.reused_connections);
  gint newconn = g_atomic_int_get(&g_downloader.new_connections);
  gint h2 = g_atomic_int_get(&g_downloader.http2_streams);

  fprintf(stderr, "[HTTP-TRANSPORT] total_requests=%d new_tcp_connections=%d reused_connections=%d reuse_rate=%.1f%%\n",
          total, newconn, reused,
          total > 0 ? 100.0 * reused / total : 0.0);
  fprintf(stderr, "[HTTP-TRANSPORT] http2_streams=%d http2_rate=%.1f%%\n",
          h2, total > 0 ? 100.0 * h2 / total : 0.0);

#if HTTP_STATS
  gint reqs = g_atomic_int_get(&global_stats.total_requests);
  gint bytes_kb = g_atomic_int_get(&global_stats.total_bytes_kb);
  gint time_ms = g_atomic_int_get(&global_stats.total_time_ms);
  gint hits = g_atomic_int_get(&global_stats.cache_hits);
  gint misses = g_atomic_int_get(&global_stats.cache_misses);
  gint stat_reused = g_atomic_int_get(&global_stats.connection_reuses);
  gint stat_newconn = g_atomic_int_get(&global_stats.new_connections);

  fprintf(stderr, "[HTTP-GLOBAL-STATS] requests=%d bytes=%dKB time=%dms avg=%.2fms/req\n",
          reqs, bytes_kb, time_ms, 
          reqs > 0 ? (double)time_ms / reqs : 0.0);
  fprintf(stderr, "[HTTP-GLOBAL-STATS] cache_hits=%d cache_misses=%d hit_rate=%.1f%%\n",
          hits, misses, 
          (hits + misses) > 0 ? 100.0 * hits / (hits + misses) : 0.0);
  fprintf(stderr, "[HTTP-GLOBAL-STATS] conn_reused=%d conn_new=%d reuse_rate=%.1f%%\n",
          stat_reused, stat_newconn,
          (stat_reused + stat_newconn) > 0 ? 100.0 * stat_reused / (stat_reused + stat_newconn) : 0.0);
#endif
}
