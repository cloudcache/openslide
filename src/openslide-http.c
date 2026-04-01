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

/* ==============================
 * Tunables — Sparse Cache + Adaptive Prefetch
 * ============================== */

/* Sparse cache limits */
#define SC_MAX_REGIONS          512              /* Max cached regions */
#define SC_MAX_BYTES_DEFAULT    (256 * 1024 * 1024) /* 256MB default */
#define SC_MERGE_GAP_BYTES      (64 * 1024)      /* Auto-merge gap */
#define SC_MIN_FETCH_BYTES      (256 * 1024)     /* Minimum HTTP fetch */

/* Adaptive prefetch */
#define AP_INITIAL_READAHEAD    (256 * 1024)     /* 256KB initial */
#define AP_MAX_READAHEAD        (16 * 1024 * 1024) /* 16MB max */
#define AP_SEQ_THRESHOLD        3                /* Ramp-up threshold */

/* Connection priming */
#define PRIME_FRONT_BYTES       (4 * 1024 * 1024)  /* 4MB from start */
#define PRIME_TAIL_BYTES        (4 * 1024 * 1024)  /* 4MB from end */
#define PRIME_ADAPTIVE_DIVISOR  50               /* ~2% of file */
#define PRIME_ADAPTIVE_MAX      (32 * 1024 * 1024) /* 32MB cap */

/* In-flight dedup */
#define MAX_INFLIGHT_RANGES     16

/* Gap coalescing for cache miss handler */
#define COALESCE_MAX_GAP        (64 * 1024)      /* Merge gaps < 64KB */
#define COALESCE_MAX_RANGES     8                /* Max gap slots */
#define COALESCE_MAX_SPAN       (8 * 1024 * 1024) /* Max merged span */

/* ==============================
 * Internal Data Structures
 * ============================== */

/* --- Sparse Cache: interval-based range cache --- */

typedef struct {
  uint64_t offset;      /* start byte offset in file */
  size_t   len;         /* length of cached data */
  uint8_t *data;        /* owned buffer */
  gint64   last_access; /* monotonic time for LRU */
} CachedRegion;

typedef struct {
  CachedRegion *regions;    /* sorted array by offset */
  guint         count;      /* number of regions */
  guint         capacity;   /* allocated slots */
  size_t        total_bytes;/* sum of all region lengths */
  size_t        max_bytes;  /* eviction threshold */
} SparseCache;

typedef struct {
  uint64_t offset;
  size_t   len;
} CacheGap;

/* In-flight fetch tracking for dedup */
typedef struct {
  uint64_t offset;
  size_t   len;
  gboolean active;
  GCond    cond;
} InflightRange;

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
  CONN_STATE_CONNECTING,
  CONN_STATE_READY,
  CONN_STATE_ERROR,
  CONN_STATE_CLOSING
} ConnState;

/* Shared connection info — one per URI */
typedef struct _http_connection {
  gchar *uri;
  uint64_t file_size;

  /* Unified sparse range cache */
  SparseCache cache;
  GMutex cache_mutex;

  /* In-flight fetch dedup */
  InflightRange inflight[MAX_INFLIGHT_RANGES];

  ConnState state;
  GCond state_cond;

  /* CURL handle pool */
  CurlHandle curl_pool[MAX_CURL_HANDLES];
  GMutex curl_mutex;

  gint ref_count;
  gint64 last_used_us;
  GMutex mutex;

#if HTTP_STATS
  volatile gint range_requests;
  volatile gint range_bytes_kb;
  volatile gint range_time_ms;
#endif
} HttpConnection;

/* Per-open file handle with adaptive prefetch state */
struct _openslide_http_file {
  HttpConnection *conn;
  int64_t position;

  /* Adaptive prefetch tracking */
  int64_t  last_read_end;
  uint32_t sequential_count;
  size_t   readahead_size;
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

/* Throttle expired-connection scans: run at most once per interval */
#define HTTP_EXPIRED_SCAN_INTERVAL_US  (5 * G_USEC_PER_SEC)
static gint64 last_expired_scan_us = 0;

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

/* Forward declarations */
static void async_downloader_init(void);
static void async_downloader_shutdown(void);
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
#ifdef CURLMOPT_MAX_HOST_CONNECTIONS
      curl_multi_setopt(g_downloader.multi_handle, CURLMOPT_MAX_HOST_CONNECTIONS, HTTP_ASYNC_SLOTS);
#endif
#ifdef CURLMOPT_MAX_TOTAL_CONNECTIONS
      curl_multi_setopt(g_downloader.multi_handle, CURLMOPT_MAX_TOTAL_CONNECTIONS, HTTP_ASYNC_SLOTS * 4L);
#endif
      for (int i = 0; i < HTTP_ASYNC_SLOTS; i++) {
        g_downloader.slots[i].easy = curl_easy_init();
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

/* ==============================
 * HTTP Range Fetch (via async downloader)
 * ============================== */

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

/* Fetch multiple ranges in parallel using the shared curl_multi downloader. */
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
 * Sparse Cache Implementation
 *
 * Unified interval-based range cache replacing separate front_window,
 * tail_window, and block_map.  Regions are sorted by offset, auto-merged
 * on insert, and evicted via LRU when the cache exceeds its byte limit.
 * ============================== */

static void sparse_cache_init(SparseCache *sc, size_t max_bytes) {
  sc->regions = NULL;
  sc->count = 0;
  sc->capacity = 0;
  sc->total_bytes = 0;
  sc->max_bytes = max_bytes;
}

static void sparse_cache_destroy(SparseCache *sc) {
  for (guint i = 0; i < sc->count; i++) {
    g_free(sc->regions[i].data);
  }
  g_free(sc->regions);
  sc->regions = NULL;
  sc->count = 0;
  sc->capacity = 0;
  sc->total_bytes = 0;
}

/* Binary search: find first region whose end (offset+len) > target_offset.
 * That is the first region that could contain target_offset. */
static guint sparse_cache_find(const SparseCache *sc, uint64_t offset) {
  guint lo = 0, hi = sc->count;
  while (lo < hi) {
    guint mid = lo + (hi - lo) / 2;
    if (sc->regions[mid].offset + sc->regions[mid].len <= offset) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return lo;
}

/* Check if [offset, offset+size) is fully contained in one cached region.
 * Must hold cache_mutex. */
static gboolean sparse_cache_contains(SparseCache *sc, uint64_t offset, size_t size) {
  if (size == 0) return TRUE;
  guint idx = sparse_cache_find(sc, offset);
  if (idx >= sc->count) return FALSE;
  CachedRegion *r = &sc->regions[idx];
  return (r->offset <= offset && r->offset + r->len >= offset + size);
}

/* Return a zero-copy view into cached data at offset.
 * Sets *out_ptr and *out_avail (bytes available from that pointer).
 * Returns TRUE if data is available at offset.  Must hold cache_mutex. */
static gboolean sparse_cache_view(SparseCache *sc, uint64_t offset,
                                  const uint8_t **out_ptr, size_t *out_avail) {
  guint idx = sparse_cache_find(sc, offset);
  if (idx >= sc->count) return FALSE;
  CachedRegion *r = &sc->regions[idx];
  if (offset < r->offset || offset >= r->offset + r->len) return FALSE;
  size_t local_off = (size_t)(offset - r->offset);
  *out_ptr = r->data + local_off;
  *out_avail = r->len - local_off;
  r->last_access = g_get_monotonic_time();
  return TRUE;
}

static void sparse_cache_grow(SparseCache *sc) {
  if (sc->count < sc->capacity) return;
  guint new_cap = sc->capacity == 0 ? 16 : sc->capacity * 2;
  if (new_cap > SC_MAX_REGIONS * 2) new_cap = SC_MAX_REGIONS * 2;
  sc->regions = g_realloc(sc->regions, new_cap * sizeof(CachedRegion));
  sc->capacity = new_cap;
}

/* Evict the least recently accessed region.  Returns bytes freed. */
static size_t sparse_cache_evict_one(SparseCache *sc) {
  if (sc->count == 0) return 0;

  guint lru_idx = 0;
  gint64 oldest = sc->regions[0].last_access;
  for (guint i = 1; i < sc->count; i++) {
    if (sc->regions[i].last_access < oldest) {
      oldest = sc->regions[i].last_access;
      lru_idx = i;
    }
  }

  size_t freed = sc->regions[lru_idx].len;
  g_free(sc->regions[lru_idx].data);

  if (lru_idx + 1 < sc->count) {
    memmove(&sc->regions[lru_idx], &sc->regions[lru_idx + 1],
            (sc->count - lru_idx - 1) * sizeof(CachedRegion));
  }
  sc->count--;
  sc->total_bytes -= freed;
  return freed;
}

/* Insert [offset, offset+len) into the cache.
 * TAKES OWNERSHIP of `data` — caller must NOT free it afterward.
 * When the new range overlaps or is adjacent (within SC_MERGE_GAP_BYTES) to
 * existing regions, they are merged into a single region.
 * Must hold cache_mutex. */
static void sparse_cache_insert(SparseCache *sc, uint64_t offset,
                                uint8_t *data, size_t len) {
  if (len == 0) {
    g_free(data);
    return;
  }

  gint64 now = g_get_monotonic_time();
  uint64_t new_end = offset + len;

  /* Find regions that overlap or are adjacent within merge gap */
  guint merge_first = sc->count;
  guint merge_last = 0;
  gboolean found_merge = FALSE;

  for (guint i = 0; i < sc->count; i++) {
    uint64_t r_start = sc->regions[i].offset;
    uint64_t r_end = r_start + sc->regions[i].len;

    gboolean overlaps = (offset <= r_end + SC_MERGE_GAP_BYTES) &&
                        (r_start <= new_end + SC_MERGE_GAP_BYTES);
    if (overlaps) {
      if (!found_merge) {
        merge_first = i;
        found_merge = TRUE;
      }
      merge_last = i + 1;
    }
  }

  if (!found_merge) {
    /* No overlap — evict if needed, then insert standalone region */
    while (sc->max_bytes > 0 && sc->total_bytes + len > sc->max_bytes) {
      if (sparse_cache_evict_one(sc) == 0) break;
    }
    while (sc->count >= SC_MAX_REGIONS) {
      sparse_cache_evict_one(sc);
    }

    sparse_cache_grow(sc);

    guint ins = 0;
    while (ins < sc->count && sc->regions[ins].offset < offset) {
      ins++;
    }
    if (ins < sc->count) {
      memmove(&sc->regions[ins + 1], &sc->regions[ins],
              (sc->count - ins) * sizeof(CachedRegion));
    }

    sc->regions[ins].offset = offset;
    sc->regions[ins].len = len;
    sc->regions[ins].data = data;
    sc->regions[ins].last_access = now;
    sc->count++;
    sc->total_bytes += len;
    return;
  }

  /* Fast path: new data fully contained within one existing region */
  if (merge_last - merge_first == 1) {
    CachedRegion *r = &sc->regions[merge_first];
    if (r->offset <= offset && r->offset + r->len >= new_end) {
      size_t local_off = (size_t)(offset - r->offset);
      memcpy(r->data + local_off, data, len);
      r->last_access = now;
      g_free(data);
      return;
    }
  }

  /* General merge: compute merged range, allocate merged buffer */
  uint64_t merged_start = MIN(offset, sc->regions[merge_first].offset);
  uint64_t merged_end = new_end;
  for (guint i = merge_first; i < merge_last; i++) {
    uint64_t r_end = sc->regions[i].offset + sc->regions[i].len;
    if (r_end > merged_end) merged_end = r_end;
  }

  size_t merged_len = (size_t)(merged_end - merged_start);
  uint8_t *merged_data = g_malloc0(merged_len);

  /* Copy existing regions into merged buffer first */
  for (guint i = merge_first; i < merge_last; i++) {
    size_t dst_off = (size_t)(sc->regions[i].offset - merged_start);
    memcpy(merged_data + dst_off, sc->regions[i].data, sc->regions[i].len);
    sc->total_bytes -= sc->regions[i].len;
    g_free(sc->regions[i].data);
  }

  /* Copy new data on top (takes priority in overlapping areas) */
  size_t new_dst_off = (size_t)(offset - merged_start);
  memcpy(merged_data + new_dst_off, data, len);
  g_free(data);

  /* Replace merged regions with single merged region */
  sc->regions[merge_first].offset = merged_start;
  sc->regions[merge_first].len = merged_len;
  sc->regions[merge_first].data = merged_data;
  sc->regions[merge_first].last_access = now;

  guint removed = merge_last - merge_first - 1;
  if (removed > 0 && merge_last < sc->count) {
    memmove(&sc->regions[merge_first + 1], &sc->regions[merge_last],
            (sc->count - merge_last) * sizeof(CachedRegion));
  }
  sc->count -= removed;
  sc->total_bytes += merged_len;

  /* Evict if over limit */
  while (sc->max_bytes > 0 && sc->total_bytes > sc->max_bytes && sc->count > 1) {
    sparse_cache_evict_one(sc);
  }
}

/* Find uncached gaps within [offset, offset+size).
 * Returns number of gaps written to `gaps` array.
 * Must hold cache_mutex. */
static guint sparse_cache_find_gaps(SparseCache *sc, uint64_t offset, size_t size,
                                    CacheGap *gaps, guint max_gaps) {
  if (size == 0 || max_gaps == 0) return 0;

  uint64_t end = offset + size;
  uint64_t cursor = offset;
  guint gap_count = 0;
  guint idx = sparse_cache_find(sc, offset);

  while (cursor < end && gap_count < max_gaps) {
    /* Skip past any region that covers cursor */
    while (idx < sc->count &&
           sc->regions[idx].offset <= cursor &&
           sc->regions[idx].offset + sc->regions[idx].len > cursor) {
      cursor = sc->regions[idx].offset + sc->regions[idx].len;
      idx++;
    }
    if (cursor >= end) break;

    /* cursor is in a gap — find where the next region starts */
    uint64_t gap_end = end;
    if (idx < sc->count && sc->regions[idx].offset < gap_end) {
      gap_end = sc->regions[idx].offset;
    }

    if (gap_end > cursor) {
      gaps[gap_count].offset = cursor;
      gaps[gap_count].len = (size_t)(gap_end - cursor);
      gap_count++;
    }
    cursor = gap_end;
  }

  return gap_count;
}

/* ==============================
 * In-Flight Fetch Dedup
 *
 * Prevents duplicate HTTP requests when multiple threads hit the same
 * cache miss simultaneously.  A thread registers its fetch range before
 * starting the download; other threads finding an overlapping range wait
 * on the condition variable instead of issuing a redundant request.
 * ============================== */

/* Check if any in-flight fetch overlaps [offset, offset+len).
 * If so, wait for it to complete and return TRUE (caller should re-check cache).
 * Must hold cache_mutex. */
static gboolean inflight_wait_if_overlapping(HttpConnection *conn,
                                             uint64_t offset, size_t len) {
  uint64_t req_end = offset + len;
  for (int i = 0; i < MAX_INFLIGHT_RANGES; i++) {
    if (conn->inflight[i].active) {
      uint64_t inf_end = conn->inflight[i].offset + conn->inflight[i].len;
      if (conn->inflight[i].offset < req_end && inf_end > offset) {
        while (conn->inflight[i].active) {
          g_cond_wait(&conn->inflight[i].cond, &conn->cache_mutex);
        }
        return TRUE;
      }
    }
  }
  return FALSE;
}

/* Register an in-flight fetch.  Returns slot index or -1 if no slot. */
static int inflight_register(HttpConnection *conn, uint64_t offset, size_t len) {
  for (int i = 0; i < MAX_INFLIGHT_RANGES; i++) {
    if (!conn->inflight[i].active) {
      conn->inflight[i].offset = offset;
      conn->inflight[i].len = len;
      conn->inflight[i].active = TRUE;
      return i;
    }
  }
  return -1;
}

/* Complete an in-flight fetch, waking all waiters. */
static void inflight_complete(HttpConnection *conn, int slot) {
  if (slot >= 0 && slot < MAX_INFLIGHT_RANGES) {
    conn->inflight[slot].active = FALSE;
    g_cond_broadcast(&conn->inflight[slot].cond);
  }
}

/* ==============================
 * Gap Coalescing + Adaptive Prefetch
 * ============================== */

/* Coalesce nearby gaps into fewer, larger gaps.
 * Input: sorted array of gaps.  Returns new gap count. */
static guint coalesce_gaps(CacheGap *gaps, guint count) {
  if (count <= 1) return count;
  guint out = 0;
  for (guint i = 1; i < count; i++) {
    uint64_t prev_end = gaps[out].offset + gaps[out].len;
    uint64_t new_end = gaps[i].offset + gaps[i].len;
    size_t span = (size_t)(new_end - gaps[out].offset);
    size_t inter_gap = 0;
    if (gaps[i].offset > prev_end) {
      inter_gap = (size_t)(gaps[i].offset - prev_end);
    }
    if (inter_gap <= COALESCE_MAX_GAP && span <= COALESCE_MAX_SPAN) {
      gaps[out].len = (size_t)(new_end - gaps[out].offset);
    } else {
      out++;
      if (out != i) gaps[out] = gaps[i];
    }
  }
  return out + 1;
}

/* Compute effective read-ahead size for a file handle at a given offset */
static size_t compute_readahead(struct _openslide_http_file *file, uint64_t offset) {
  if (!file) return AP_INITIAL_READAHEAD;
  HttpConnection *conn = file->conn;
  size_t base = file->readahead_size;

  /* Near file start (metadata region): boost */
  if (offset < 1024 * 1024) {
    if (base < 2 * 1024 * 1024) base = 2 * 1024 * 1024;
  }

  /* Near file end: extend to cover remainder */
  if (conn->file_size > 0 && offset + base + 4 * 1024 * 1024 >= conn->file_size) {
    size_t to_end = (size_t)(conn->file_size - offset);
    if (to_end > base) base = to_end;
  }

  return MIN(base, (size_t) AP_MAX_READAHEAD);
}

/* ==============================
 * Cache-Miss Handler
 *
 * Central entry point for ensuring a byte range is cached.  Handles:
 * 1. Fast-path cache hit
 * 2. In-flight dedup (wait for overlapping fetch)
 * 3. Adaptive read-ahead sizing
 * 4. Gap discovery + coalescing
 * 5. Minimum fetch size enforcement
 * 6. Single or parallel HTTP fetch
 * 7. Sparse cache insertion (with auto-merge)
 * ============================== */

static bool http_cache_ensure(HttpConnection *conn,
                              struct _openslide_http_file *file,
                              uint64_t offset, size_t size,
                              GError **err) {
  if (size == 0 || offset >= conn->file_size) return true;
  size_t capped = MIN(size, (size_t)(conn->file_size - offset));

retry:
  g_mutex_lock(&conn->cache_mutex);

  /* Fast path: already cached */
  if (sparse_cache_contains(&conn->cache, offset, capped)) {
    g_mutex_unlock(&conn->cache_mutex);
    return true;
  }

  /* Wait if an in-flight fetch covers our range */
  if (inflight_wait_if_overlapping(conn, offset, capped)) {
    if (sparse_cache_contains(&conn->cache, offset, capped)) {
      g_mutex_unlock(&conn->cache_mutex);
      return true;
    }
    /* Data still not cached (fetch may have failed).  Fall through. */
  }

  /* Compute read-ahead and extended range */
  size_t readahead = compute_readahead(file, offset);
  size_t fetch_size = capped > readahead ? capped : readahead;
  uint64_t fetch_end = offset + fetch_size;
  if (fetch_end > conn->file_size) fetch_end = conn->file_size;
  fetch_size = (size_t)(fetch_end - offset);

  /* Find gaps in the extended range */
  CacheGap gaps[COALESCE_MAX_RANGES];
  guint gap_count = sparse_cache_find_gaps(&conn->cache, offset, fetch_size,
                                           gaps, COALESCE_MAX_RANGES);
  if (gap_count == 0) {
    g_mutex_unlock(&conn->cache_mutex);
    return true;
  }

  /* Coalesce nearby gaps */
  gap_count = coalesce_gaps(gaps, gap_count);

  /* Enforce minimum fetch size per gap */
  for (guint i = 0; i < gap_count; i++) {
    if (gaps[i].len < SC_MIN_FETCH_BYTES) {
      uint64_t gap_end = gaps[i].offset + SC_MIN_FETCH_BYTES;
      if (gap_end > conn->file_size) gap_end = conn->file_size;
      gaps[i].len = (size_t)(gap_end - gaps[i].offset);
    }
  }

  /* Register in-flight ranges */
  int inflight_slots[COALESCE_MAX_RANGES];
  for (guint i = 0; i < gap_count; i++) {
    inflight_slots[i] = inflight_register(conn, gaps[i].offset, gaps[i].len);
  }

  g_mutex_unlock(&conn->cache_mutex);

  /* --- Network I/O (no cache lock held) --- */

  bool ok = true;

  if (gap_count == 1) {
    /* Single gap: direct fetch */
    uint8_t *buf = g_malloc(gaps[0].len);
    size_t written = 0;
    ok = http_fetch_range(conn, gaps[0].offset, gaps[0].len,
                          buf, &written, NULL, err);
    if (ok && written > 0) {
      buf = g_realloc(buf, written);
      g_mutex_lock(&conn->cache_mutex);
      sparse_cache_insert(&conn->cache, gaps[0].offset, buf, written);
      g_mutex_unlock(&conn->cache_mutex);
      buf = NULL;  /* ownership transferred */
    }
    g_free(buf);
  } else {
    /* Multiple gaps: parallel fetch */
    uint64_t offsets[COALESCE_MAX_RANGES];
    size_t   lens[COALESCE_MAX_RANGES];
    uint8_t *buffers[COALESCE_MAX_RANGES];

    for (guint i = 0; i < gap_count; i++) {
      offsets[i] = gaps[i].offset;
      lens[i] = gaps[i].len;
      buffers[i] = g_malloc(gaps[i].len);
    }

    ok = http_fetch_parallel(conn, offsets, lens, buffers, gap_count, err);

    g_mutex_lock(&conn->cache_mutex);
    for (guint i = 0; i < gap_count; i++) {
      if (ok && buffers[i]) {
        sparse_cache_insert(&conn->cache, offsets[i], buffers[i], lens[i]);
        buffers[i] = NULL;  /* ownership transferred */
      }
      g_free(buffers[i]);
    }
    g_mutex_unlock(&conn->cache_mutex);
  }

  /* Complete in-flight ranges */
  g_mutex_lock(&conn->cache_mutex);
  for (guint i = 0; i < gap_count; i++) {
    inflight_complete(conn, inflight_slots[i]);
  }
  g_mutex_unlock(&conn->cache_mutex);

  if (!ok) return false;

  /* Verify the originally requested range is now cached.
   * It could still be missing if we only partially fetched. */
  g_mutex_lock(&conn->cache_mutex);
  gboolean covered = sparse_cache_contains(&conn->cache, offset, capped);
  g_mutex_unlock(&conn->cache_mutex);
  if (!covered) {
    /* Shouldn't normally happen, but retry once. */
    static volatile gint retry_guard = 0;
    if (g_atomic_int_add(&retry_guard, 1) == 0) {
      g_atomic_int_set(&retry_guard, 0);
      goto retry;
    }
    g_atomic_int_set(&retry_guard, 0);
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "Cache miss persists after fetch for %s at offset %" PRIu64,
                conn->uri, offset);
    return false;
  }

  return true;
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

  /* Free CURL handles */
  g_mutex_lock(&conn->curl_mutex);
  for (int i = 0; i < MAX_CURL_HANDLES; i++) {
    if (conn->curl_pool[i].curl) {
      curl_easy_cleanup(conn->curl_pool[i].curl);
      conn->curl_pool[i].curl = NULL;
    }
  }
  g_mutex_unlock(&conn->curl_mutex);

  /* Free sparse cache */
  g_mutex_lock(&conn->cache_mutex);
  sparse_cache_destroy(&conn->cache);
  g_mutex_unlock(&conn->cache_mutex);

  /* Free inflight conds */
  for (int i = 0; i < MAX_INFLIGHT_RANGES; i++) {
    g_cond_clear(&conn->inflight[i].cond);
  }

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

static HttpConnection *http_connection_create(const char *uri, GError **err G_GNUC_UNUSED) {
  HttpConnection *conn = g_new0(HttpConnection, 1);
  conn->uri = g_strdup(uri);
  conn->ref_count = 1;
  conn->last_used_us = g_get_monotonic_time();
  conn->state = CONN_STATE_INIT;

  g_mutex_init(&conn->mutex);
  g_mutex_init(&conn->cache_mutex);
  g_mutex_init(&conn->curl_mutex);
  g_cond_init(&conn->state_cond);

  sparse_cache_init(&conn->cache, SC_MAX_BYTES_DEFAULT);

  for (int i = 0; i < MAX_INFLIGHT_RANGES; i++) {
    g_cond_init(&conn->inflight[i].cond);
  }

  return conn;
}

/* ==============================
 * Connection Priming
 *
 * On first connection to a URI, fetch the front window (first N bytes)
 * and tail window (last N bytes) into the sparse cache.  The front
 * fetch also discovers the total file size via the Content-Range header.
 * Window sizes adapt to file size for large slides.
 * ============================== */

static bool http_prime_connection(HttpConnection *conn, uint64_t *out_size, GError **err) {
  /* Step 1: Fetch front window (discovers file size) */
  size_t front_want = PRIME_FRONT_BYTES;
  uint8_t *front_buf = g_malloc(front_want);
  size_t front_written = 0;
  uint64_t discovered_size = 0;

  if (!http_fetch_range(conn, 0, front_want, front_buf, &front_written,
                        &discovered_size, err) || front_written == 0) {
    g_free(front_buf);
    return false;
  }

  if (discovered_size > 0) {
    conn->file_size = discovered_size;
  } else {
    conn->file_size = front_written;
  }
  *out_size = conn->file_size;

  /* Insert front data into sparse cache (takes ownership) */
  front_buf = g_realloc(front_buf, front_written);
  g_mutex_lock(&conn->cache_mutex);
  sparse_cache_insert(&conn->cache, 0, front_buf, front_written);
  g_mutex_unlock(&conn->cache_mutex);
  /* front_buf ownership transferred — do not free */

  /* Step 2: Fetch tail window (adaptive size based on file_size) */
  if (conn->file_size > (uint64_t) front_written) {
    size_t tail_want = PRIME_TAIL_BYTES;
    size_t adaptive = (size_t)(conn->file_size / PRIME_ADAPTIVE_DIVISOR);
    if (adaptive > PRIME_ADAPTIVE_MAX) adaptive = PRIME_ADAPTIVE_MAX;
    if (adaptive > tail_want) tail_want = adaptive;

    uint64_t tail_offset = conn->file_size > tail_want
                           ? conn->file_size - tail_want : 0;
    if (tail_offset < front_written) tail_offset = front_written;

    if (tail_offset < conn->file_size) {
      tail_want = (size_t)(conn->file_size - tail_offset);
      uint8_t *tail_buf = g_malloc(tail_want);
      size_t tail_written = 0;
      GError *tail_err = NULL;

      if (http_fetch_range(conn, tail_offset, tail_want, tail_buf,
                           &tail_written, NULL, &tail_err) && tail_written > 0) {
        tail_buf = g_realloc(tail_buf, tail_written);
        g_mutex_lock(&conn->cache_mutex);
        sparse_cache_insert(&conn->cache, tail_offset, tail_buf, tail_written);
        g_mutex_unlock(&conn->cache_mutex);
        /* tail_buf ownership transferred */
      } else {
        g_free(tail_buf);
        g_clear_error(&tail_err);
      }
    }
  }

  HTTP_LOG("[HTTP-PRIME] %s size=%" PRIu64 " cache_regions=%u cached=%zuB\n",
           conn->uri, conn->file_size, conn->cache.count, conn->cache.total_bytes);
  return true;
}

/* ==============================
 * Connection Pool Lookup
 * ============================== */

/* Fast path: pool hit with READY connection, no expired scan */
static HttpConnection *http_connection_get_fast(const char *uri) {
  if (!http_initialized || !conn_pool) {
    return NULL;
  }

  g_mutex_lock(&pool_mutex);
  HttpConnection *conn = g_hash_table_lookup(conn_pool, uri);
  if (conn) {
    g_mutex_lock(&conn->mutex);
    if (conn->state == CONN_STATE_READY) {
      conn->ref_count++;
      conn->last_used_us = g_get_monotonic_time();
      g_mutex_unlock(&conn->mutex);
      g_mutex_unlock(&pool_mutex);
      return conn;
    }
    g_mutex_unlock(&conn->mutex);
  }
  g_mutex_unlock(&pool_mutex);
  return NULL;
}

static HttpConnection *http_connection_get_or_create(const char *uri, GError **err) {
  ensure_pool_mutex_init();
  _openslide_http_init();

  g_mutex_lock(&pool_mutex);

  /* Throttled expired scan */
  GList *expired = NULL;
  gint64 now_us = g_get_monotonic_time();
  if (now_us - last_expired_scan_us >= HTTP_EXPIRED_SCAN_INTERVAL_US) {
    expired = http_pool_collect_expired_locked();
    last_expired_scan_us = now_us;
  }

  HttpConnection *conn = conn_pool ? g_hash_table_lookup(conn_pool, uri) : NULL;

  if (conn) {
    g_mutex_lock(&conn->mutex);

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

  /* Create new connection */
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

  /* Network I/O outside pool_mutex */
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
  /* Fast path: reuse existing READY connection */
  HttpConnection *conn = http_connection_get_fast(uri);
  if (!conn) {
    conn = http_connection_get_or_create(uri, err);
  }
  if (!conn) {
    return NULL;
  }

  struct _openslide_http_file *file = g_new0(struct _openslide_http_file, 1);
  file->conn = conn;
  file->position = 0;
  file->last_read_end = -1;
  file->sequential_count = 0;
  file->readahead_size = AP_INITIAL_READAHEAD;

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

/* ==============================
 * Public Read API
 * ============================== */

size_t _openslide_http_read(struct _openslide_http_file *file,
                            void *buf, size_t size, GError **err) {
  if (!file || !buf || size == 0) {
    return 0;
  }

  HttpConnection *conn = file->conn;
  uint64_t pos = (uint64_t) file->position;

  if (pos >= conn->file_size) {
    return 0;
  }
  size_t to_read = MIN(size, (size_t)(conn->file_size - pos));

  /* Update adaptive prefetch state */
  if (file->last_read_end >= 0 && pos == (uint64_t) file->last_read_end) {
    file->sequential_count++;
    if (file->sequential_count >= AP_SEQ_THRESHOLD) {
      size_t new_ra = file->readahead_size * 2;
      if (new_ra > AP_MAX_READAHEAD) new_ra = AP_MAX_READAHEAD;
      file->readahead_size = new_ra;
    }
  } else {
    file->sequential_count = 0;
    file->readahead_size = AP_INITIAL_READAHEAD;
  }

  /* Ensure data is cached */
  if (!http_cache_ensure(conn, file, pos, to_read, err)) {
    return 0;
  }

  /* Copy from sparse cache */
  uint8_t *dst = (uint8_t *) buf;
  size_t total = 0;

  g_mutex_lock(&conn->cache_mutex);
  while (total < to_read) {
    const uint8_t *ptr;
    size_t avail;
    if (!sparse_cache_view(&conn->cache, pos + total, &ptr, &avail)) {
      break;
    }
    size_t copy = MIN(avail, to_read - total);
    memcpy(dst + total, ptr, copy);
    total += copy;
  }
  g_mutex_unlock(&conn->cache_mutex);

  file->position += (int64_t) total;
  file->last_read_end = file->position;
  return total;
}

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

size_t _openslide_http_pread(struct _openslide_http_file *file,
                             uint64_t offset, void *buf, size_t size,
                             GError **err) {
  if (!file || !buf || size == 0) {
    return 0;
  }

  HttpConnection *conn = file->conn;

  if (offset >= conn->file_size) {
    return 0;
  }
  size_t to_read = MIN(size, (size_t)(conn->file_size - offset));

  if (!http_cache_ensure(conn, file, offset, to_read, err)) {
    return 0;
  }

  uint8_t *dst = (uint8_t *) buf;
  size_t total = 0;

  g_mutex_lock(&conn->cache_mutex);
  while (total < to_read) {
    const uint8_t *ptr;
    size_t avail;
    if (!sparse_cache_view(&conn->cache, offset + total, &ptr, &avail)) {
      break;
    }
    size_t copy = MIN(avail, to_read - total);
    memcpy(dst + total, ptr, copy);
    total += copy;
  }
  g_mutex_unlock(&conn->cache_mutex);

  return total;
}

bool _openslide_http_pread_ptr(struct _openslide_http_file *file,
                               uint64_t offset,
                               const uint8_t **out_ptr,
                               size_t *out_avail,
                               GError **err) {
  if (!file || !out_ptr || !out_avail) {
    return false;
  }

  HttpConnection *conn = file->conn;

  if (offset >= conn->file_size) {
    *out_ptr = NULL;
    *out_avail = 0;
    return true;
  }

  if (!http_cache_ensure(conn, file, offset, 1, err)) {
    return false;
  }

  g_mutex_lock(&conn->cache_mutex);
  if (!sparse_cache_view(&conn->cache, offset, out_ptr, out_avail)) {
    g_mutex_unlock(&conn->cache_mutex);
    *out_ptr = NULL;
    *out_avail = 0;
    return true;
  }
  g_mutex_unlock(&conn->cache_mutex);

  return true;
}

/* ==============================
 * Statistics API
 * ============================== */

void _openslide_http_print_stats(void) {
#if HTTP_STATS
  gint reqs = g_atomic_int_get(&global_stats.total_requests);
  gint bytes_kb = g_atomic_int_get(&global_stats.total_bytes_kb);
  gint time_ms = g_atomic_int_get(&global_stats.total_time_ms);
  gint hits = g_atomic_int_get(&global_stats.cache_hits);
  gint misses = g_atomic_int_get(&global_stats.cache_misses);
  gint reused = g_atomic_int_get(&global_stats.connection_reuses);
  gint newconn = g_atomic_int_get(&global_stats.new_connections);

  fprintf(stderr, "[HTTP-GLOBAL-STATS] requests=%d bytes=%dKB time=%dms avg=%.2fms/req\n",
          reqs, bytes_kb, time_ms,
          reqs > 0 ? (double)time_ms / reqs : 0.0);
  fprintf(stderr, "[HTTP-GLOBAL-STATS] cache_hits=%d cache_misses=%d hit_rate=%.1f%%\n",
          hits, misses,
          (hits + misses) > 0 ? 100.0 * hits / (hits + misses) : 0.0);
  fprintf(stderr, "[HTTP-GLOBAL-STATS] conn_reused=%d conn_new=%d reuse_rate=%.1f%%\n",
          reused, newconn,
          (reused + newconn) > 0 ? 100.0 * reused / (reused + newconn) : 0.0);
#else
  fprintf(stderr, "[HTTP-GLOBAL-STATS] Statistics disabled (HTTP_STATS=0)\n");
#endif
}
