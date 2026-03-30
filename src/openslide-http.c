/*
 * openslide-http.c - HTTP Range-based file access (fsspec-style design)
 *
 * Core principles from fsspec:
 * 1. seek() = pure memory operation (zero network I/O)
 * 2. Single CURL session per connection, reused for ALL requests
 * 3. Block-aligned cache (4MB blocks, cache hit = microsecond access)
 * 4. Connection pool with instance sharing
 */

#include <config.h>

#include "openslide-http.h"
#include "openslide-private.h"

#include <curl/curl.h>
#include <glib.h>
#include <inttypes.h>
#include <string.h>

/* ============================================================================
 * Configuration
 * ============================================================================ */

#define BLOCK_SIZE          (4 * 1024 * 1024)   /* 4MB - same as fsspec default */
#define MAX_CACHE_BLOCKS    64                   /* 256MB max cache per connection */
#define CONNECT_TIMEOUT_MS  10000
#define TRANSFER_TIMEOUT_MS 60000
#define MAX_RETRIES         3

/* Debug control */
#define HTTP_DEBUG 0

#if HTTP_DEBUG
  #define HTTP_LOG(...) fprintf(stderr, __VA_ARGS__)
#else
  #define HTTP_LOG(...) ((void)0)
#endif

/* ============================================================================
 * Data Structures (minimal, like fsspec)
 * ============================================================================ */

/* Cached block */
typedef struct {
  uint8_t *data;
  size_t len;
  guint64 block_idx;
} CacheBlock;

/* Shared connection - one per URI, shared by all file handles */
typedef struct _HttpConnection {
  char *uri;
  uint64_t file_size;
  
  /* Single CURL session - reused for ALL requests (key to performance!) */
  CURL *curl;
  GMutex curl_mutex;
  
  /* Block cache with LRU eviction */
  GHashTable *cache;        /* block_idx -> CacheBlock* */
  GQueue *lru;              /* LRU order (oldest at tail) */
  GMutex cache_mutex;
  
  /* Reference counting */
  gint ref_count;
} HttpConnection;

/* File handle - lightweight, just position state */
struct _openslide_http_file {
  HttpConnection *conn;
  int64_t pos;              /* Current position - seek() only modifies this! */
};

/* ============================================================================
 * Global State
 * ============================================================================ */

static GHashTable *g_conn_pool = NULL;  /* uri -> HttpConnection* */
static GMutex g_pool_mutex;
static gboolean g_initialized = FALSE;

static void ensure_init(void) {
  static gsize once = 0;
  if (g_once_init_enter(&once)) {
    g_mutex_init(&g_pool_mutex);
    g_conn_pool = g_hash_table_new(g_str_hash, g_str_equal);
    curl_global_init(CURL_GLOBAL_DEFAULT);
    g_initialized = TRUE;
    g_once_init_leave(&once, 1);
  }
}

/* ============================================================================
 * CURL Helpers
 * ============================================================================ */

typedef struct {
  uint8_t *dst;
  size_t capacity;
  size_t written;
} WriteCtx;

static size_t write_cb(void *data, size_t sz, size_t nmemb, void *userp) {
  WriteCtx *ctx = userp;
  size_t total = sz * nmemb;
  size_t to_copy = MIN(total, ctx->capacity - ctx->written);
  if (to_copy > 0) {
    memcpy(ctx->dst + ctx->written, data, to_copy);
    ctx->written += to_copy;
  }
  return total;
}

/* Setup CURL for persistent connection - called ONCE */
static void curl_setup(CURL *curl, const char *uri) {
  curl_easy_setopt(curl, CURLOPT_URL, uri);
  curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 5L);
  curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, (long)CONNECT_TIMEOUT_MS);
  curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)TRANSFER_TIMEOUT_MS);
  
  /* CRITICAL: Connection persistence */
  curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
  curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 60L);
  curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 30L);
  curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 0L);
  curl_easy_setopt(curl, CURLOPT_FRESH_CONNECT, 0L);
  
  /* HTTP/2 */
#ifdef CURL_HTTP_VERSION_2_0
  curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2_0);
#endif

  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
  curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
  curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
  curl_easy_setopt(curl, CURLOPT_USERAGENT, "OpenSlide/4.0");
}

/* Fetch byte range - reuses connection */
static bool fetch_range(HttpConnection *conn, uint64_t offset, size_t len,
                        uint8_t *buf, size_t *out_written, GError **err) {
  char range_hdr[128];
  snprintf(range_hdr, sizeof(range_hdr), 
           "Range: bytes=%" PRIu64 "-%" PRIu64, offset, offset + len - 1);
  
  struct curl_slist *hdrs = curl_slist_append(NULL, range_hdr);
  WriteCtx ctx = { .dst = buf, .capacity = len, .written = 0 };
  
  g_mutex_lock(&conn->curl_mutex);
  
  /* Set per-request options - DO NOT call curl_easy_reset()! */
  curl_easy_setopt(conn->curl, CURLOPT_HTTPHEADER, hdrs);
  curl_easy_setopt(conn->curl, CURLOPT_WRITEFUNCTION, write_cb);
  curl_easy_setopt(conn->curl, CURLOPT_WRITEDATA, &ctx);
  curl_easy_setopt(conn->curl, CURLOPT_NOBODY, 0L);
  curl_easy_setopt(conn->curl, CURLOPT_HTTPGET, 1L);
  
  CURLcode code = CURLE_OK;
  long http_code = 0;
  
  for (int retry = 0; retry <= MAX_RETRIES; retry++) {
    ctx.written = 0;
    code = curl_easy_perform(conn->curl);
    curl_easy_getinfo(conn->curl, CURLINFO_RESPONSE_CODE, &http_code);
    
    if (code == CURLE_OK && (http_code == 200 || http_code == 206)) {
      break;
    }
    
    if (retry < MAX_RETRIES) {
      HTTP_LOG("[HTTP] Retry %d: curl=%d http=%ld\n", retry + 1, code, http_code);
      g_usleep(100000 * (1 << retry));
    }
  }
  
  g_mutex_unlock(&conn->curl_mutex);
  curl_slist_free_all(hdrs);
  
  if (code != CURLE_OK || (http_code != 200 && http_code != 206)) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "HTTP failed: curl=%d http=%ld", code, http_code);
    return false;
  }
  
  *out_written = ctx.written;
  HTTP_LOG("[HTTP] Fetched %" PRIu64 "+%zu -> %zu bytes\n", offset, len, ctx.written);
  return true;
}

/* Get file size via HEAD */
static bool get_size(HttpConnection *conn, uint64_t *out_size, GError **err) {
  g_mutex_lock(&conn->curl_mutex);
  
  curl_easy_setopt(conn->curl, CURLOPT_NOBODY, 1L);
  curl_easy_setopt(conn->curl, CURLOPT_HTTPHEADER, NULL);
  curl_easy_setopt(conn->curl, CURLOPT_WRITEFUNCTION, NULL);
  
  CURLcode code = curl_easy_perform(conn->curl);
  long http_code = 0;
  double cl = 0;
  curl_easy_getinfo(conn->curl, CURLINFO_RESPONSE_CODE, &http_code);
  curl_easy_getinfo(conn->curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &cl);
  
  g_mutex_unlock(&conn->curl_mutex);
  
  if (code != CURLE_OK || http_code != 200 || cl <= 0) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                "HEAD failed: curl=%d http=%ld cl=%.0f", code, http_code, cl);
    return false;
  }
  
  *out_size = (uint64_t)cl;
  HTTP_LOG("[HTTP] Size: %" PRIu64 "\n", *out_size);
  return true;
}

/* ============================================================================
 * Block Cache
 * ============================================================================ */

static void block_free(CacheBlock *b) {
  if (b) {
    g_free(b->data);
    g_free(b);
  }
}

static void cache_evict_if_full(HttpConnection *conn) {
  while (g_queue_get_length(conn->lru) >= MAX_CACHE_BLOCKS) {
    guint64 *oldest = g_queue_pop_tail(conn->lru);
    if (oldest) {
      CacheBlock *b = g_hash_table_lookup(conn->cache, oldest);
      g_hash_table_remove(conn->cache, oldest);
      block_free(b);
      g_free(oldest);
    }
  }
}

static guint u64_hash(gconstpointer v) { return (guint)(*(guint64*)v); }
static gboolean u64_eq(gconstpointer a, gconstpointer b) { 
  return *(guint64*)a == *(guint64*)b; 
}

/* Get block - from cache or fetch */
static CacheBlock *get_block(HttpConnection *conn, guint64 idx, GError **err) {
  g_mutex_lock(&conn->cache_mutex);
  
  CacheBlock *b = g_hash_table_lookup(conn->cache, &idx);
  if (b) {
    /* Cache hit - move to front of LRU */
    GList *link = g_queue_find(conn->lru, &b->block_idx);
    if (link) {
      g_queue_unlink(conn->lru, link);
      g_queue_push_head_link(conn->lru, link);
    }
    g_mutex_unlock(&conn->cache_mutex);
    HTTP_LOG("[CACHE] Hit block %" PRIu64 "\n", idx);
    return b;
  }
  g_mutex_unlock(&conn->cache_mutex);
  
  /* Cache miss - fetch */
  HTTP_LOG("[CACHE] Miss block %" PRIu64 "\n", idx);
  
  uint64_t offset = idx * BLOCK_SIZE;
  if (offset >= conn->file_size) return NULL;
  
  size_t len = BLOCK_SIZE;
  if (offset + len > conn->file_size) {
    len = (size_t)(conn->file_size - offset);
  }
  
  uint8_t *data = g_malloc(len);
  size_t written = 0;
  
  if (!fetch_range(conn, offset, len, data, &written, err) || written == 0) {
    g_free(data);
    return NULL;
  }
  
  b = g_new0(CacheBlock, 1);
  b->data = data;
  b->len = written;
  b->block_idx = idx;
  
  g_mutex_lock(&conn->cache_mutex);
  
  /* Double-check another thread didn't add it */
  CacheBlock *existing = g_hash_table_lookup(conn->cache, &idx);
  if (existing) {
    g_mutex_unlock(&conn->cache_mutex);
    block_free(b);
    return existing;
  }
  
  cache_evict_if_full(conn);
  
  guint64 *key = g_new(guint64, 1);
  *key = idx;
  g_hash_table_insert(conn->cache, key, b);
  g_queue_push_head(conn->lru, key);
  
  g_mutex_unlock(&conn->cache_mutex);
  return b;
}

/* ============================================================================
 * Connection Pool
 * ============================================================================ */

static HttpConnection *conn_create(const char *uri, GError **err) {
  HttpConnection *c = g_new0(HttpConnection, 1);
  c->uri = g_strdup(uri);
  c->ref_count = 1;
  
  g_mutex_init(&c->curl_mutex);
  g_mutex_init(&c->cache_mutex);
  
  c->curl = curl_easy_init();
  if (!c->curl) {
    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED, "CURL init failed");
    g_free(c->uri);
    g_free(c);
    return NULL;
  }
  
  curl_setup(c->curl, uri);
  
  if (!get_size(c, &c->file_size, err)) {
    curl_easy_cleanup(c->curl);
    g_free(c->uri);
    g_free(c);
    return NULL;
  }
  
  c->cache = g_hash_table_new(u64_hash, u64_eq);
  c->lru = g_queue_new();
  
  HTTP_LOG("[HTTP] Created connection: %s (size=%" PRIu64 ")\n", uri, c->file_size);
  return c;
}

static void conn_destroy(HttpConnection *c) {
  if (!c) return;
  
  HTTP_LOG("[HTTP] Destroying: %s\n", c->uri);
  
  g_mutex_lock(&c->cache_mutex);
  GHashTableIter iter;
  gpointer k, v;
  g_hash_table_iter_init(&iter, c->cache);
  while (g_hash_table_iter_next(&iter, &k, &v)) {
    block_free(v);
    g_free(k);
  }
  g_hash_table_destroy(c->cache);
  g_queue_free(c->lru);
  g_mutex_unlock(&c->cache_mutex);
  g_mutex_clear(&c->cache_mutex);
  
  g_mutex_lock(&c->curl_mutex);
  if (c->curl) curl_easy_cleanup(c->curl);
  g_mutex_unlock(&c->curl_mutex);
  g_mutex_clear(&c->curl_mutex);
  
  g_free(c->uri);
  g_free(c);
}

static HttpConnection *conn_get(const char *uri, GError **err) {
  ensure_init();
  
  g_mutex_lock(&g_pool_mutex);
  HttpConnection *c = g_hash_table_lookup(g_conn_pool, uri);
  if (c) {
    g_atomic_int_inc(&c->ref_count);
    g_mutex_unlock(&g_pool_mutex);
    HTTP_LOG("[HTTP] Reuse connection: %s (ref=%d)\n", uri, c->ref_count);
    return c;
  }
  g_mutex_unlock(&g_pool_mutex);
  
  c = conn_create(uri, err);
  if (!c) return NULL;
  
  g_mutex_lock(&g_pool_mutex);
  HttpConnection *existing = g_hash_table_lookup(g_conn_pool, uri);
  if (existing) {
    g_atomic_int_inc(&existing->ref_count);
    g_mutex_unlock(&g_pool_mutex);
    conn_destroy(c);
    return existing;
  }
  g_hash_table_insert(g_conn_pool, c->uri, c);
  g_mutex_unlock(&g_pool_mutex);
  
  return c;
}

static void conn_unref(HttpConnection *c) {
  if (!c) return;
  if (g_atomic_int_dec_and_test(&c->ref_count)) {
    g_mutex_lock(&g_pool_mutex);
    g_hash_table_remove(g_conn_pool, c->uri);
    g_mutex_unlock(&g_pool_mutex);
    conn_destroy(c);
  }
}

/* ============================================================================
 * Public API
 * ============================================================================ */

bool _openslide_is_remote_path(const char *path) {
  return path && (g_str_has_prefix(path, "http://") || 
                  g_str_has_prefix(path, "https://"));
}

void _openslide_http_init(void) {
  ensure_init();
}

void _openslide_http_cleanup(void) {
  if (!g_initialized) return;
  
  g_mutex_lock(&g_pool_mutex);
  if (g_conn_pool) {
    GHashTableIter iter;
    gpointer k, v;
    GList *to_destroy = NULL;
    g_hash_table_iter_init(&iter, g_conn_pool);
    while (g_hash_table_iter_next(&iter, &k, &v)) {
      to_destroy = g_list_prepend(to_destroy, v);
    }
    g_hash_table_destroy(g_conn_pool);
    g_conn_pool = NULL;
    g_mutex_unlock(&g_pool_mutex);
    
    for (GList *l = to_destroy; l; l = l->next) {
      conn_destroy(l->data);
    }
    g_list_free(to_destroy);
  } else {
    g_mutex_unlock(&g_pool_mutex);
  }
  
  curl_global_cleanup();
}

struct _openslide_http_file *_openslide_http_open(const char *uri, GError **err) {
  HttpConnection *c = conn_get(uri, err);
  if (!c) return NULL;
  
  struct _openslide_http_file *f = g_new0(struct _openslide_http_file, 1);
  f->conn = c;
  f->pos = 0;
  return f;
}

void _openslide_http_close(struct _openslide_http_file *f) {
  if (!f) return;
  conn_unref(f->conn);
  g_free(f);
}

int64_t _openslide_http_size(struct _openslide_http_file *f) {
  return f ? (int64_t)f->conn->file_size : -1;
}

/* seek() - PURE MEMORY OPERATION (like fsspec!) */
int64_t _openslide_http_seek(struct _openslide_http_file *f,
                             int64_t offset, int whence) {
  if (!f) return -1;
  
  int64_t new_pos;
  int64_t size = (int64_t)f->conn->file_size;
  
  switch (whence) {
    case SEEK_SET: new_pos = offset; break;
    case SEEK_CUR: new_pos = f->pos + offset; break;
    case SEEK_END: new_pos = size + offset; break;
    default: return -1;
  }
  
  if (new_pos < 0) new_pos = 0;
  if (new_pos > size) new_pos = size;
  
  f->pos = new_pos;
  return new_pos;
}

int64_t _openslide_http_tell(struct _openslide_http_file *f) {
  return f ? f->pos : -1;
}

/* read() - Check cache, fetch block if miss, return data */
size_t _openslide_http_read(struct _openslide_http_file *f,
                            void *buf, size_t size, GError **err) {
  if (!f || !buf || size == 0) return 0;
  
  HttpConnection *c = f->conn;
  uint8_t *dst = buf;
  int64_t pos = f->pos;
  
  if (pos >= (int64_t)c->file_size) return 0;
  
  size_t avail = (size_t)(c->file_size - (uint64_t)pos);
  size_t to_read = MIN(size, avail);
  size_t total = 0;
  
  while (total < to_read) {
    guint64 idx = (uint64_t)(pos + (int64_t)total) / BLOCK_SIZE;
    size_t off_in_block = (uint64_t)(pos + (int64_t)total) % BLOCK_SIZE;
    
    CacheBlock *b = get_block(c, idx, err);
    if (!b || !b->data) break;
    if (off_in_block >= b->len) break;
    
    size_t copy = MIN(b->len - off_in_block, to_read - total);
    memcpy(dst + total, b->data + off_in_block, copy);
    total += copy;
  }
  
  f->pos += (int64_t)total;
  return total;
}

size_t _openslide_http_pread(struct _openslide_http_file *f,
                             void *buf, uint64_t offset, size_t size,
                             GError **err) {
  if (!f) return 0;
  int64_t old = f->pos;
  f->pos = (int64_t)offset;
  size_t n = _openslide_http_read(f, buf, size, err);
  f->pos = old;
  return n;
}

bool _openslide_http_read_exact(struct _openslide_http_file *f,
                                void *buf, size_t size, GError **err) {
  size_t n = _openslide_http_read(f, buf, size, err);
  if (n < size) {
    if (err && !*err) {
      g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_FAILED,
                  "Short read: wanted %zu, got %zu", size, n);
    }
    return false;
  }
  return true;
}

void _openslide_http_print_stats(void) {
  if (!g_initialized) return;
  
  g_mutex_lock(&g_pool_mutex);
  fprintf(stderr, "[HTTP] Pool: %u connections\n",
          g_hash_table_size(g_conn_pool));
  
  GHashTableIter iter;
  gpointer k, v;
  g_hash_table_iter_init(&iter, g_conn_pool);
  while (g_hash_table_iter_next(&iter, &k, &v)) {
    HttpConnection *c = v;
    fprintf(stderr, "[HTTP] %s: ref=%d cache=%u\n",
            c->uri, c->ref_count, g_hash_table_size(c->cache));
  }
  g_mutex_unlock(&g_pool_mutex);
}

/* Compatibility stubs */
void _openslide_http_set_config(const OpenslideHTTPConfig *cfg G_GNUC_UNUSED) {}
const OpenslideHTTPConfig *_openslide_http_get_config(void) { return NULL; }
