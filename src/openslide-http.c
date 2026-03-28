#include "openslide-http.h"
#include "openslide/error.h"
#include <glib.h>
#include <curl/curl.h>
#include <inttypes.h>
#include <string.h>

// ==============================
// 全局动态配置（线程安全）
// ==============================
static GMutex           config_mutex;
static OpenslideHTTPConfig http_config = {
    .block_size             = 256 * 1024,
    .max_cache_blocks       = 256,
    .retry_max              = 3,
    .retry_delay_ms         = 200,
    .connect_timeout_ms     = 5000,
    .transfer_timeout_ms    = 15000,
    .low_speed_limit        = 10240,
    .low_speed_time         = 5,
    .pool_ttl_sec           = 300,
};

void openslide_http_set_config(const OpenslideHTTPConfig *cfg) {
    g_mutex_lock(&config_mutex);
    if (cfg) {
        http_config = *cfg;
    }
    g_mutex_unlock(&config_mutex);
}

const OpenslideHTTPConfig *openslide_http_get_config(void) {
    return &http_config;
}

// ==============================
// 内部类型定义
// ==============================
typedef struct {
    uint8_t    *data;
    size_t      len;
} HttpBlock;

typedef struct _HttpFile HttpFile;

struct _HttpFile {
    CURL       *curl;
    gchar      *uri;
    uint64_t    file_size;

    GHashTable *block_map;
    GList      *lru_list;
    guint       cache_count;

    gint        ref;
    gint64      last_access;
    GMutex      mutex;
};

typedef struct {
    uint8_t    *dst;
    size_t      remain;
    size_t      written;
} WriteCtx;

// ==============================
// 全局连接池 & 文件池
// ==============================
static GMutex     pool_mutex;
static GHashTable *file_pool = NULL;
static CURLSH    *curl_share = NULL;

// ==============================
// CURL 共享连接池
// ==============================
static void curl_share_init_once(void) {
    g_mutex_lock(&pool_mutex);
    if (curl_share == NULL) {
        curl_share = curl_share_init();
        curl_share_setopt(curl_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
        curl_share_setopt(curl_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_SSL_SESSION);
        curl_share_setopt(curl_share, CURLSHOPT_SHARE, CURL_LOCK_DATA_CONNECT);
    }
    g_mutex_unlock(&pool_mutex);
}

static void curl_lock_cb(CURL *c, curl_lock_data l, curl_lock_access a, void *u) {
    static GMutex m[CURL_LOCK_DATA_LAST];
    g_mutex_lock(&m[l]);
}

static void curl_unlock_cb(CURL *c, curl_lock_data l, void *u) {
    static GMutex m[CURL_LOCK_DATA_LAST];
    g_mutex_unlock(&m[l]);
}

static void http_curl_setup_common(CURL *curl, const char *uri) {
    const OpenslideHTTPConfig *cfg = openslide_http_get_config();

    curl_easy_setopt(curl, CURLOPT_URL, uri);
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 2L);
    curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_DNS_CACHE_TIMEOUT, 60L);

    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, (long)cfg->connect_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, (long)cfg->transfer_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, (long)cfg->low_speed_limit);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, (long)cfg->low_speed_time);
    curl_easy_setopt(curl, CURLOPT_SHARE, curl_share);
}

// ==============================
// 写回调防越界
// ==============================
static size_t http_write_callback(void *data, size_t sz, size_t nmemb, void *user) {
    WriteCtx *ctx = (WriteCtx *)user;
    size_t will_write = sz * nmemb;

    if (will_write > ctx->remain) will_write = ctx->remain;
    if (will_write == 0) return 0;

    memcpy(ctx->dst, data, will_write);
    ctx->dst += will_write;
    ctx->written += will_write;
    ctx->remain -= will_write;

    return will_write;
}

// ==============================
// Range 获取
// ==============================
static bool http_fetch_range(HttpFile *f,
                             uint64_t offset, size_t len,
                             uint8_t *dst, size_t *out_written,
                             GError **err) {
    const OpenslideHTTPConfig *cfg = openslide_http_get_config();
    *out_written = 0;

    WriteCtx ctx = { .dst = dst, .remain = len, .written = 0 };

    for (int retry = 0; retry <= cfg->retry_max; retry++) {
        g_autofree gchar *range = g_strdup_printf("bytes=%" PRIu64 "-%" PRIu64,
                                                  offset + ctx.written, offset + len - 1);
        curl_easy_reset(f->curl);
        http_curl_setup_common(f->curl, f->uri);
        curl_easy_setopt(f->curl, CURLOPT_RANGE, range);
        curl_easy_setopt(f->curl, CURLOPT_WRITEFUNCTION, http_write_callback);
        curl_easy_setopt(f->curl, CURLOPT_WRITEDATA, &ctx);

        CURLcode code = curl_easy_perform(f->curl);
        if (code == CURLE_OK) {
            *out_written = ctx.written;
            return TRUE;
        }

        gboolean retryable = (
            code == CURLE_COULDNT_CONNECT
            || code == CURLE_OPERATION_TIMEDOUT
            || code == CURLE_RECV_ERROR
            || code == CURLE_SEND_ERROR
            || code == CURLE_GOT_NOTHING
            || code == CURLE_PARTIAL_FILE
        );
        if (!retryable) break;

        if (retry < cfg->retry_max) {
            g_usleep(cfg->retry_delay_ms * 1000 * (1 << retry));
        }
    }

    if (ctx.written > 0) {
        *out_written = ctx.written;
        return TRUE;
    }

    g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_IO, "HTTP range failed");
    return FALSE;
}

// ==============================
// LRU 缓存
// ==============================
static void lru_promote(HttpFile *f, guint64 idx) {
    f->lru_list = g_list_remove(f->lru_list, GUINT_TO_POINTER(idx));
    f->lru_list = g_list_prepend(f->lru_list, GUINT_TO_POINTER(idx));
}

static void lru_evict(HttpFile *f) {
    if (f->cache_count == 0) return;

    guint64 last_idx = GPOINTER_TO_UINT(g_list_last(f->lru_list)->data);
    f->lru_list = g_list_remove_last(f->lru_list);

    HttpBlock *b = g_hash_table_lookup(f->block_map, GUINT_TO_POINTER(last_idx));
    if (b) {
        g_free(b->data);
        g_free(b);
    }
    g_hash_table_remove(f->block_map, GUINT_TO_POINTER(last_idx));
    f->cache_count--;
}

// ==============================
// 获取块
// ==============================
static HttpBlock *http_get_block(HttpFile *f, guint64 block_idx, GError **err) {
    const OpenslideHTTPConfig *cfg = openslide_http_get_config();
    g_mutex_lock(&f->mutex);

    HttpBlock *b = g_hash_table_lookup(f->block_map, GUINT_TO_POINTER(block_idx));
    if (b) {
        lru_promote(f, block_idx);
        g_mutex_unlock(&f->mutex);
        return b;
    }

    while (f->cache_count >= cfg->max_cache_blocks) {
        lru_evict(f);
    }

    uint64_t offset = block_idx * cfg->block_size;
    size_t len = cfg->block_size;

    if (offset > f->file_size) {
        g_mutex_unlock(&f->mutex);
        g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_IO, "block out of bounds");
        return NULL;
    }
    if (offset + len > f->file_size) {
        len = f->file_size - offset;
    }

    b = g_new(HttpBlock, 1);
    b->len = len;
    b->data = g_malloc(len);

    size_t written;
    if (!http_fetch_range(f, offset, len, b->data, &written, err) || written == 0) {
        g_free(b->data);
        g_free(b);
        g_mutex_unlock(&f->mutex);
        return NULL;
    }
    if (written < len) {
        b->len = written;
    }

    g_hash_table_insert(f->block_map, GUINT_TO_POINTER(block_idx), b);
    f->lru_list = g_list_prepend(f->lru_list, GUINT_TO_POINTER(block_idx));
    f->cache_count++;

    g_mutex_unlock(&f->mutex);
    return b;
}

// ==============================
// 安全 read
// ==============================
static bool http_read(osrbfile *bf,
                      uint64_t offset, size_t len, void *dest, GError **err) {
    HttpFile *f = (HttpFile *)bf;
    uint8_t *dst = (uint8_t *)dest;

    if (offset >= f->file_size) {
        memset(dst, 0, len);
        g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_IO, "read beyond end");
        return FALSE;
    }

    uint64_t avail = f->file_size - offset;
    size_t to_read = MIN(len, (size_t)avail);
    if (to_read < len) {
        memset(dst + to_read, 0, len - to_read);
    }

    const OpenslideHTTPConfig *cfg = openslide_http_get_config();
    uint64_t cur = offset;
    size_t rem = to_read;

    while (rem > 0) {
        guint64 idx = cur / cfg->block_size;
        uint32_t off = cur % cfg->block_size;

        HttpBlock *b = http_get_block(f, idx, err);
        if (!b) return FALSE;

        size_t b_avail = b->len - off;
        if (b_avail == 0) break;

        size_t cp = MIN(rem, b_avail);
        memcpy(dst, b->data + off, cp);

        dst += cp;
        cur += cp;
        rem -= cp;
    }

    g_mutex_lock(&f->mutex);
    f->last_access = g_get_monotonic_time() / 1000000;
    g_mutex_unlock(&f->mutex);

    return TRUE;
}

static uint64_t http_get_size(osrbfile *bf, GError **err) {
    return ((HttpFile *)bf)->file_size;
}

// ==============================
// 关闭 & 引用计数
// ==============================
static void http_close(osrbfile *bf) {
    HttpFile *f = (HttpFile *)bf;

    g_mutex_lock(&pool_mutex);
    gboolean last = g_atomic_int_dec_and_test(&f->ref);
    if (!last) {
        g_mutex_unlock(&pool_mutex);
        return;
    }

    g_hash_table_remove(file_pool, f->uri);
    g_mutex_unlock(&pool_mutex);

    GList *l = f->lru_list;
    while (l) {
        guint64 idx = GPOINTER_TO_UINT(l->data);
        HttpBlock *b = g_hash_table_lookup(f->block_map, GUINT_TO_POINTER(idx));
        if (b) {
            g_free(b->data);
            g_free(b);
        }
        l = l->next;
    }
    g_list_free(f->lru_list);
    g_hash_table_destroy(f->block_map);

    g_free(f->uri);
    curl_easy_cleanup(f->curl);
    g_mutex_clear(&f->mutex);
    g_free(f);
}

static const osrbfile_class http_class = {
    .read     = http_read,
    .get_size = http_get_size,
    .close    = http_close,
};

// ==============================
// 池化打开
// ==============================
osrbfile *osbackend_http_open(const char *uri, GError **err) {
    const OpenslideHTTPConfig *cfg = openslide_http_get_config();
    curl_share_init_once();

    g_mutex_lock(&pool_mutex);

    if (!file_pool) {
        file_pool = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, NULL);
        curl_global_init(CURL_GLOBAL_ALL);
        curl_share_setopt(curl_share, CURLSHOPT_LOCKFUNC, curl_lock_cb);
        curl_share_setopt(curl_share, CURLSHOPT_UNLOCKFUNC, curl_unlock_cb);
    }

    // 过期清理
    gint64 now = g_get_monotonic_time() / 1000000;
    GList *keys = g_hash_table_get_keys(file_pool);
    for (GList *l = keys; l; l = l->next) {
        const gchar *u = l->data;
        HttpFile *f = g_hash_table_lookup(file_pool, u);
        if (now - f->last_access > cfg->pool_ttl_sec) {
            g_hash_table_remove(file_pool, u);
        }
    }
    g_list_free(keys);

    // 复用
    HttpFile *f = g_hash_table_lookup(file_pool, uri);
    if (f) {
        g_atomic_int_inc(&f->ref);
        f->last_access = now;
        g_mutex_unlock(&pool_mutex);
        return (osrbfile *)f;
    }

    // 新建 curl
    CURL *curl = curl_easy_init();
    if (!curl) {
        g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_IO, "curl init failed");
        g_mutex_unlock(&pool_mutex);
        return NULL;
    }

    curl_easy_setopt(curl, CURLOPT_URL, uri);
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT_MS, (long)cfg->connect_timeout_ms);
    curl_easy_setopt(curl, CURLOPT_SHARE, curl_share);

    CURLcode code = curl_easy_perform(curl);
    if (code != CURLE_OK) {
        g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_IO, "HEAD failed");
        curl_easy_cleanup(curl);
        g_mutex_unlock(&pool_mutex);
        return NULL;
    }

    double sz = 0;
    curl_easy_getinfo(curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD, &sz);
    uint64_t file_size = (uint64_t)sz;

    if (file_size == 0) {
        g_set_error(err, OPENSLIDE_ERROR, OPENSLIDE_ERROR_IO, "empty file size");
        curl_easy_cleanup(curl);
        g_mutex_unlock(&pool_mutex);
        return NULL;
    }

    // 新建 HttpFile
    f = g_new0(HttpFile, 1);
    f->base.class = &http_class;
    f->uri = g_strdup(uri);
    f->curl = curl;
    f->file_size = file_size;
    f->block_map = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL, NULL);
    f->lru_list = NULL;
    f->cache_count = 0;
    f->ref = 1;
    f->last_access = now;
    g_mutex_init(&f->mutex);

    g_hash_table_insert(file_pool, g_strdup(uri), f);
    g_mutex_unlock(&pool_mutex);

    return (osrbfile *)f;
}
