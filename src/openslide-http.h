#pragma once

#include <stdint.h>
#include <stdbool.h>
#include "openslide/openslide.h"

G_BEGIN_DECLS

// HTTP 后端配置结构体
typedef struct {
    size_t          block_size;             // 块大小（默认 256KB）
    guint           max_cache_blocks;       // 单文件 LRU 块上限
    int             retry_max;              // 最大重试次数
    int             retry_delay_ms;         // 初始退避毫秒
    int             connect_timeout_ms;     // 连接超时
    int             transfer_timeout_ms;    // 传输超时
    int             low_speed_limit;        // 低速阈值 bytes/s
    int             low_speed_time;         // 低速判定秒数
    int             pool_ttl_sec;           // HttpFile 池过期秒数
} OpenslideHTTPConfig;

// 全局配置设置（程序启动时调用一次）
void openslide_http_set_config(const OpenslideHTTPConfig *cfg);

// 获取当前生效配置
const OpenslideHTTPConfig *openslide_http_get_config(void);

// 打开 HTTP/HTTPS 后端
osrbfile *osbackend_http_open(const char *uri, GError **err);

G_END_DECLS
