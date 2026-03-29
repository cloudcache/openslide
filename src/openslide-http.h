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

#ifndef OPENSLIDE_OPENSLIDE_HTTP_H_
#define OPENSLIDE_OPENSLIDE_HTTP_H_

#include <glib.h>
#include <stdbool.h>
#include <stdint.h>

/* Forward declaration - actual struct defined in openslide-http.c */
struct _openslide_http_file;

/* Configuration structure for HTTP backend */
typedef struct {
  uint32_t block_size;        /* Block size for caching (default: 256KB) */
  uint32_t max_cache_blocks;  /* Max blocks per file (default: 256) */
  uint32_t retry_max;         /* Max retry count (default: 3) */
  uint32_t retry_delay_ms;    /* Initial retry delay in ms (default: 200) */
  uint32_t connect_timeout_ms;/* Connection timeout (default: 5000) */
  uint32_t transfer_timeout_ms;/* Transfer timeout (default: 15000) */
  uint32_t low_speed_limit;   /* Low speed threshold bytes/s (default: 10240) */
  uint32_t low_speed_time;    /* Low speed time in seconds (default: 5) */
  uint32_t pool_ttl_sec;      /* Pool entry TTL in seconds (default: 300) */
} OpenslideHTTPConfig;

/*
 * URL Detection
 * Returns true if path starts with http://, https://, or s3://
 */
bool _openslide_is_remote_path(const char *path);

/*
 * HTTP Subsystem Initialization and Cleanup
 * Called automatically on first use, but can be called explicitly
 */
void _openslide_http_init(void);
void _openslide_http_cleanup(void);

/*
 * Configuration
 */
void _openslide_http_set_config(const OpenslideHTTPConfig *cfg);
const OpenslideHTTPConfig *_openslide_http_get_config(void);

/*
 * File Operations
 */

/* Open a remote file handle */
struct _openslide_http_file *_openslide_http_open(const char *uri,
                                                   GError **err);

/* Read from current position, returns bytes read */
size_t _openslide_http_read(struct _openslide_http_file *file,
                            void *buf, size_t size, GError **err);

/* Read exact number of bytes, returns false on short read */
bool _openslide_http_read_exact(struct _openslide_http_file *file,
                                void *buf, size_t size, GError **err);

/* Seek to position */
bool _openslide_http_seek(struct _openslide_http_file *file,
                          int64_t offset, int whence, GError **err);

/* Get current position */
int64_t _openslide_http_tell(struct _openslide_http_file *file,
                             GError **err);

/* Get file size */
int64_t _openslide_http_size(struct _openslide_http_file *file,
                             GError **err);

/* Close file handle (decrements refcount, actual cleanup when refcount=0) */
void _openslide_http_close(struct _openslide_http_file *file);

/* Get URI string */
const char *_openslide_http_get_uri(struct _openslide_http_file *file);

#endif /* OPENSLIDE_OPENSLIDE_HTTP_H_ */
