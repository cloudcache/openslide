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

#pragma once

#include <glib.h>
#include <stdbool.h>
#include <stdint.h>

G_BEGIN_DECLS

/* HTTP backend configuration */
typedef struct {
  size_t block_size;           /* block size for caching (default 256KB) */
  guint max_cache_blocks;      /* max cached blocks per file */
  int retry_max;               /* max retry attempts */
  int retry_delay_ms;          /* initial backoff delay in ms */
  int connect_timeout_ms;      /* connection timeout in ms */
  int transfer_timeout_ms;     /* transfer timeout in ms */
  int low_speed_limit;         /* low speed threshold bytes/s */
  int low_speed_time;          /* low speed detection time in seconds */
  int pool_ttl_sec;            /* file pool TTL in seconds */
} OpenslideHTTPConfig;

/* Set global HTTP configuration (call once at startup, thread-safe) */
void _openslide_http_set_config(const OpenslideHTTPConfig *cfg);

/* Get current HTTP configuration */
const OpenslideHTTPConfig *_openslide_http_get_config(void);

/* Check if path is a remote URL (http://, https://, s3://) */
bool _openslide_is_remote_path(const char *path);

/* Initialize HTTP subsystem (called automatically, idempotent) */
void _openslide_http_init(void);

/* Cleanup HTTP subsystem (call at program exit) */
void _openslide_http_cleanup(void);

/* Forward declaration - actual struct defined in openslide-http.c */
struct _openslide_http_file;

/* Open a remote file handle */
struct _openslide_http_file *_openslide_http_open(const char *uri,
                                                   GError **err);

/* Read from remote file */
size_t _openslide_http_read(struct _openslide_http_file *file,
                            void *buf, size_t size, GError **err);

/* Read exact bytes from remote file */
bool _openslide_http_read_exact(struct _openslide_http_file *file,
                                void *buf, size_t size, GError **err);

/* Seek in remote file */
bool _openslide_http_seek(struct _openslide_http_file *file,
                          int64_t offset, int whence, GError **err);

/* Get current position in remote file */
int64_t _openslide_http_tell(struct _openslide_http_file *file, GError **err);

/* Get remote file size */
int64_t _openslide_http_size(struct _openslide_http_file *file, GError **err);

/* Close remote file handle */
void _openslide_http_close(struct _openslide_http_file *file);

G_END_DECLS
