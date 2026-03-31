/*
 *  OpenSlide, a library for reading whole slide image files
 *
 *  Copyright (c) 2024
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

#ifndef OPENSLIDE_OPENSLIDE_FSSPEC_H_
#define OPENSLIDE_OPENSLIDE_FSSPEC_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <glib.h>

/* Opaque handle for fsspec-backed file */
struct _openslide_fsspec_file;

/* Check if fsspec backend is available (Python + fsspec installed) */
bool _openslide_fsspec_available(void);

/* Check if path should be handled by fsspec (s3://, gs://, az://, etc.) */
bool _openslide_is_fsspec_path(const char *path);

/* Open a remote file via fsspec */
struct _openslide_fsspec_file *_openslide_fsspec_open(const char *uri, GError **err);

/* Close fsspec file handle */
void _openslide_fsspec_close(struct _openslide_fsspec_file *file);

/* Read from current position - fsspec handles caching internally */
size_t _openslide_fsspec_read(struct _openslide_fsspec_file *file,
                              void *buf, size_t size, GError **err);

/* Read exact bytes or fail */
bool _openslide_fsspec_read_exact(struct _openslide_fsspec_file *file,
                                  void *buf, size_t size, GError **err);

/* Seek - pure memory operation, no network I/O */
bool _openslide_fsspec_seek(struct _openslide_fsspec_file *file,
                            int64_t offset, int whence, GError **err);

/* Tell current position */
int64_t _openslide_fsspec_tell(struct _openslide_fsspec_file *file);

/* Get file size */
int64_t _openslide_fsspec_size(struct _openslide_fsspec_file *file, GError **err);

/* Zero-copy read - returns pointer to internal buffer */
bool _openslide_fsspec_pread_ptr(struct _openslide_fsspec_file *file,
                                 uint64_t offset,
                                 const uint8_t **out_ptr,
                                 size_t *out_avail,
                                 GError **err);

/* Print statistics for debugging */
void _openslide_fsspec_print_stats(void);

#endif /* OPENSLIDE_OPENSLIDE_FSSPEC_H_ */
