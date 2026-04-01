/*
 *  OpenSlide, a library for reading whole slide image files
 *
 *  Copyright (c) 2007-2024 Carnegie Mellon University
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

#ifndef OPENSLIDE_OPENSLIDE_OPENDAL_H_
#define OPENSLIDE_OPENSLIDE_OPENDAL_H_

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <glib.h>

/* Opaque file handle for opendal-backed remote files */
struct _openslide_opendal_file;

/* Check if opendal backend is available (compiled with HAVE_OPENDAL) */
bool _openslide_opendal_available(void);

/* Check if a path should use opendal (s3://, gs://, az://, etc.) */
bool _openslide_is_opendal_path(const char *path);

/* Open a remote file via opendal */
struct _openslide_opendal_file *_openslide_opendal_open(const char *uri,
                                                         GError **err);

/* Close and free resources */
void _openslide_opendal_close(struct _openslide_opendal_file *file);

/* Read data at current position, advance position */
size_t _openslide_opendal_read(struct _openslide_opendal_file *file,
                                void *buf, size_t size, GError **err);

/* Read exact number of bytes, fail if not possible */
bool _openslide_opendal_read_exact(struct _openslide_opendal_file *file,
                                    void *buf, size_t size, GError **err);

/* Seek to position (pure memory operation - no network I/O) */
bool _openslide_opendal_seek(struct _openslide_opendal_file *file,
                              int64_t offset, int whence, GError **err);

/* Get current position */
int64_t _openslide_opendal_tell(struct _openslide_opendal_file *file);

/* Get file size */
int64_t _openslide_opendal_size(struct _openslide_opendal_file *file,
                                 GError **err);

/* Zero-copy read: returns pointer to cached data if available.
 * The pointer is valid until the next read/seek operation. */
bool _openslide_opendal_pread_ptr(struct _openslide_opendal_file *file,
                                   uint64_t offset,
                                   const uint8_t **out_ptr,
                                   size_t *out_avail,
                                   GError **err);

#endif /* OPENSLIDE_OPENSLIDE_OPENDAL_H_ */
