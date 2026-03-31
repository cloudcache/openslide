# OpenSlide HTTP Backend Performance Review

## Overview

Review of `src/openslide-http.c`, `src/openslide-http.h`, `src/openslide-file.c` focusing on seek/read latency for remote WSI access (DZI/metadata/tile). Benchmarked with `openslide-show-properties` against a 177MB Aperio SVS file on GCS.

## Benchmark Data

```
time openslide-show-properties https://storage.googleapis.com/.../Aperio/CMU-1.svs
real  0m5.662s
user  0m0.077s
sys   0m0.099s
```

### Key Metrics from Logs

| Metric | Value | Assessment |
|--------|-------|------------|
| Total wall time | **5.662s** (properties only) | Far too high |
| `_openslide_fopen` calls | **~96 times** (same URI) | Severely redundant |
| Actual HTTP Range requests | **5 requests** / 34,127KB | Data volume reasonable |
| Connection reuse rate | conn_new=1, conn_reuse=93 | Pool working well |
| Front window | 8MB prefetched | Does not cover file tail |
| Tail extent fetches | 4 serial requests (blocks 18-21) | Not merged |

---

## Problem Analysis

### Problem 1: Excessive File Open/Close Cycles (Highest Impact)

**Location**: `openslide-file.c:144-159`

Every `_openslide_fopen` call for a remote file walks the full connection acquisition path:

```
_openslide_fopen
  -> _openslide_http_open
    -> http_connection_get_or_create
      -> pool_mutex lock
      -> http_pool_collect_expired_locked()  // scans ALL pool entries
      -> conn->mutex lock
      -> g_new0(_openslide_http_file)
      -> fprintf(stderr, ...) x2
```

Even with connection pool hits, each of the 96 calls incurs:

- **2 mutex acquisitions** (`pool_mutex` + `conn->mutex`)
- **1 full expired-entry scan** of the connection pool hash table
- **1 heap allocation** (`g_new0`) + later `g_free`
- **2 `fprintf(stderr, ...)`** blocking I/O calls

96 open/close cycles = 192 mutex lock/unlock pairs + 96 expired scans + 192 fprintf calls.

**Root cause**: OpenSlide's original architecture assumes `fopen` is nearly free (true for local files). The HTTP backend inherits this assumption but adds significant per-open overhead.

**Evidence from logs**:
```
[HTTP-OBSERVE] reason=open-threshold opens=32  ... range_requests=5 range_bytes=34127KB
[HTTP-OBSERVE] reason=open-threshold opens=64  ... range_requests=5 range_bytes=34127KB
[HTTP-OBSERVE] reason=open-threshold opens=96  ... range_requests=5 range_bytes=34127KB
```

Between opens 32 and 96, **zero additional range requests were issued** — all opens were pure overhead with no actual data transfer.

---

### Problem 2: Tail Reads Not Covered by Front Window

**Location**: `openslide-http.c:2003-2017` (`http_prime_connection`)

TIFF/SVS files store IFD (Image File Directory) chains that frequently reference the **end** of the file. The current strategy only prefetches a front window:

```c
#define HTTP_FRONT_WINDOW_BYTES  (4 * 1024 * 1024)   // 4MB
#define HTTP_FRONT_WINDOW_MAX_BYTES (8 * 1024 * 1024) // 8MB max
```

Log evidence of tail fetches after front window is loaded:
```
[HTTP-EXTENT] Fetching blocks 18-18 (8388608 bytes)
[HTTP-EXTENT] Fetching blocks 19-19 (8388608 bytes)
[HTTP-EXTENT] Fetching blocks 20-20 (8388608 bytes)
[HTTP-EXTENT] Fetching blocks 21-21 (1391811 bytes)
```

These 4 requests are issued **serially** — each must complete before the next begins. For a GCS endpoint with ~100ms RTT, this alone costs ~400ms minimum.

---

### Problem 3: Serial Extent Fetches Instead of Merged Requests

**Location**: `openslide-http.c:1609-1647` (`http_prefetch_missing_blocks`)

The prefetch loop iterates over missing blocks and calls `http_fetch_extent` for each contiguous gap. However, `extent_cap_end_block` (line 1415-1450) caps each extent:

```c
#define HTTP_NORMAL_MAX_EXTENT_BYTES    (1024 * 1024)   // 1MB cap
#define HTTP_METADATA_MAX_EXTENT_BYTES  (2 * 1024 * 1024) // 2MB for metadata
```

For tail reads that span multiple MB, this forces multiple serial HTTP requests instead of one large request.

---

### Problem 4: `http_fetch_parallel` Is Unused

**Location**: `openslide-http.c:1186`

```c
G_GNUC_UNUSED
static bool http_fetch_parallel(HttpConnection *conn, ...)
```

A parallel fetch function exists but is marked unused. The async downloader infrastructure (`AsyncBatch`, `AsyncEasySlot`, `curl_multi`) is fully implemented but only serves single-range requests through `http_fetch_range`. The batch coalescing (`async_collect_batch_locked`) only merges jobs that happen to be queued simultaneously — serial metadata reads never overlap.

---

### Problem 5: No TIFF-Structure-Aware Prefetching

**Location**: `openslide-http.c:1649-1684` (`http_plan_prefetch_blocks`)

The prefetch planner uses generic heuristics:

```c
if (offset < HTTP_METADATA_WINDOW_BYTES) {
    // expand to metadata window end
} else if (to_read <= HTTP_SMALL_READ_THRESHOLD_BYTES) {
    // add 256KB read-ahead
}
```

It has no awareness that TIFF IFD parsing will immediately seek to the file tail after reading the header. A TIFF-aware strategy would prefetch both head and tail on first open.

---

## Comparison with fsspec

| Feature | Current Implementation | fsspec (HTTPFileSystem) |
|---------|----------------------|------------------------|
| File open | Full pool lookup + mutex + alloc per `fopen` | Single `AbstractBufferedFile` instance, reused |
| Metadata reads | 4-8MB front window + per-block fetch | Configurable `block_size`, `readahead`, single buffer |
| Tail reads | Serial per-block extent fetches | Negative-offset seek + single range request |
| Request merging | Small-block coalescing (max 512KB span) | `cat_ranges()` submits multiple disjoint ranges in batch |
| Caching | LRU block cache (256KB granularity) | Optional `blockcache=True`, whole-file or block-level |
| Size discovery | Range GET with Content-Range header parse | Single HEAD request (lightweight) |
| Parallel I/O | Infrastructure exists but unused | `cat_ranges()` with `asyncio` for concurrent fetches |

### Key fsspec Design Principles Worth Adopting

1. **Open is cheap**: fsspec's `open()` returns a lightweight buffer wrapper — no network calls, no locks beyond the initial `_fetch_range` on first read.

2. **Batch range reads**: `cat_ranges(paths, starts, ends)` allows the caller to declare all needed ranges upfront, enabling the backend to merge or parallelize.

3. **Adaptive block sizing**: fsspec allows per-file `block_size` tuning. For TIFF metadata scanning, a large block size (8-16MB) reduces round trips; for tile reads, smaller blocks avoid over-fetching.

---

## Improvement Proposals

### Proposal 1: Lightweight File Handle (P0 — estimated -2~3s)

**Goal**: Eliminate the 96x open/close overhead.

**Approach**: Make `_openslide_http_open` a near-zero-cost operation when the connection already exists in the pool.

```c
// openslide-http.c — fast path for existing connections

struct _openslide_http_file *_openslide_http_open(const char *uri, GError **err) {
  // Fast path: check pool without expired scan
  HttpConnection *conn = http_connection_get_cached(uri);
  if (conn) {
    // Skip: pool_mutex contention, expired scan, fprintf
    return http_file_new_lightweight(conn);
  }
  // Slow path: full connection setup (only on first open)
  conn = http_connection_get_or_create(uri, err);
  ...
}
```

Key changes:
- Split `http_connection_get_or_create` into fast-path (pool hit) and slow-path (create)
- Move `http_pool_collect_expired_locked()` to a timer or threshold-based trigger instead of every open
- Remove `fprintf` from the hot path (gate behind `HTTP_DEBUG` or environment variable)

---

### Proposal 2: Head + Tail Dual Window Prefetch (P0 — estimated -1~2s)

**Goal**: Eliminate serial tail extent fetches by prefetching both ends on first connection.

```c
#define HTTP_TAIL_WINDOW_BYTES (4 * 1024 * 1024)  // 4MB from end

static bool http_prime_connection(HttpConnection *conn, uint64_t *out_size, GError **err) {
  // Step 1: Fetch front window (existing)
  if (!http_fill_front_window(conn, HTTP_FRONT_WINDOW_BYTES, err))
    return false;

  // Step 2: Fetch tail window in parallel
  if (conn->file_size > HTTP_FRONT_WINDOW_BYTES + HTTP_TAIL_WINDOW_BYTES) {
    uint64_t tail_offset = conn->file_size - HTTP_TAIL_WINDOW_BYTES;
    if (!http_fill_tail_window(conn, tail_offset, HTTP_TAIL_WINDOW_BYTES, err))
      return false;  // Non-fatal: fall back to on-demand
  }

  *out_size = conn->file_size;
  return true;
}
```

New `tail_window` fields on `HttpConnection`:
```c
typedef struct _http_connection {
  ...
  uint8_t *tail_window_data;
  uint64_t tail_window_offset;  // absolute file offset where tail starts
  size_t   tail_window_len;
  ...
} HttpConnection;
```

Ideally, the head and tail fetches should run **concurrently** using `http_fetch_parallel` or two async jobs submitted simultaneously.

---

### Proposal 3: Increase Tail Extent Cap (P1 — estimated -0.5~1s)

**Goal**: When reading near the file tail, merge into fewer, larger requests.

```c
// openslide-http.c — extent_cap_end_block modification

#define HTTP_TAIL_MAX_EXTENT_BYTES  (8 * 1024 * 1024)

static guint64 extent_cap_end_block(HttpConnection *conn,
                                    guint64 start_block,
                                    guint64 requested_end_block) {
  ...
  size_t max_bytes = HTTP_NORMAL_MAX_EXTENT_BYTES;

  if (start_offset < HTTP_METADATA_WINDOW_BYTES) {
    max_bytes = HTTP_METADATA_MAX_EXTENT_BYTES;
  }
  // NEW: tail region gets larger extent cap
  else if (conn->file_size > 0 &&
           start_offset > conn->file_size - HTTP_TAIL_MAX_EXTENT_BYTES) {
    max_bytes = HTTP_TAIL_MAX_EXTENT_BYTES;
  }
  ...
}
```

---

### Proposal 4: Remove fprintf from Hot Path (P1 — estimated -0.1~0.3s)

**Location**: `openslide-file.c:147,158`

```c
// Current: always prints
fprintf(stderr, "[FILE] Opening remote path: %s\n", path);
fprintf(stderr, "[FILE] SUCCESS opened remote: %s\n", path);

// Proposed: compile-time or environment-variable gate
#ifndef OPENSLIDE_FILE_DEBUG
#define OPENSLIDE_FILE_DEBUG 0
#endif

#if OPENSLIDE_FILE_DEBUG
  #define FILE_LOG(...) fprintf(stderr, __VA_ARGS__)
#else
  #define FILE_LOG(...) ((void)0)
#endif
```

---

### Proposal 5: Activate `http_fetch_parallel` for Metadata Phase (P2 — estimated -0.5s)

**Goal**: Use the existing parallel fetch infrastructure during the initial metadata scan.

When `http_prefetch_missing_blocks` detects multiple non-contiguous gaps (e.g., head + tail), submit them simultaneously:

```c
static bool http_prefetch_missing_blocks(HttpConnection *conn,
                                         guint64 start_block,
                                         guint64 end_block,
                                         GError **err) {
  // Collect all missing ranges first
  GArray *ranges = collect_missing_ranges(conn, start_block, end_block);

  if (ranges->len > 1) {
    // Multiple gaps: use parallel fetch
    return http_fetch_parallel_ranges(conn, ranges, err);
  }

  // Single gap: existing serial path
  return http_fetch_extent(conn, ...);
}
```

---

### Proposal 6: fsspec-Inspired Adaptive Block Size (P2 — future)

Allow the block size to adapt based on access pattern:

- **Metadata phase** (first N opens, small scattered reads): Use large blocks (4-8MB) to reduce round trips
- **Tile phase** (larger sequential reads): Use standard blocks (256KB) to avoid over-fetching
- **Detection heuristic**: Track `sequential_hits` field already present in `_openslide_http_file` (line 216)

---

## Expected Impact Summary

| Priority | Improvement | Estimated Savings | Complexity |
|----------|------------|-------------------|------------|
| **P0** | Lightweight file handle (eliminate 96x open overhead) | **-2~3s** | Medium |
| **P0** | Head + Tail dual window prefetch | **-1~2s** | Medium |
| **P1** | Increase tail extent cap | **-0.5~1s** | Low |
| **P1** | Remove fprintf from hot path | **-0.1~0.3s** | Trivial |
| **P2** | Activate parallel fetch for metadata | **-0.5s** | Medium |
| **P2** | Adaptive block sizing | **-0.3s** | High |

**Combined expected result**: `openslide-show-properties` remote access time reduced from **5.6s to ~1~2s** (bounded by network RTT and TLS handshake for first request).

---

## File Reference

| File | Role | Lines |
|------|------|-------|
| `src/openslide-http.h` | Public HTTP backend API (128 lines) | Config struct, file ops, zero-copy API |
| `src/openslide-http.c` | HTTP implementation (2433 lines) | Connection pool, block cache, async downloader, LRU |
| `src/openslide-file.c` | Unified file abstraction (361 lines) | Routes local vs HTTP, seek/read/tell dispatch |

---

*Generated: 2026-03-31*
