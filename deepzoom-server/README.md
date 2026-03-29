# DeepZoom Server for OpenSlide

A high-performance DeepZoom tile server built with Go and CGO, supporting both local and remote (HTTP/HTTPS/S3) whole slide images via OpenSlide.

## Features

- **DeepZoom (DZI) tile serving** - Compatible with OpenSeadragon
- **Remote slide support** - Load slides directly from HTTP/HTTPS/S3 URLs
- **Split-screen comparison** - Compare local and remote slides side by side
- **Real-time performance metrics** - WebSocket-based latency monitoring
- **RESTful API** - Metadata, thumbnails, labels, macros, and ROI cropping

## Prerequisites

- Go 1.21+
- OpenSlide library with HTTP support (see parent project)
- pkg-config

## Installation

```bash
# Install OpenSlide (with HTTP support)
cd ../
meson setup builddir
meson compile -C builddir
sudo meson install -C builddir

# Build the server
cd deepzoom-server
make build
```

## Usage

```bash
# Run the server
make run

# Or with custom options
./bin/deepzoom-server \
  -addr :8080 \
  -tile-size 254 \
  -tile-format jpeg \
  -tile-quality 85 \
  -static-dir ./static
```

## API Endpoints

### DeepZoom

| Endpoint | Description |
|----------|-------------|
| `GET /dzi/{path}` | DZI descriptor XML |
| `GET /tiles/{path}_files/{level}/{col}_{row}.{format}` | Tile image |

### Metadata & Images

| Endpoint | Description |
|----------|-------------|
| `GET /api/metadata/{path}` | Complete slide metadata |
| `GET /api/thumbnail/{path}` | Thumbnail image |
| `GET /api/label/{path}` | Label image |
| `GET /api/macro/{path}` | Macro image |
| `GET /api/crop/{path}?level=&x=&y=&w=&h=` | Crop region |

### Stats

| Endpoint | Description |
|----------|-------------|
| `GET /api/stats` | Performance statistics |
| `WS /ws/stats` | Real-time stats via WebSocket |

## Path Encoding

- **Local paths**: URL-encoded (e.g., `/api/metadata/data/slides/test%20file.svs`)
- **Remote URLs**: Base64-encoded (e.g., `/api/metadata/base64:aHR0cHM6Ly8uLi4=`)

## Frontend

Open `http://localhost:8080` in your browser to access the dual-pane viewer with:
- Left panel: Local slide
- Right panel: Remote slide
- Synchronized navigation (toggle-able)
- Draggable performance monitor

## License

LGPL-2.1 (same as OpenSlide)
