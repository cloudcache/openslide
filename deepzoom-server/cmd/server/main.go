// DeepZoom Server for OpenSlide
package main

import (
	"flag"
	"log"
	"time"

	"github.com/cloudcache/deepzoom-server/pkg/server"
)

func main() {
	// Parse command line flags
	addr := flag.String("addr", ":8080", "Server address")
	tileSize := flag.Int("tile-size", 254, "Tile size in pixels")
	tileOverlap := flag.Int("tile-overlap", 1, "Tile overlap in pixels")
	tileFormat := flag.String("tile-format", "jpeg", "Tile format (jpeg or png)")
	tileQuality := flag.Int("tile-quality", 85, "JPEG quality (1-100)")
	cacheTTL := flag.Duration("cache-ttl", 30*time.Minute, "Slide cache TTL")
	staticDir := flag.String("static-dir", "./static", "Static files directory")
	flag.Parse()

	config := &server.Config{
		Addr:        *addr,
		TileSize:    *tileSize,
		TileOverlap: *tileOverlap,
		TileFormat:  *tileFormat,
		TileQuality: *tileQuality,
		CacheTTL:    *cacheTTL,
		StaticDir:   *staticDir,
	}

	srv := server.New(config)
	log.Fatal(srv.Run())
}
