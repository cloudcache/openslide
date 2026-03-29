// Package deepzoom provides DeepZoom (DZI) generation for OpenSlide slides.
package deepzoom

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"math"

	"github.com/cloudcache/deepzoom-server/pkg/openslide"
)

const (
	DefaultTileSize = 512  // Larger tiles = fewer HTTP requests, better for object storage
	DefaultOverlap  = 0    // No overlap - reduces tile count and data transfer
	DefaultFormat   = "jpeg"
)

// DZI represents a Deep Zoom Image descriptor.
type DZI struct {
	XMLName  xml.Name `xml:"Image"`
	XMLNS    string   `xml:"xmlns,attr"`
	Format   string   `xml:"Format,attr"`
	Overlap  int      `xml:"Overlap,attr"`
	TileSize int      `xml:"TileSize,attr"`
	Size     DZISize  `xml:"Size"`
}

// DZISize represents the size element in DZI.
type DZISize struct {
	Width  int64 `xml:"Width,attr"`
	Height int64 `xml:"Height,attr"`
}

// Generator generates DeepZoom tiles from OpenSlide slides.
type Generator struct {
	slide    *openslide.Slide
	tileSize int
	overlap  int
	format   string
	quality  int

	// Cached values
	width      int64
	height     int64
	levelCount int
}

// GeneratorOptions configures the DeepZoom generator.
type GeneratorOptions struct {
	TileSize int    // Tile size in pixels (default: 254)
	Overlap  int    // Tile overlap in pixels (default: 1)
	Format   string // Output format: "jpeg" or "png" (default: "jpeg")
	Quality  int    // JPEG quality 1-100 (default: 85)
}

// NewGenerator creates a new DeepZoom generator for a slide.
func NewGenerator(slide *openslide.Slide, opts *GeneratorOptions) (*Generator, error) {
	if opts == nil {
		opts = &GeneratorOptions{}
	}

	tileSize := opts.TileSize
	if tileSize <= 0 {
		tileSize = DefaultTileSize
	}

	overlap := opts.Overlap
	if overlap < 0 {
		overlap = DefaultOverlap
	}

	format := opts.Format
	if format == "" {
		format = DefaultFormat
	}

	quality := opts.Quality
	if quality <= 0 || quality > 100 {
		quality = 85
	}

	width, height, err := slide.GetLevelDimensions(0)
	if err != nil {
		return nil, err
	}

	// Calculate number of DZI levels
	maxDim := width
	if height > maxDim {
		maxDim = height
	}
	levelCount := int(math.Ceil(math.Log2(float64(maxDim)))) + 1

	return &Generator{
		slide:      slide,
		tileSize:   tileSize,
		overlap:    overlap,
		format:     format,
		quality:    quality,
		width:      width,
		height:     height,
		levelCount: levelCount,
	}, nil
}

// GetDZI returns the DZI descriptor XML.
func (g *Generator) GetDZI() ([]byte, error) {
	dzi := DZI{
		XMLNS:    "http://schemas.microsoft.com/deepzoom/2008",
		Format:   g.format,
		Overlap:  g.overlap,
		TileSize: g.tileSize,
		Size: DZISize{
			Width:  g.width,
			Height: g.height,
		},
	}

	output, err := xml.MarshalIndent(dzi, "", "  ")
	if err != nil {
		return nil, err
	}

	return append([]byte(xml.Header), output...), nil
}

// GetTile returns a tile at the specified DZI level and coordinates.
func (g *Generator) GetTile(level, col, row int) ([]byte, error) {
	// Calculate dimensions at this DZI level
	scale := math.Pow(2, float64(g.levelCount-1-level))
	levelWidth := int64(math.Ceil(float64(g.width) / scale))
	levelHeight := int64(math.Ceil(float64(g.height) / scale))

	// Calculate tile bounds
	tileX := col * g.tileSize
	tileY := row * g.tileSize

	// Calculate overlap adjustments
	x := tileX
	y := tileY
	w := g.tileSize + g.overlap
	h := g.tileSize + g.overlap

	if col > 0 {
		x -= g.overlap
		w += g.overlap
	}
	if row > 0 {
		y -= g.overlap
		h += g.overlap
	}

	// Clamp to level dimensions
	if int64(x+w) > levelWidth {
		w = int(levelWidth) - x
	}
	if int64(y+h) > levelHeight {
		h = int(levelHeight) - y
	}

	if w <= 0 || h <= 0 {
		return nil, fmt.Errorf("invalid tile coordinates: level=%d, col=%d, row=%d", level, col, row)
	}

	// Find best OpenSlide level for this DZI level
	osLevel := g.slide.GetBestLevelForDownsample(scale)
	osDownsample, _ := g.slide.GetLevelDownsample(osLevel)

	// Calculate coordinates in level 0
	x0 := int64(float64(x) * scale)
	y0 := int64(float64(y) * scale)

	// Calculate dimensions to read from OpenSlide level
	readW := int32(math.Ceil(float64(w) * scale / osDownsample))
	readH := int32(math.Ceil(float64(h) * scale / osDownsample))

	// Read region
	img, err := g.slide.ReadRegion(osLevel, x0, y0, readW, readH)
	if err != nil {
		return nil, err
	}

	// Resize if necessary (when using a lower resolution level)
	if int(readW) != w || int(readH) != h {
		img = resizeImage(img, w, h)
	}

	// Encode to output format
	return g.encodeImage(img)
}

// GetLevelCount returns the number of DZI levels.
func (g *Generator) GetLevelCount() int {
	return g.levelCount
}

// GetLevelDimensions returns the dimensions at a specific DZI level.
func (g *Generator) GetLevelDimensions(level int) (width, height int64) {
	scale := math.Pow(2, float64(g.levelCount-1-level))
	return int64(math.Ceil(float64(g.width) / scale)),
		int64(math.Ceil(float64(g.height) / scale))
}

// GetTileCount returns the number of tiles at a specific DZI level.
func (g *Generator) GetTileCount(level int) (cols, rows int) {
	w, h := g.GetLevelDimensions(level)
	cols = int(math.Ceil(float64(w) / float64(g.tileSize)))
	rows = int(math.Ceil(float64(h) / float64(g.tileSize)))
	return
}

func (g *Generator) encodeImage(img image.Image) ([]byte, error) {
	var buf bytes.Buffer

	switch g.format {
	case "jpeg", "jpg":
		err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: g.quality})
		if err != nil {
			return nil, err
		}
	case "png":
		err := png.Encode(&buf, img)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported format: %s", g.format)
	}

	return buf.Bytes(), nil
}

// resizeImage performs simple nearest-neighbor resize.
func resizeImage(src *image.RGBA, width, height int) *image.RGBA {
	srcBounds := src.Bounds()
	srcW := srcBounds.Dx()
	srcH := srcBounds.Dy()

	dst := image.NewRGBA(image.Rect(0, 0, width, height))

	xRatio := float64(srcW) / float64(width)
	yRatio := float64(srcH) / float64(height)

	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			srcX := int(float64(x) * xRatio)
			srcY := int(float64(y) * yRatio)
			if srcX >= srcW {
				srcX = srcW - 1
			}
			if srcY >= srcH {
				srcY = srcH - 1
			}
			dst.Set(x, y, src.At(srcX, srcY))
		}
	}

	return dst
}
