// Package server provides the HTTP server for DeepZoom tile serving.
package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image/jpeg"
	"image/png"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cloudcache/deepzoom-server/pkg/deepzoom"
	"github.com/cloudcache/deepzoom-server/pkg/metrics"
	"github.com/cloudcache/deepzoom-server/pkg/openslide"
	"github.com/cloudcache/deepzoom-server/pkg/pathutil"
	"github.com/gin-gonic/gin"
)

// Server is the DeepZoom HTTP server.
type Server struct {
	router  *gin.Engine
	slides  sync.Map // path -> *slideEntry
	metrics *metrics.Collector
	config  *Config
}

type slideEntry struct {
	slide     *openslide.Slide
	generator *deepzoom.Generator
	lastUsed  time.Time
	mu        sync.Mutex
}

// Config holds server configuration.
type Config struct {
	Addr        string
	TileSize    int
	TileOverlap int
	TileFormat  string
	TileQuality int
	CacheTTL    time.Duration
	StaticDir   string
}

// DefaultConfig returns a default server configuration.
func DefaultConfig() *Config {
	return &Config{
		Addr:        ":8080",
		TileSize:    254,
		TileOverlap: 1,
		TileFormat:  "jpeg",
		TileQuality: 85,
		CacheTTL:    30 * time.Minute,
		StaticDir:   "./static",
	}
}

// New creates a new server instance.
func New(config *Config) *Server {
	if config == nil {
		config = DefaultConfig()
	}

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	s := &Server{
		router:  router,
		metrics: metrics.NewCollector(),
		config:  config,
	}

	s.setupRoutes()
	go s.cleanupLoop()

	return s
}

func (s *Server) setupRoutes() {
	// Metrics middleware
	s.router.Use(s.metricsMiddleware())

	// CORS
	s.router.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// API routes
	api := s.router.Group("/api")
	{
		api.GET("/metadata/*path", s.handleMetadata)
		api.GET("/thumbnail/*path", s.handleThumbnail)
		api.GET("/label/*path", s.handleLabel)
		api.GET("/macro/*path", s.handleMacro)
		api.GET("/crop/*path", s.handleCrop)
		api.GET("/stats", s.handleStats)
	}

	// DeepZoom routes
	s.router.GET("/dzi/*path", s.handleDZI)
	s.router.GET("/tiles/*path", s.handleTile)

	// WebSocket for real-time stats
	s.router.GET("/ws/stats", s.handleWebSocket)

	// Static files (frontend)
	s.router.Static("/static", s.config.StaticDir)
	s.router.StaticFile("/", s.config.StaticDir+"/index.html")
}

func (s *Server) metricsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		// Extract path to determine if remote
		rawPath := c.Param("path")
		isRemote := false
		if rawPath != "" {
			sp, err := pathutil.ParseFromURL(rawPath)
			if err == nil {
				isRemote = sp.IsRemote()
			}
		}

		c.Next()

		duration := time.Since(start)
		s.metrics.RecordRequest(c.Request.URL.Path, c.Writer.Status(), duration, isRemote)
	}
}

// getSlide retrieves or opens a slide.
func (s *Server) getSlide(slidePath *pathutil.SlidePath) (*slideEntry, error) {
	path := slidePath.String()

	// Check cache
	if entry, ok := s.slides.Load(path); ok {
		e := entry.(*slideEntry)
		e.mu.Lock()
		e.lastUsed = time.Now()
		e.mu.Unlock()
		return e, nil
	}

	// Open slide
	slide, err := openslide.Open(path)
	if err != nil {
		return nil, err
	}

	// Create generator
	gen, err := deepzoom.NewGenerator(slide, &deepzoom.GeneratorOptions{
		TileSize: s.config.TileSize,
		Overlap:  s.config.TileOverlap,
		Format:   s.config.TileFormat,
		Quality:  s.config.TileQuality,
	})
	if err != nil {
		slide.Close()
		return nil, err
	}

	entry := &slideEntry{
		slide:     slide,
		generator: gen,
		lastUsed:  time.Now(),
	}

	s.slides.Store(path, entry)
	return entry, nil
}

func (s *Server) parsePath(c *gin.Context) (*pathutil.SlidePath, error) {
	rawPath := c.Param("path")
	if rawPath == "" {
		return nil, fmt.Errorf("path is required")
	}
	return pathutil.ParseFromURL(rawPath)
}

// handleMetadata returns slide metadata.
func (s *Server) handleMetadata(c *gin.Context) {
	sp, err := s.parsePath(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	entry, err := s.getSlide(sp)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	meta, err := entry.slide.GetMetadata()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, meta)
}

// handleThumbnail returns the thumbnail associated image.
func (s *Server) handleThumbnail(c *gin.Context) {
	s.handleAssociatedImage(c, "thumbnail")
}

// handleLabel returns the label associated image.
func (s *Server) handleLabel(c *gin.Context) {
	s.handleAssociatedImage(c, "label")
}

// handleMacro returns the macro associated image.
func (s *Server) handleMacro(c *gin.Context) {
	s.handleAssociatedImage(c, "macro")
}

func (s *Server) handleAssociatedImage(c *gin.Context, name string) {
	sp, err := s.parsePath(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	entry, err := s.getSlide(sp)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	img, err := entry.slide.GetAssociatedImage(name)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	// Encode as JPEG
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 85}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Data(http.StatusOK, "image/jpeg", buf.Bytes())
}

// handleCrop returns a cropped region of the slide.
func (s *Server) handleCrop(c *gin.Context) {
	sp, err := s.parsePath(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Parse query parameters
	level, _ := strconv.ParseInt(c.DefaultQuery("level", "0"), 10, 32)
	x, _ := strconv.ParseInt(c.Query("x"), 10, 64)
	y, _ := strconv.ParseInt(c.Query("y"), 10, 64)
	w, _ := strconv.ParseInt(c.Query("w"), 10, 32)
	h, _ := strconv.ParseInt(c.Query("h"), 10, 32)
	format := c.DefaultQuery("format", "jpeg")
	quality, _ := strconv.Atoi(c.DefaultQuery("quality", "85"))

	if w <= 0 || h <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "width and height are required"})
		return
	}

	entry, err := s.getSlide(sp)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	img, err := entry.slide.ReadRegion(int32(level), x, y, int32(w), int32(h))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	var buf bytes.Buffer
	var contentType string

	switch format {
	case "png":
		if err := png.Encode(&buf, img); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		contentType = "image/png"
	default:
		if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: quality}); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		contentType = "image/jpeg"
	}

	c.Data(http.StatusOK, contentType, buf.Bytes())
}

// handleDZI returns the DZI descriptor.
func (s *Server) handleDZI(c *gin.Context) {
	sp, err := s.parsePath(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	entry, err := s.getSlide(sp)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	dzi, err := entry.generator.GetDZI()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Data(http.StatusOK, "application/xml", dzi)
}

// handleTile returns a DeepZoom tile.
// Path format: /tiles/{path}_files/{level}/{col}_{row}.{format}
func (s *Server) handleTile(c *gin.Context) {
	fullPath := c.Param("path")

	// Parse: /path/to/slide.svs_files/12/3_4.jpeg
	filesIdx := strings.LastIndex(fullPath, "_files/")
	if filesIdx == -1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid tile path"})
		return
	}

	slidePath := fullPath[:filesIdx]
	tilePart := fullPath[filesIdx+7:] // after "_files/"

	// Parse level/col_row.format
	parts := strings.Split(tilePart, "/")
	if len(parts) != 2 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid tile path format"})
		return
	}

	level, err := strconv.Atoi(parts[0])
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid level"})
		return
	}

	// Parse col_row.format
	tileFile := parts[1]
	dotIdx := strings.LastIndex(tileFile, ".")
	if dotIdx == -1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid tile filename"})
		return
	}

	coords := tileFile[:dotIdx]
	coordParts := strings.Split(coords, "_")
	if len(coordParts) != 2 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid tile coordinates"})
		return
	}

	col, _ := strconv.Atoi(coordParts[0])
	row, _ := strconv.Atoi(coordParts[1])

	sp, err := pathutil.ParseFromURL(slidePath)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	entry, err := s.getSlide(sp)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}

	tile, err := entry.generator.GetTile(level, col, row)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	contentType := "image/jpeg"
	if s.config.TileFormat == "png" {
		contentType = "image/png"
	}

	c.Data(http.StatusOK, contentType, tile)
}

// handleStats returns current metrics.
func (s *Server) handleStats(c *gin.Context) {
	stats := s.metrics.GetStats()
	c.JSON(http.StatusOK, stats)
}

// handleWebSocket handles WebSocket connections for real-time stats.
func (s *Server) handleWebSocket(c *gin.Context) {
	s.metrics.HandleWebSocket(c.Writer, c.Request)
}

// cleanupLoop periodically cleans up unused slides.
func (s *Server) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		s.slides.Range(func(key, value interface{}) bool {
			entry := value.(*slideEntry)
			entry.mu.Lock()
			if now.Sub(entry.lastUsed) > s.config.CacheTTL {
				entry.slide.Close()
				s.slides.Delete(key)
			}
			entry.mu.Unlock()
			return true
		})
	}
}

// Run starts the HTTP server.
func (s *Server) Run() error {
	log.Printf("Starting DeepZoom server on %s", s.config.Addr)
	log.Printf("OpenSlide version: %s", openslide.GetVersion())
	return s.router.Run(s.config.Addr)
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

// StatsJSON returns current stats as JSON bytes.
func (s *Server) StatsJSON() ([]byte, error) {
	return json.Marshal(s.metrics.GetStats())
}
