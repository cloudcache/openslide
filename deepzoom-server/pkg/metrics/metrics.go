// Package metrics provides request metrics collection and real-time updates.
package metrics

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// RequestStats holds statistics for a single request type.
type RequestStats struct {
	Count        int64         `json:"count"`
	TotalTime    time.Duration `json:"total_time_ns"`
	MinTime      time.Duration `json:"min_time_ns"`
	MaxTime      time.Duration `json:"max_time_ns"`
	AvgTime      time.Duration `json:"avg_time_ns"`
	LastTime     time.Duration `json:"last_time_ns"`
	ErrorCount   int64         `json:"error_count"`
	LastAccessed time.Time     `json:"last_accessed"`
}

// Stats holds overall statistics.
type Stats struct {
	Local        *SourceStats     `json:"local"`
	Remote       *SourceStats     `json:"remote"`
	TotalRequest int64            `json:"total_requests"`
	Uptime       time.Duration    `json:"uptime_ns"`
	StartTime    time.Time        `json:"start_time"`
	Endpoints    map[string]int64 `json:"endpoints"`
}

// SourceStats holds statistics for a specific source type (local/remote).
type SourceStats struct {
	Requests     int64         `json:"requests"`
	Errors       int64         `json:"errors"`
	TotalTime    time.Duration `json:"total_time_ns"`
	AvgTime      time.Duration `json:"avg_time_ns"`
	MinTime      time.Duration `json:"min_time_ns"`
	MaxTime      time.Duration `json:"max_time_ns"`
	LastTime     time.Duration `json:"last_time_ns"`
	LastAccessed time.Time     `json:"last_accessed"`
}

// RecentRequest holds info about a recent request.
type RecentRequest struct {
	Path      string        `json:"path"`
	Status    int           `json:"status"`
	Duration  time.Duration `json:"duration_ns"`
	IsRemote  bool          `json:"is_remote"`
	Timestamp time.Time     `json:"timestamp"`
}

// wsConn wraps a websocket connection with its own write mutex
type wsConn struct {
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func (w *wsConn) WriteMessage(messageType int, data []byte) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	return w.conn.WriteMessage(messageType, data)
}

func (w *wsConn) Close() error {
	return w.conn.Close()
}

// Collector collects and aggregates metrics.
type Collector struct {
	mu          sync.RWMutex
	startTime   time.Time
	local       *SourceStats
	remote      *SourceStats
	endpoints   map[string]int64
	recent      []RecentRequest
	recentMax   int
	subscribers map[*wsConn]bool
	subMu       sync.RWMutex
	upgrader    websocket.Upgrader
}

// NewCollector creates a new metrics collector.
func NewCollector() *Collector {
	return &Collector{
		startTime:   time.Now(),
		local:       &SourceStats{MinTime: time.Hour},
		remote:      &SourceStats{MinTime: time.Hour},
		endpoints:   make(map[string]int64),
		recent:      make([]RecentRequest, 0, 100),
		recentMax:   100,
		subscribers: make(map[*wsConn]bool),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins
			},
		},
	}
}

// RecordRequest records a request's metrics.
func (c *Collector) RecordRequest(path string, status int, duration time.Duration, isRemote bool) {
	c.mu.Lock()

	// Choose source stats
	var stats *SourceStats
	if isRemote {
		stats = c.remote
	} else {
		stats = c.local
	}

	stats.Requests++
	stats.TotalTime += duration
	stats.LastTime = duration
	stats.LastAccessed = time.Now()

	if duration < stats.MinTime {
		stats.MinTime = duration
	}
	if duration > stats.MaxTime {
		stats.MaxTime = duration
	}
	if stats.Requests > 0 {
		stats.AvgTime = stats.TotalTime / time.Duration(stats.Requests)
	}

	if status >= 400 {
		stats.Errors++
	}

	// Record endpoint
	c.endpoints[path]++

	// Add to recent requests
	req := RecentRequest{
		Path:      path,
		Status:    status,
		Duration:  duration,
		IsRemote:  isRemote,
		Timestamp: time.Now(),
	}

	if len(c.recent) >= c.recentMax {
		c.recent = c.recent[1:]
	}
	c.recent = append(c.recent, req)

	c.mu.Unlock()

	// Broadcast to WebSocket subscribers
	c.broadcastUpdate(req)
}

// GetStats returns current statistics.
func (c *Collector) GetStats() *Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Make copies
	local := *c.local
	remote := *c.remote

	endpoints := make(map[string]int64)
	for k, v := range c.endpoints {
		endpoints[k] = v
	}

	return &Stats{
		Local:        &local,
		Remote:       &remote,
		TotalRequest: local.Requests + remote.Requests,
		Uptime:       time.Since(c.startTime),
		StartTime:    c.startTime,
		Endpoints:    endpoints,
	}
}

// GetRecentRequests returns recent requests.
func (c *Collector) GetRecentRequests(limit int) []RecentRequest {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if limit <= 0 || limit > len(c.recent) {
		limit = len(c.recent)
	}

	start := len(c.recent) - limit
	result := make([]RecentRequest, limit)
	copy(result, c.recent[start:])
	return result
}

// HandleWebSocket handles WebSocket connections for real-time stats.
func (c *Collector) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	rawConn, err := c.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	conn := &wsConn{conn: rawConn}

	c.subMu.Lock()
	c.subscribers[conn] = true
	c.subMu.Unlock()

	// Send initial stats
	stats := c.GetStats()
	data, _ := json.Marshal(map[string]interface{}{
		"type":  "stats",
		"stats": stats,
	})
	conn.WriteMessage(websocket.TextMessage, data)

	// Send recent requests
	recent := c.GetRecentRequests(20)
	recentData, _ := json.Marshal(map[string]interface{}{
		"type":   "recent",
		"recent": recent,
	})
	conn.WriteMessage(websocket.TextMessage, recentData)

	// Keep connection alive and handle disconnect
	defer func() {
		c.subMu.Lock()
		delete(c.subscribers, conn)
		c.subMu.Unlock()
		conn.Close()
	}()

	for {
		_, _, err := rawConn.ReadMessage()
		if err != nil {
			break
		}
	}
}

// broadcastUpdate sends an update to all WebSocket subscribers.
func (c *Collector) broadcastUpdate(req RecentRequest) {
	c.subMu.RLock()
	defer c.subMu.RUnlock()

	if len(c.subscribers) == 0 {
		return
	}

	data, err := json.Marshal(map[string]interface{}{
		"type":    "request",
		"request": req,
	})
	if err != nil {
		return
	}

	for conn := range c.subscribers {
		go func(ws *wsConn) {
			ws.WriteMessage(websocket.TextMessage, data)
		}(conn)
	}
}
