// Package openslide provides Go bindings for the OpenSlide library.
package openslide

/*
#cgo pkg-config: openslide
#include <openslide/openslide.h>
#include <stdlib.h>
#include <string.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"image"
	"image/color"
	"runtime"
	"strings"
	"sync"
	"unsafe"
)

var (
	ErrOpenFailed     = errors.New("failed to open slide")
	ErrReadFailed     = errors.New("failed to read region")
	ErrInvalidLevel   = errors.New("invalid level")
	ErrAssocNotFound  = errors.New("associated image not found")
)

// Slide represents an opened whole slide image.
type Slide struct {
	osr      *C.openslide_t
	path     string
	isRemote bool
	mu       sync.RWMutex
	closed   bool
}

// LevelInfo contains information about a single level.
type LevelInfo struct {
	Level      int32   `json:"level"`
	Width      int64   `json:"width"`
	Height     int64   `json:"height"`
	Downsample float64 `json:"downsample"`
}

// Metadata contains complete slide metadata.
type Metadata struct {
	Path             string            `json:"path"`
	IsRemote         bool              `json:"is_remote"`
	Vendor           string            `json:"vendor"`
	QuickHash        string            `json:"quickhash"`
	LevelCount       int32             `json:"level_count"`
	Levels           []LevelInfo       `json:"levels"`
	Properties       map[string]string `json:"properties"`
	AssociatedImages []string          `json:"associated_images"`
	MPP              float64           `json:"mpp,omitempty"`
	ObjectivePower   float64           `json:"objective_power,omitempty"`
}

// Open opens a whole slide image file.
// The path can be a local file path or a remote URL (http://, https://, s3://).
func Open(path string) (*Slide, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	osr := C.openslide_open(cPath)
	if osr == nil {
		return nil, fmt.Errorf("%w: %s", ErrOpenFailed, path)
	}

	// Check for errors
	if errStr := C.openslide_get_error(osr); errStr != nil {
		err := C.GoString(errStr)
		C.openslide_close(osr)
		return nil, fmt.Errorf("%w: %s - %s", ErrOpenFailed, path, err)
	}

	isRemote := strings.HasPrefix(path, "http://") ||
		strings.HasPrefix(path, "https://") ||
		strings.HasPrefix(path, "s3://")

	s := &Slide{
		osr:      osr,
		path:     path,
		isRemote: isRemote,
	}

	runtime.SetFinalizer(s, (*Slide).Close)
	return s, nil
}

// Close closes the slide and releases resources.
func (s *Slide) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	if s.osr != nil {
		C.openslide_close(s.osr)
		s.osr = nil
	}
	s.closed = true
	runtime.SetFinalizer(s, nil)
	return nil
}

// Path returns the slide file path.
func (s *Slide) Path() string {
	return s.path
}

// IsRemote returns true if this is a remote slide.
func (s *Slide) IsRemote() bool {
	return s.isRemote
}

// GetLevelCount returns the number of levels in the slide.
func (s *Slide) GetLevelCount() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0
	}
	return int32(C.openslide_get_level_count(s.osr))
}

// GetLevelDimensions returns the dimensions of a specific level.
func (s *Slide) GetLevelDimensions(level int32) (width, height int64, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, 0, errors.New("slide is closed")
	}

	levelCount := int32(C.openslide_get_level_count(s.osr))
	if level < 0 || level >= levelCount {
		return 0, 0, ErrInvalidLevel
	}

	var w, h C.int64_t
	C.openslide_get_level_dimensions(s.osr, C.int32_t(level), &w, &h)
	return int64(w), int64(h), nil
}

// GetLevelDownsample returns the downsample factor of a specific level.
func (s *Slide) GetLevelDownsample(level int32) (float64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0, errors.New("slide is closed")
	}

	levelCount := int32(C.openslide_get_level_count(s.osr))
	if level < 0 || level >= levelCount {
		return 0, ErrInvalidLevel
	}

	return float64(C.openslide_get_level_downsample(s.osr, C.int32_t(level))), nil
}

// GetBestLevelForDownsample returns the best level for the given downsample factor.
func (s *Slide) GetBestLevelForDownsample(downsample float64) int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return 0
	}
	return int32(C.openslide_get_best_level_for_downsample(s.osr, C.double(downsample)))
}

// ReadRegion reads a region from the slide at the specified level.
// x and y are in level 0 coordinates.
func (s *Slide) ReadRegion(level int32, x, y int64, width, height int32) (*image.RGBA, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errors.New("slide is closed")
	}

	levelCount := int32(C.openslide_get_level_count(s.osr))
	if level < 0 || level >= levelCount {
		return nil, ErrInvalidLevel
	}

	// Allocate buffer for ARGB data
	bufSize := int(width) * int(height) * 4
	buf := make([]byte, bufSize)

	C.openslide_read_region(
		s.osr,
		(*C.uint32_t)(unsafe.Pointer(&buf[0])),
		C.int64_t(x),
		C.int64_t(y),
		C.int32_t(level),
		C.int64_t(width),
		C.int64_t(height),
	)

	// Check for errors
	if errStr := C.openslide_get_error(s.osr); errStr != nil {
		return nil, fmt.Errorf("%w: %s", ErrReadFailed, C.GoString(errStr))
	}

	// Convert ARGB (OpenSlide) to RGBA (Go image)
	img := image.NewRGBA(image.Rect(0, 0, int(width), int(height)))
	for i := 0; i < int(width)*int(height); i++ {
		// OpenSlide uses pre-multiplied ARGB in native byte order
		offset := i * 4
		b := buf[offset+0]
		g := buf[offset+1]
		r := buf[offset+2]
		a := buf[offset+3]

		// Un-premultiply alpha
		if a > 0 && a < 255 {
			r = uint8(int(r) * 255 / int(a))
			g = uint8(int(g) * 255 / int(a))
			b = uint8(int(b) * 255 / int(a))
		}

		img.SetRGBA(i%int(width), i/int(width), color.RGBA{R: r, G: g, B: b, A: a})
	}

	return img, nil
}

// GetProperties returns all slide properties.
func (s *Slide) GetProperties() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil
	}

	props := make(map[string]string)
	names := C.openslide_get_property_names(s.osr)
	if names == nil {
		return props
	}

	// Iterate through null-terminated array
	for i := 0; ; i++ {
		namePtr := *(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(names)) + uintptr(i)*unsafe.Sizeof(names)))
		if namePtr == nil {
			break
		}
		name := C.GoString(namePtr)
		valuePtr := C.openslide_get_property_value(s.osr, namePtr)
		if valuePtr != nil {
			props[name] = C.GoString(valuePtr)
		}
	}

	return props
}

// GetAssociatedImageNames returns the names of available associated images.
func (s *Slide) GetAssociatedImageNames() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil
	}

	var names []string
	cNames := C.openslide_get_associated_image_names(s.osr)
	if cNames == nil {
		return names
	}

	// Iterate through null-terminated array
	for i := 0; ; i++ {
		namePtr := *(**C.char)(unsafe.Pointer(uintptr(unsafe.Pointer(cNames)) + uintptr(i)*unsafe.Sizeof(cNames)))
		if namePtr == nil {
			break
		}
		names = append(names, C.GoString(namePtr))
	}

	return names
}

// GetAssociatedImage returns an associated image by name (e.g., "thumbnail", "label", "macro").
func (s *Slide) GetAssociatedImage(name string) (*image.RGBA, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errors.New("slide is closed")
	}

	cName := C.CString(name)
	defer C.free(unsafe.Pointer(cName))

	var w, h C.int64_t
	C.openslide_get_associated_image_dimensions(s.osr, cName, &w, &h)

	if w <= 0 || h <= 0 {
		return nil, ErrAssocNotFound
	}

	// Allocate buffer
	bufSize := int(w) * int(h) * 4
	buf := make([]byte, bufSize)

	C.openslide_read_associated_image(
		s.osr,
		cName,
		(*C.uint32_t)(unsafe.Pointer(&buf[0])),
	)

	// Check for errors
	if errStr := C.openslide_get_error(s.osr); errStr != nil {
		return nil, fmt.Errorf("%w: %s", ErrReadFailed, C.GoString(errStr))
	}

	// Convert ARGB to RGBA
	img := image.NewRGBA(image.Rect(0, 0, int(w), int(h)))
	for i := 0; i < int(w)*int(h); i++ {
		offset := i * 4
		b := buf[offset+0]
		g := buf[offset+1]
		r := buf[offset+2]
		a := buf[offset+3]

		if a > 0 && a < 255 {
			r = uint8(int(r) * 255 / int(a))
			g = uint8(int(g) * 255 / int(a))
			b = uint8(int(b) * 255 / int(a))
		}

		img.SetRGBA(i%int(w), i/int(w), color.RGBA{R: r, G: g, B: b, A: a})
	}

	return img, nil
}

// GetMetadata returns complete slide metadata.
func (s *Slide) GetMetadata() (*Metadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, errors.New("slide is closed")
	}

	props := s.GetProperties()
	levelCount := s.GetLevelCount()

	meta := &Metadata{
		Path:             s.path,
		IsRemote:         s.isRemote,
		Vendor:           props["openslide.vendor"],
		QuickHash:        props["openslide.quickhash-1"],
		LevelCount:       levelCount,
		Levels:           make([]LevelInfo, levelCount),
		Properties:       props,
		AssociatedImages: s.GetAssociatedImageNames(),
	}

	// Populate level info
	for i := int32(0); i < levelCount; i++ {
		w, h, _ := s.GetLevelDimensions(i)
		ds, _ := s.GetLevelDownsample(i)
		meta.Levels[i] = LevelInfo{
			Level:      i,
			Width:      w,
			Height:     h,
			Downsample: ds,
		}
	}

	// Parse MPP if available
	if mppX, ok := props["openslide.mpp-x"]; ok {
		fmt.Sscanf(mppX, "%f", &meta.MPP)
	}

	// Parse objective power if available
	if objPower, ok := props["openslide.objective-power"]; ok {
		fmt.Sscanf(objPower, "%f", &meta.ObjectivePower)
	}

	return meta, nil
}

// DetectVendor returns the vendor string for a file without opening it.
func DetectVendor(path string) string {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	vendor := C.openslide_detect_vendor(cPath)
	if vendor == nil {
		return ""
	}
	return C.GoString(vendor)
}

// GetVersion returns the OpenSlide library version.
func GetVersion() string {
	return C.GoString(C.openslide_get_version())
}
