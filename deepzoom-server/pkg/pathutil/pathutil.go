// Package pathutil provides unified path handling for local and remote slides.
package pathutil

import (
	"encoding/base64"
	"errors"
	"net/url"
	"strings"
)

// PathType represents the type of slide path.
type PathType int

const (
	PathTypeLocal PathType = iota // Local file system path
	PathTypeHTTP                  // http:// or https://
	PathTypeS3                    // s3://
)

var (
	ErrInvalidPath     = errors.New("invalid path")
	ErrInvalidEncoding = errors.New("invalid path encoding")
)

// SlidePath represents a unified slide path.
type SlidePath struct {
	Raw        string   // Original path from URL
	Type       PathType // Path type
	Normalized string   // Normalized path for OpenSlide
}

// ParseFromURL parses a slide path from HTTP request URL path.
// Supports three formats:
//  1. Plain local path: /api/xxx/path/to/slide.svs (URL decoded)
//  2. Base64 encoded: /api/xxx/base64:xxxxx
//  3. Direct URL: /api/xxx/http://... or /api/xxx/https://...
func ParseFromURL(urlPath string) (*SlidePath, error) {
	if urlPath == "" {
		return nil, ErrInvalidPath
	}

	// Remove leading slash if present
	path := strings.TrimPrefix(urlPath, "/")

	sp := &SlidePath{Raw: urlPath}

	// Check for base64 encoded path
	if strings.HasPrefix(path, "base64:") {
		encoded := strings.TrimPrefix(path, "base64:")
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			// Try URL-safe base64
			decoded, err = base64.URLEncoding.DecodeString(encoded)
			if err != nil {
				return nil, ErrInvalidEncoding
			}
		}
		sp.Normalized = string(decoded)
		sp.Type = detectPathType(sp.Normalized)
		return sp, nil
	}

	// Check for direct URL (http://, https://, s3://)
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		sp.Normalized = path
		sp.Type = PathTypeHTTP
		return sp, nil
	}
	if strings.HasPrefix(path, "s3://") {
		sp.Normalized = path
		sp.Type = PathTypeS3
		return sp, nil
	}

	// Plain local path - URL decode
	decoded, err := url.PathUnescape(path)
	if err != nil {
		return nil, ErrInvalidEncoding
	}

	// Ensure absolute path
	if !strings.HasPrefix(decoded, "/") {
		decoded = "/" + decoded
	}

	sp.Normalized = decoded
	sp.Type = PathTypeLocal
	return sp, nil
}

// ToURLPath converts the path to a safe URL path for frontend use.
func (p *SlidePath) ToURLPath() string {
	switch p.Type {
	case PathTypeHTTP, PathTypeS3:
		// Encode remote URLs with base64
		return "base64:" + base64.StdEncoding.EncodeToString([]byte(p.Normalized))
	default:
		// Encode each path segment for local paths
		parts := strings.Split(strings.TrimPrefix(p.Normalized, "/"), "/")
		for i, part := range parts {
			parts[i] = url.PathEscape(part)
		}
		return strings.Join(parts, "/")
	}
}

// IsRemote returns true if this is a remote path.
func (p *SlidePath) IsRemote() bool {
	return p.Type == PathTypeHTTP || p.Type == PathTypeS3
}

// String returns the normalized path for OpenSlide.
func (p *SlidePath) String() string {
	return p.Normalized
}

// detectPathType detects the path type from a normalized path.
func detectPathType(path string) PathType {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return PathTypeHTTP
	}
	if strings.HasPrefix(path, "s3://") {
		return PathTypeS3
	}
	return PathTypeLocal
}

// EncodePath encodes a path for use in API URLs.
// Local paths are URL-encoded, remote URLs are base64-encoded.
func EncodePath(path string) string {
	pathType := detectPathType(path)
	switch pathType {
	case PathTypeHTTP, PathTypeS3:
		return "base64:" + base64.StdEncoding.EncodeToString([]byte(path))
	default:
		// URL encode each segment
		parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
		for i, part := range parts {
			parts[i] = url.PathEscape(part)
		}
		return strings.Join(parts, "/")
	}
}

// DecodePath decodes a path from an API URL.
func DecodePath(encoded string) (string, error) {
	sp, err := ParseFromURL(encoded)
	if err != nil {
		return "", err
	}
	return sp.Normalized, nil
}
