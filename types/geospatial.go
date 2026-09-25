package types

import (
	"fmt"
)

// GeospatialJSONMode controls how GEOMETRY/GEOGRAPHY values are rendered to JSON
type GeospatialJSONMode int

const (
	GeospatialModeHex     GeospatialJSONMode = iota // wkb_hex (+ crs/algorithm)
	GeospatialModeBase64                            // wkb_b64 (+ crs/algorithm)
	GeospatialModeGeoJSON                           // GeoJSON geometry (fallback to hex on parse failure)
	GeospatialModeHybrid                            // both: {geojson:..., wkb_hex/base64:..., crs, algorithm}
)

// String returns the mode's name, which the write path's errors name the form by.
func (m GeospatialJSONMode) String() string {
	switch m {
	case GeospatialModeHex:
		return "hex"
	case GeospatialModeBase64:
		return "base64"
	case GeospatialModeGeoJSON:
		return "GeoJSON"
	case GeospatialModeHybrid:
		return "hybrid"
	default:
		return fmt.Sprintf("GeospatialJSONMode(%d)", int(m))
	}
}

// WKB geometry type constants
const (
	WKBPoint              uint32 = 1
	WKBLineString         uint32 = 2
	WKBPolygon            uint32 = 3
	WKBMultiPoint         uint32 = 4
	WKBMultiLineString    uint32 = 5
	WKBMultiPolygon       uint32 = 6
	WKBGeometryCollection uint32 = 7
	// wkbMaxGeometryType is Triangle, the top of the standardized WKB/SQL-MM code space.
	// Codes above 7 have no reader here: their coordinates are declined, their declared
	// type kept.
	wkbMaxGeometryType uint32 = 17
	// maxGeometryDepth caps nested GeometryCollections. Every walk over one recurses per
	// level, and a level costs nine bytes, so a column value carries as many frames as it
	// likes without it. No data nests anywhere near this deep.
	maxGeometryDepth = 32
)

// GeospatialReprojector transforms a GeoJSON geometry from an input CRS to CRS84 (lon/lat degrees).
// Return (geojson, true) if reprojection applied; (nil, false) to indicate failure or not supported.
type GeospatialReprojector func(crs string, geojson map[string]any) (map[string]any, bool)

// GeospatialConfig holds per-instance settings for geospatial JSON rendering.
type GeospatialConfig struct {
	// GeometryJSONMode controls how GEOMETRY values are rendered (default: GeospatialModeHex)
	GeometryJSONMode GeospatialJSONMode
	// GeographyJSONMode controls how GEOGRAPHY values are rendered (default: GeospatialModeGeoJSON)
	GeographyJSONMode GeospatialJSONMode
	// Reprojector is an optional CRS transformation function
	Reprojector GeospatialReprojector
	// HybridUseBase64 selects base64 (true) or hex (false) for raw WKB in Hybrid mode
	HybridUseBase64 bool
	// GeoJSONAsFeature toggles whether GeoJSON mode emits Feature objects (default: true)
	GeoJSONAsFeature bool
	// CoordPrecision sets decimal places to round coordinates for GeoJSON output.
	// Default is 6. Use -1 to disable rounding.
	// See RFC 7946 §11.2: https://datatracker.ietf.org/doc/html/rfc7946#section-11.2
	CoordPrecision int
}

// GeospatialOption is a functional option for configuring GeospatialConfig.
type GeospatialOption func(*GeospatialConfig)

// NewGeospatialConfig creates a GeospatialConfig with default values, modified by opts.
func NewGeospatialConfig(opts ...GeospatialOption) *GeospatialConfig {
	cfg := DefaultGeospatialConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// DefaultGeospatialConfig returns a GeospatialConfig with default values.
func DefaultGeospatialConfig() *GeospatialConfig {
	return &GeospatialConfig{
		GeometryJSONMode:  GeospatialModeHex,
		GeographyJSONMode: GeospatialModeGeoJSON,
		GeoJSONAsFeature:  true,
		CoordPrecision:    6,
	}
}

// WithGeometryJSONMode sets the JSON rendering mode for GEOMETRY values.
func WithGeometryJSONMode(m GeospatialJSONMode) GeospatialOption {
	return func(c *GeospatialConfig) { c.GeometryJSONMode = m }
}

// WithGeographyJSONMode sets the JSON rendering mode for GEOGRAPHY values.
func WithGeographyJSONMode(m GeospatialJSONMode) GeospatialOption {
	return func(c *GeospatialConfig) { c.GeographyJSONMode = m }
}

// WithGeospatialReprojector registers a CRS reprojection function. Pass nil to disable.
func WithGeospatialReprojector(r GeospatialReprojector) GeospatialOption {
	return func(c *GeospatialConfig) { c.Reprojector = r }
}

// WithGeospatialHybridRawBase64 selects base64 (true) or hex (false) for raw WKB in Hybrid mode.
func WithGeospatialHybridRawBase64(useBase64 bool) GeospatialOption {
	return func(c *GeospatialConfig) { c.HybridUseBase64 = useBase64 }
}

// WithGeospatialGeoJSONAsFeature toggles whether GeoJSON mode emits Feature objects.
func WithGeospatialGeoJSONAsFeature(asFeature bool) GeospatialOption {
	return func(c *GeospatialConfig) { c.GeoJSONAsFeature = asFeature }
}

// WithGeospatialCoordinatePrecision sets decimal places to round coordinates for GeoJSON.
// Default is 6. Use -1 to disable rounding.
func WithGeospatialCoordinatePrecision(precision int) GeospatialOption {
	return func(c *GeospatialConfig) { c.CoordPrecision = precision }
}

// defaultGeospatialConfig is a shared immutable instance used when no explicit
// config is provided, avoiding per-call allocation in hot paths.
var defaultGeospatialConfig = DefaultGeospatialConfig()

// maxCoordPrecision is the upper bound for coordinate rounding precision.
// Beyond 12 decimal places, float64 cannot represent the difference reliably,
// and math.Pow(10, p) grows toward overflow for very large p.
const maxCoordPrecision = 12
