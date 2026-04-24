package types

import (
	"encoding/binary"
)

// GeospatialJSONMode controls how GEOMETRY/GEOGRAPHY values are rendered to JSON
type GeospatialJSONMode int

const (
	GeospatialModeHex     GeospatialJSONMode = iota // wkb_hex (+ crs/algorithm)
	GeospatialModeBase64                            // wkb_b64 (+ crs/algorithm)
	GeospatialModeGeoJSON                           // GeoJSON geometry (fallback to hex on parse failure)
	GeospatialModeHybrid                            // both: {geojson:..., wkb_hex/base64:..., crs, algorithm}
)

// WKB geometry type constants
const (
	WKBPoint              uint32 = 1
	WKBLineString         uint32 = 2
	WKBPolygon            uint32 = 3
	WKBMultiPoint         uint32 = 4
	WKBMultiLineString    uint32 = 5
	WKBMultiPolygon       uint32 = 6
	WKBGeometryCollection uint32 = 7
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

// BoundingBoxCalculator accumulates coordinate bounds from geospatial data
type BoundingBoxCalculator struct {
	minX, minY, maxX, maxY float64
	initialized            bool
}

// NewBoundingBoxCalculator creates a new bounding box calculator
func NewBoundingBoxCalculator() *BoundingBoxCalculator {
	return &BoundingBoxCalculator{}
}

// AddPoint adds a coordinate point to the bounding box calculation
func (b *BoundingBoxCalculator) AddPoint(x, y float64) {
	if !b.initialized {
		b.minX, b.maxX = x, x
		b.minY, b.maxY = y, y
		b.initialized = true
		return
	}

	b.minX = min(b.minX, x)
	b.maxX = max(b.maxX, x)
	b.minY = min(b.minY, y)
	b.maxY = max(b.maxY, y)
}

// GetBounds returns the calculated bounding box coordinates
func (b *BoundingBoxCalculator) GetBounds() (minX, minY, maxX, maxY float64, ok bool) {
	if !b.initialized {
		return 0, 0, 0, 0, false
	}
	return b.minX, b.minY, b.maxX, b.maxY, true
}

func u32(b []byte, offset int, bigEndian bool) (uint32, bool) {
	if offset+4 > len(b) {
		return 0, false
	}
	if bigEndian {
		return binary.BigEndian.Uint32(b[offset : offset+4]), true
	}
	return binary.LittleEndian.Uint32(b[offset : offset+4]), true
}

// addPointsFromCoords adds all coordinate pairs to the calculator.
func (b *BoundingBoxCalculator) addPointsFromCoords(coords [][]float64) {
	for _, point := range coords {
		if len(point) == 2 {
			b.AddPoint(point[0], point[1])
		}
	}
}

// addPointsFromRings adds all coordinate pairs from polygon rings to the calculator.
func (b *BoundingBoxCalculator) addPointsFromRings(rings [][][]float64) {
	for _, ring := range rings {
		b.addPointsFromCoords(ring)
	}
}

// mergeTempBounds merges bounds from a temporary calculator into this one.
func (b *BoundingBoxCalculator) mergeTempBounds(temp *BoundingBoxCalculator) {
	if minX, minY, maxX, maxY, ok := temp.GetBounds(); ok {
		b.AddPoint(minX, minY)
		b.AddPoint(maxX, maxY)
	}
}

func (b *BoundingBoxCalculator) addMultiPointWKB(wkb []byte, off int, be bool) {
	tempCalc := NewBoundingBoxCalculator()
	n, ok := u32(wkb, off, be)
	if !ok {
		return
	}
	off += 4
	for i := uint32(0); i < n; i++ {
		pointBE, newOff, ok := readSubGeomHeader(wkb, off, be, WKBPoint)
		if !ok {
			return
		}
		coords, ptOff, ok := parsePoint(wkb, pointBE, newOff, -1)
		if !ok {
			return
		}
		if len(coords) == 2 {
			tempCalc.AddPoint(coords[0], coords[1])
		}
		off = ptOff
	}
	b.mergeTempBounds(tempCalc)
}

func (b *BoundingBoxCalculator) addMultiLineStringWKB(wkb []byte, off int, be bool) {
	tempCalc := NewBoundingBoxCalculator()
	n, ok := u32(wkb, off, be)
	if !ok {
		return
	}
	off += 4
	for i := uint32(0); i < n; i++ {
		lineBE, newOff, ok := readSubGeomHeader(wkb, off, be, WKBLineString)
		if !ok {
			return
		}
		coords, lineOff, ok := parseLineString(wkb, lineBE, newOff, -1)
		if !ok {
			return
		}
		tempCalc.addPointsFromCoords(coords)
		off = lineOff
	}
	b.mergeTempBounds(tempCalc)
}

func (b *BoundingBoxCalculator) addMultiPolygonWKB(wkb []byte, off int, be bool) {
	tempCalc := NewBoundingBoxCalculator()
	n, ok := u32(wkb, off, be)
	if !ok {
		return
	}
	off += 4
	for i := uint32(0); i < n; i++ {
		polyBE, newOff, ok := readSubGeomHeader(wkb, off, be, WKBPolygon)
		if !ok {
			return
		}
		rings, polyOff, ok := parsePolygon(wkb, polyBE, newOff, -1)
		if !ok {
			return
		}
		tempCalc.addPointsFromRings(rings)
		off = polyOff
	}
	b.mergeTempBounds(tempCalc)
}

func (b *BoundingBoxCalculator) addGeometryCollectionWKB(wkb []byte, off int, be bool) {
	n, ok := u32(wkb, off, be)
	if !ok {
		return
	}
	off += 4
	for i := uint32(0); i < n; i++ {
		geomSize, ok := calculateWKBSize(wkb[off:])
		if !ok {
			return
		}
		geomEnd := off + geomSize
		if geomEnd > len(wkb) {
			return
		}
		_ = b.AddWKB(wkb[off:geomEnd])
		off = geomEnd
	}
}

// AddWKB recursively processes WKB data to extract all coordinate points
func (b *BoundingBoxCalculator) AddWKB(wkb []byte) error {
	if len(wkb) < 5 {
		return nil
	}

	be := wkb[0] == 0
	gType, ok := u32(wkb, 1, be)
	if !ok {
		return nil
	}
	off := 5

	const noRound = -1
	switch gType % 1000 {
	case WKBPoint:
		coords, _, ok := parsePoint(wkb, be, off, noRound)
		if ok && len(coords) == 2 {
			b.AddPoint(coords[0], coords[1])
		}
	case WKBLineString:
		coords, _, ok := parseLineString(wkb, be, off, noRound)
		if ok {
			b.addPointsFromCoords(coords)
		}
	case WKBPolygon:
		coords, _, ok := parsePolygon(wkb, be, off, noRound)
		if ok {
			b.addPointsFromRings(coords)
		}
	case WKBMultiPoint:
		b.addMultiPointWKB(wkb, off, be)
	case WKBMultiLineString:
		b.addMultiLineStringWKB(wkb, off, be)
	case WKBMultiPolygon:
		b.addMultiPolygonWKB(wkb, off, be)
	case WKBGeometryCollection:
		b.addGeometryCollectionWKB(wkb, off, be)
	}
	return nil
}

// maxCoordPrecision is the upper bound for coordinate rounding precision.
// Beyond 12 decimal places, float64 cannot represent the difference reliably,
// and math.Pow(10, p) grows toward overflow for very large p.
const maxCoordPrecision = 12
