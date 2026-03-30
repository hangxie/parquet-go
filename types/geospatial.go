package types

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"math"
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

// AddWKB recursively processes WKB data to extract all coordinate points
func (b *BoundingBoxCalculator) AddWKB(wkb []byte) error {
	if len(wkb) < 5 {
		return nil
	}

	order := wkb[0]
	be := order == 0

	gType, ok := u32(wkb, 1, be)
	if !ok {
		return nil
	}
	off := 5

	// gType may contain extended wkbType (https://libgeos.org/specifications/wkb/#iso-wkb)
	const noRound = -1 // bounding box uses raw coordinates
	switch gType % 1000 {
	case WKBPoint:
		coords, _, ok := parsePoint(wkb, be, off, noRound)
		if ok && len(coords) == 2 {
			b.AddPoint(coords[0], coords[1])
		}
	case WKBLineString:
		coords, _, ok := parseLineString(wkb, be, off, noRound)
		if ok {
			for _, point := range coords {
				if len(point) == 2 {
					b.AddPoint(point[0], point[1])
				}
			}
		}
	case WKBPolygon:
		coords, _, ok := parsePolygon(wkb, be, off, noRound)
		if ok {
			for _, ring := range coords {
				for _, point := range ring {
					if len(point) == 2 {
						b.AddPoint(point[0], point[1])
					}
				}
			}
		}
	case WKBMultiPoint:
		tempCalc := NewBoundingBoxCalculator()
		n, ok := u32(wkb, off, be)
		if !ok {
			return nil
		}
		off += 4
		for i := uint32(0); i < n; i++ {
			if off >= len(wkb) {
				return nil
			}
			pointOrder := wkb[off]
			pointBE := pointOrder == 0
			off++

			pointType, ok := u32(wkb, off, be)
			if !ok || pointType != WKBPoint {
				return nil
			}
			off += 4

			if off+16 > len(wkb) {
				return nil
			}

			var x, y float64
			if pointBE {
				x = math.Float64frombits(binary.BigEndian.Uint64(wkb[off : off+8]))
				y = math.Float64frombits(binary.BigEndian.Uint64(wkb[off+8 : off+16]))
			} else {
				x = math.Float64frombits(binary.LittleEndian.Uint64(wkb[off : off+8]))
				y = math.Float64frombits(binary.LittleEndian.Uint64(wkb[off+8 : off+16]))
			}
			tempCalc.AddPoint(x, y)
			off += 16
		}
		if minX, minY, maxX, maxY, ok := tempCalc.GetBounds(); ok {
			b.AddPoint(minX, minY)
			b.AddPoint(maxX, maxY)
		}
	case WKBMultiLineString:
		tempCalc := NewBoundingBoxCalculator()
		n, ok := u32(wkb, off, be)
		if !ok {
			return nil
		}
		off += 4
		for i := uint32(0); i < n; i++ {
			if off >= len(wkb) {
				return nil
			}
			lineOrder := wkb[off]
			lineBE := lineOrder == 0
			off++

			lineType, ok := u32(wkb, off, be)
			if !ok || lineType != WKBLineString {
				return nil
			}
			off += 4

			if off+4 > len(wkb) {
				return nil
			}

			var pointCount uint32
			if lineBE {
				pointCount = binary.BigEndian.Uint32(wkb[off : off+4])
			} else {
				pointCount = binary.LittleEndian.Uint32(wkb[off : off+4])
			}
			off += 4

			if off+int(pointCount)*16 > len(wkb) {
				return nil
			}
			for j := uint32(0); j < pointCount; j++ {
				if off+16 > len(wkb) {
					return nil
				}

				var x, y float64
				if lineBE {
					x = math.Float64frombits(binary.BigEndian.Uint64(wkb[off : off+8]))
					y = math.Float64frombits(binary.BigEndian.Uint64(wkb[off+8 : off+16]))
				} else {
					x = math.Float64frombits(binary.LittleEndian.Uint64(wkb[off : off+8]))
					y = math.Float64frombits(binary.LittleEndian.Uint64(wkb[off+8 : off+16]))
				}
				tempCalc.AddPoint(x, y)
				off += 16
			}
		}
		if minX, minY, maxX, maxY, ok := tempCalc.GetBounds(); ok {
			b.AddPoint(minX, minY)
			b.AddPoint(maxX, maxY)
		}
	case WKBMultiPolygon:
		tempCalc := NewBoundingBoxCalculator()
		n, ok := u32(wkb, off, be)
		if !ok {
			return nil
		}
		off += 4
		for i := uint32(0); i < n; i++ {
			if off >= len(wkb) {
				return nil
			}
			polyOrder := wkb[off]
			polyBE := polyOrder == 0
			off++

			polyType, ok := u32(wkb, off, be)
			if !ok || polyType != WKBPolygon {
				return nil
			}
			off += 4

			if off+4 > len(wkb) {
				return nil
			}

			var ringCount uint32
			if polyBE {
				ringCount = binary.BigEndian.Uint32(wkb[off : off+4])
			} else {
				ringCount = binary.LittleEndian.Uint32(wkb[off : off+4])
			}
			off += 4

			for r := uint32(0); r < ringCount; r++ {
				if off+4 > len(wkb) {
					return nil
				}

				var pointCount uint32
				if polyBE {
					pointCount = binary.BigEndian.Uint32(wkb[off : off+4])
				} else {
					pointCount = binary.LittleEndian.Uint32(wkb[off : off+4])
				}
				off += 4

				if off+int(pointCount)*16 > len(wkb) {
					return nil
				}
				for j := uint32(0); j < pointCount; j++ {
					if off+16 > len(wkb) {
						return nil
					}

					var x, y float64
					if polyBE {
						x = math.Float64frombits(binary.BigEndian.Uint64(wkb[off : off+8]))
						y = math.Float64frombits(binary.BigEndian.Uint64(wkb[off+8 : off+16]))
					} else {
						x = math.Float64frombits(binary.LittleEndian.Uint64(wkb[off : off+8]))
						y = math.Float64frombits(binary.LittleEndian.Uint64(wkb[off+8 : off+16]))
					}
					tempCalc.AddPoint(x, y)
					off += 16
				}
			}
		}
		if minX, minY, maxX, maxY, ok := tempCalc.GetBounds(); ok {
			b.AddPoint(minX, minY)
			b.AddPoint(maxX, maxY)
		}
	case WKBGeometryCollection:
		n, ok := u32(wkb, off, be)
		if !ok {
			return nil
		}
		off += 4
		for i := uint32(0); i < n; i++ {
			geomStart := off
			geomSize, ok := calculateWKBSize(wkb[off:])
			if !ok {
				return nil
			}
			geomEnd := geomStart + geomSize
			if geomEnd > len(wkb) {
				return nil
			}
			geomWKB := wkb[geomStart:geomEnd]
			// Ignore error for recursive geometry processing
			_ = b.AddWKB(geomWKB)
			off = geomEnd
		}
	}
	return nil
}

// maxCoordPrecision is the upper bound for coordinate rounding precision.
// Beyond 12 decimal places, float64 cannot represent the difference reliably,
// and math.Pow(10, p) grows toward overflow for very large p.
const maxCoordPrecision = 12

// roundCoordinate rounds coordinates based on precision setting.
// Precision is clamped to [0, 12]. Use precision < 0 to disable rounding.
func roundCoordinate(v float64, precision int) float64 {
	if precision < 0 {
		return v
	}
	if precision > maxCoordPrecision {
		precision = maxCoordPrecision
	}
	pow := math.Pow(10, float64(precision))
	return math.Round(v*pow) / pow
}

// wkbToGeoJSON converts WKB (2D Point/LineString/Polygon/Multi*) to a GeoJSON geometry map
// Returns (geoJSON, true) on success; (nil, false) on failure
func wkbToGeoJSON(b []byte, precision int) (map[string]any, bool) {
	if len(b) < 5 {
		return nil, false
	}
	order := b[0] // 0 = BigEndian, 1 = LittleEndian
	be := order == 0

	gType, ok := u32(b, 1, be)
	if !ok {
		return nil, false
	}
	off := 5

	round := func(v float64) float64 {
		return roundCoordinate(v, precision)
	}

	// gType may contain extended wkbType (https://libgeos.org/specifications/wkb/#iso-wkb)
	switch gType % 1000 {
	case WKBPoint:
		coords, _, ok := parsePoint(b, be, off, precision)
		if !ok {
			return nil, false
		}
		return map[string]any{"type": "Point", "coordinates": coords}, true
	case WKBLineString:
		coords, _, ok := parseLineString(b, be, off, precision)
		if !ok {
			return nil, false
		}
		return map[string]any{"type": "LineString", "coordinates": coords}, true
	case WKBPolygon:
		coords, _, ok := parsePolygon(b, be, off, precision)
		if !ok {
			return nil, false
		}
		return map[string]any{"type": "Polygon", "coordinates": coords}, true
	case WKBMultiPoint:
		n, ok := u32(b, off, be)
		if !ok {
			return nil, false
		}
		off += 4
		coords := make([][]float64, 0, n)
		for i := uint32(0); i < n; i++ {
			// Each point starts with byte order and geometry type
			if off >= len(b) {
				return nil, false
			}
			pointOrder := b[off]
			pointBE := pointOrder == 0
			off++

			// Read and verify point type
			pointType, ok := u32(b, off, be)
			if !ok || pointType != WKBPoint {
				return nil, false
			}
			off += 4

			// Read coordinates using the point's byte order
			var x, y float64
			if pointBE {
				if off+16 > len(b) {
					return nil, false
				}
				x = math.Float64frombits(binary.BigEndian.Uint64(b[off : off+8]))
				y = math.Float64frombits(binary.BigEndian.Uint64(b[off+8 : off+16]))
			} else {
				if off+16 > len(b) {
					return nil, false
				}
				x = math.Float64frombits(binary.LittleEndian.Uint64(b[off : off+8]))
				y = math.Float64frombits(binary.LittleEndian.Uint64(b[off+8 : off+16]))
			}
			coords = append(coords, []float64{round(x), round(y)})
			off += 16
		}
		return map[string]any{"type": "MultiPoint", "coordinates": coords}, true
	case WKBMultiLineString:
		n, ok := u32(b, off, be)
		if !ok {
			return nil, false
		}
		off += 4
		lines := make([][][]float64, 0, n)
		for i := uint32(0); i < n; i++ {
			// Each linestring starts with byte order and geometry type
			if off >= len(b) {
				return nil, false
			}
			lineOrder := b[off]
			lineBE := lineOrder == 0
			off++

			// Read and verify linestring type
			lineType, ok := u32(b, off, be)
			if !ok || lineType != WKBLineString {
				return nil, false
			}
			off += 4

			// Read number of points in this linestring
			var pointCount uint32
			if lineBE {
				if off+4 > len(b) {
					return nil, false
				}
				pointCount = binary.BigEndian.Uint32(b[off : off+4])
			} else {
				if off+4 > len(b) {
					return nil, false
				}
				pointCount = binary.LittleEndian.Uint32(b[off : off+4])
			}
			off += 4

			// Read points
			coords := make([][]float64, 0, pointCount)
			for j := uint32(0); j < pointCount; j++ {
				var x, y float64
				if lineBE {
					if off+16 > len(b) {
						return nil, false
					}
					x = math.Float64frombits(binary.BigEndian.Uint64(b[off : off+8]))
					y = math.Float64frombits(binary.BigEndian.Uint64(b[off+8 : off+16]))
				} else {
					if off+16 > len(b) {
						return nil, false
					}
					x = math.Float64frombits(binary.LittleEndian.Uint64(b[off : off+8]))
					y = math.Float64frombits(binary.LittleEndian.Uint64(b[off+8 : off+16]))
				}
				coords = append(coords, []float64{round(x), round(y)})
				off += 16
			}
			lines = append(lines, coords)
		}
		return map[string]any{"type": "MultiLineString", "coordinates": lines}, true
	case WKBMultiPolygon:
		n, ok := u32(b, off, be)
		if !ok {
			return nil, false
		}
		off += 4
		polygons := make([][][][]float64, 0, n)
		for i := uint32(0); i < n; i++ {
			// Each polygon starts with byte order and geometry type
			if off >= len(b) {
				return nil, false
			}
			polyOrder := b[off]
			polyBE := polyOrder == 0
			off++

			// Read and verify polygon type
			polyType, ok := u32(b, off, be)
			if !ok || polyType != WKBPolygon {
				return nil, false
			}
			off += 4

			// Read number of rings in this polygon
			var ringCount uint32
			if polyBE {
				if off+4 > len(b) {
					return nil, false
				}
				ringCount = binary.BigEndian.Uint32(b[off : off+4])
			} else {
				if off+4 > len(b) {
					return nil, false
				}
				ringCount = binary.LittleEndian.Uint32(b[off : off+4])
			}
			off += 4

			// Read rings
			rings := make([][][]float64, 0, ringCount)
			for r := uint32(0); r < ringCount; r++ {
				// Read number of points in this ring
				var pointCount uint32
				if polyBE {
					if off+4 > len(b) {
						return nil, false
					}
					pointCount = binary.BigEndian.Uint32(b[off : off+4])
				} else {
					if off+4 > len(b) {
						return nil, false
					}
					pointCount = binary.LittleEndian.Uint32(b[off : off+4])
				}
				off += 4

				// Read points
				ring := make([][]float64, 0, pointCount)
				for j := uint32(0); j < pointCount; j++ {
					var x, y float64
					if polyBE {
						if off+16 > len(b) {
							return nil, false
						}
						x = math.Float64frombits(binary.BigEndian.Uint64(b[off : off+8]))
						y = math.Float64frombits(binary.BigEndian.Uint64(b[off+8 : off+16]))
					} else {
						if off+16 > len(b) {
							return nil, false
						}
						x = math.Float64frombits(binary.LittleEndian.Uint64(b[off : off+8]))
						y = math.Float64frombits(binary.LittleEndian.Uint64(b[off+8 : off+16]))
					}
					ring = append(ring, []float64{round(x), round(y)})
					off += 16
				}
				rings = append(rings, ring)
			}
			polygons = append(polygons, rings)
		}
		return map[string]any{"type": "MultiPolygon", "coordinates": polygons}, true
	case WKBGeometryCollection:
		n, ok := u32(b, off, be)
		if !ok {
			return nil, false
		}
		off += 4
		geometries := make([]map[string]any, 0, n)
		for i := uint32(0); i < n; i++ {
			// Each geometry in the collection is a complete WKB geometry
			// We need to determine its size to extract it properly
			geomStart := off

			// Use a helper function to calculate WKB geometry size
			geomSize, ok := calculateWKBSize(b[off:])
			if !ok {
				return nil, false
			}

			// Extract the individual geometry WKB
			geomEnd := geomStart + geomSize
			if geomEnd > len(b) {
				return nil, false
			}
			geomWKB := b[geomStart:geomEnd]

			// Recursively parse the individual geometry
			subGeom, ok := wkbToGeoJSON(geomWKB, precision)
			if !ok {
				return nil, false
			}
			geometries = append(geometries, subGeom)
			off = geomEnd
		}
		return map[string]any{"type": "GeometryCollection", "geometries": geometries}, true
	default:
		return nil, false
	}
}

// parsePoint parses a WKB Point and returns the coordinates and bytes consumed
func parsePoint(b []byte, be bool, off, precision int) ([]float64, int, bool) {
	if off+16 > len(b) {
		return nil, 0, false
	}
	var x, y float64
	if be {
		x = math.Float64frombits(binary.BigEndian.Uint64(b[off : off+8]))
		y = math.Float64frombits(binary.BigEndian.Uint64(b[off+8 : off+16]))
	} else {
		x = math.Float64frombits(binary.LittleEndian.Uint64(b[off : off+8]))
		y = math.Float64frombits(binary.LittleEndian.Uint64(b[off+8 : off+16]))
	}
	return []float64{roundCoordinate(x, precision), roundCoordinate(y, precision)}, off + 16, true
}

// parseLineString parses a WKB LineString and returns the coordinates and bytes consumed
func parseLineString(b []byte, be bool, off, precision int) ([][]float64, int, bool) {
	if off+4 > len(b) {
		return nil, 0, false
	}
	var numPoints uint32
	if be {
		numPoints = binary.BigEndian.Uint32(b[off : off+4])
	} else {
		numPoints = binary.LittleEndian.Uint32(b[off : off+4])
	}
	off += 4

	if off+int(numPoints)*16 > len(b) {
		return nil, 0, false
	}

	coords := make([][]float64, 0, numPoints)
	for i := uint32(0); i < numPoints; i++ {
		point, newOff, ok := parsePoint(b, be, off, precision)
		if !ok {
			return nil, 0, false
		}
		coords = append(coords, point)
		off = newOff
	}
	return coords, off, true
}

// parsePolygon parses a WKB Polygon and returns the coordinates and bytes consumed
func parsePolygon(b []byte, be bool, off, precision int) ([][][]float64, int, bool) {
	if off+4 > len(b) {
		return nil, 0, false
	}
	var numRings uint32
	if be {
		numRings = binary.BigEndian.Uint32(b[off : off+4])
	} else {
		numRings = binary.LittleEndian.Uint32(b[off : off+4])
	}
	off += 4

	rings := make([][][]float64, 0, numRings)
	for r := uint32(0); r < numRings; r++ {
		if off+4 > len(b) {
			return nil, 0, false
		}
		var numPoints uint32
		if be {
			numPoints = binary.BigEndian.Uint32(b[off : off+4])
		} else {
			numPoints = binary.LittleEndian.Uint32(b[off : off+4])
		}
		off += 4

		if off+int(numPoints)*16 > len(b) {
			return nil, 0, false
		}

		ring := make([][]float64, 0, numPoints)
		for i := uint32(0); i < numPoints; i++ {
			point, newOff, ok := parsePoint(b, be, off, precision)
			if !ok {
				return nil, 0, false
			}
			ring = append(ring, point)
			off = newOff
		}
		rings = append(rings, ring)
	}
	return rings, off, true
}

// calculateWKBSize determines the total byte size of a WKB geometry
func calculateWKBSize(b []byte) (int, bool) {
	if len(b) < 5 {
		return 0, false
	}

	order := b[0]
	be := order == 0

	// Read geometry type
	var gType uint32
	if be {
		gType = binary.BigEndian.Uint32(b[1:5])
	} else {
		gType = binary.LittleEndian.Uint32(b[1:5])
	}

	off := 5 // byte order + type

	// gType may contain extended wkbType (https://libgeos.org/specifications/wkb/#iso-wkb)
	switch gType % 1000 {
	case WKBPoint:
		_, newOff, ok := parsePoint(b, be, off, -1)
		if !ok {
			return 0, false
		}
		return newOff, true

	case WKBLineString:
		_, newOff, ok := parseLineString(b, be, off, -1)
		if !ok {
			return 0, false
		}
		return newOff, true

	case WKBPolygon:
		_, newOff, ok := parsePolygon(b, be, off, -1)
		if !ok {
			return 0, false
		}
		return newOff, true

	case WKBMultiPoint:
		numPoints, ok := u32(b, off, be)
		if !ok {
			return 0, false
		}
		off += 4
		// Each point has its own byte order + type + coordinates
		return off + int(numPoints)*(1+4+16), true

	case WKBMultiLineString:
		numLines, ok := u32(b, off, be)
		if !ok {
			return 0, false
		}
		off += 4

		for l := uint32(0); l < numLines; l++ {
			// Each LineString has byte order + type + point count + points
			if off+1+4+4 > len(b) {
				return 0, false
			}
			lineOrder := b[off]
			lineBE := lineOrder == 0
			off += 1 + 4 // skip byte order + type

			var linePoints uint32
			if lineBE {
				linePoints = binary.BigEndian.Uint32(b[off : off+4])
			} else {
				linePoints = binary.LittleEndian.Uint32(b[off : off+4])
			}
			off += 4 + int(linePoints)*16
		}
		return off, true

	case WKBMultiPolygon:
		numPolys, ok := u32(b, off, be)
		if !ok {
			return 0, false
		}
		off += 4

		for p := uint32(0); p < numPolys; p++ {
			// Each Polygon has byte order + type + ring count
			if off+1+4+4 > len(b) {
				return 0, false
			}
			polyOrder := b[off]
			polyBE := polyOrder == 0
			off += 1 + 4 // skip byte order + type

			var numRings uint32
			if polyBE {
				numRings = binary.BigEndian.Uint32(b[off : off+4])
			} else {
				numRings = binary.LittleEndian.Uint32(b[off : off+4])
			}
			off += 4

			for r := uint32(0); r < numRings; r++ {
				if off+4 > len(b) {
					return 0, false
				}
				var ringPoints uint32
				if polyBE {
					ringPoints = binary.BigEndian.Uint32(b[off : off+4])
				} else {
					ringPoints = binary.LittleEndian.Uint32(b[off : off+4])
				}
				off += 4 + int(ringPoints)*16
			}
		}
		return off, true

	case WKBGeometryCollection:
		numGeoms, ok := u32(b, off, be)
		if !ok {
			return 0, false
		}
		off += 4

		for g := uint32(0); g < numGeoms; g++ {
			subSize, ok := calculateWKBSize(b[off:])
			if !ok {
				return 0, false
			}
			off += subSize
		}
		return off, true

	default:
		return 0, false
	}
}

func wrapGeoJSONHybrid(geo map[string]any, raw []byte, useBase64, include bool) map[string]any {
	out := map[string]any{"geojson": geo}
	if include {
		if useBase64 {
			out["wkb_b64"] = base64.StdEncoding.EncodeToString(raw)
		} else {
			out["wkb_hex"] = hex.EncodeToString(raw)
		}
	}
	return out
}

func makeGeoJSONFeature(geo, props map[string]any) map[string]any {
	return map[string]any{
		"type":       "Feature",
		"geometry":   geo,
		"properties": props,
	}
}
