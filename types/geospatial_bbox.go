package types

import (
	"encoding/binary"
)

// BoundingBoxCalculator accumulates coordinate bounds from geospatial data
type BoundingBoxCalculator struct {
	minX, minY, maxX, maxY float64
	initialized            bool
	// unreadable records a value whose coordinates this walk could not read: bytes that
	// are not WKB it understands, or a geometry type it has no reader for.
	unreadable bool
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

// markUnreadable records a value whose coordinates could not be read, withdrawing the bounds.
func (b *BoundingBoxCalculator) markUnreadable() {
	// A box covering only the values the walk understood is smaller than the data, and a
	// reader pushing a spatial filter down to it would skip rows that match.
	b.unreadable = true
}

// BoundsUnknown reports whether a value was added whose coordinates could not be read.
func (b *BoundingBoxCalculator) BoundsUnknown() bool {
	// This is how a caller tells bounds that describe less than the values given from
	// having been given nothing to measure; GetBounds returns false for both.
	return b.unreadable
}

// GetBounds returns the calculated bounding box coordinates, and false where there are
// none to report or a value in the set was one the walk could not read.
func (b *BoundingBoxCalculator) GetBounds() (minX, minY, maxX, maxY float64, ok bool) {
	if !b.initialized || b.unreadable {
		return 0, 0, 0, 0, false
	}
	return b.minX, b.minY, b.maxX, b.maxY, true
}

func u32(b []byte, offset int, bigEndian bool) (uint32, bool) {
	if offset < 0 || offset+4 > len(b) {
		return 0, false
	}
	if bigEndian {
		return binary.BigEndian.Uint32(b[offset : offset+4]), true
	}
	return binary.LittleEndian.Uint32(b[offset : offset+4]), true
}

// addPointsFromCoords adds all coordinate pairs to the calculator.
func (b *BoundingBoxCalculator) addPointsFromCoords(coords [][]float64) {
	// A position carries an elevation where the type declares Z, and the box is x and y.
	for _, point := range coords {
		if len(point) >= 2 {
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

func (b *BoundingBoxCalculator) addMultiPointWKB(wkb []byte, off int, be bool, gType uint32) {
	tempCalc := NewBoundingBoxCalculator()
	n, ok := u32(wkb, off, be)
	if !ok {
		b.markUnreadable()
		return
	}
	off += 4
	for i := uint32(0); i < n; i++ {
		pointBE, newOff, ok := readSubGeomHeader(wkb, off, wkbMemberType(gType, WKBPoint))
		if !ok {
			b.markUnreadable()
			return
		}
		coords, ptOff, ok := parsePoint(wkb, pointBE, newOff, -1, gType)
		if !ok {
			b.markUnreadable()
			return
		}
		if len(coords) >= 2 {
			tempCalc.AddPoint(coords[0], coords[1])
		}
		off = ptOff
	}
	b.mergeTempBounds(tempCalc)
}

func (b *BoundingBoxCalculator) addMultiLineStringWKB(wkb []byte, off int, be bool, gType uint32) {
	tempCalc := NewBoundingBoxCalculator()
	n, ok := u32(wkb, off, be)
	if !ok {
		b.markUnreadable()
		return
	}
	off += 4
	for i := uint32(0); i < n; i++ {
		lineBE, newOff, ok := readSubGeomHeader(wkb, off, wkbMemberType(gType, WKBLineString))
		if !ok {
			b.markUnreadable()
			return
		}
		coords, lineOff, ok := parseLineString(wkb, lineBE, newOff, -1, gType)
		if !ok {
			b.markUnreadable()
			return
		}
		tempCalc.addPointsFromCoords(coords)
		off = lineOff
	}
	b.mergeTempBounds(tempCalc)
}

func (b *BoundingBoxCalculator) addMultiPolygonWKB(wkb []byte, off int, be bool, gType uint32) {
	tempCalc := NewBoundingBoxCalculator()
	n, ok := u32(wkb, off, be)
	if !ok {
		b.markUnreadable()
		return
	}
	off += 4
	for i := uint32(0); i < n; i++ {
		polyBE, newOff, ok := readSubGeomHeader(wkb, off, wkbMemberType(gType, WKBPolygon))
		if !ok {
			b.markUnreadable()
			return
		}
		rings, polyOff, ok := parsePolygon(wkb, polyBE, newOff, -1, gType)
		if !ok {
			b.markUnreadable()
			return
		}
		tempCalc.addPointsFromRings(rings)
		off = polyOff
	}
	b.mergeTempBounds(tempCalc)
}

func (b *BoundingBoxCalculator) addGeometryCollectionWKB(wkb []byte, off int, be bool, depth int) {
	n, ok := u32(wkb, off, be)
	if !ok {
		b.markUnreadable()
		return
	}
	off += 4
	for i := uint32(0); i < n; i++ {
		geomSize, ok := calculateWKBSize(wkb[off:])
		if !ok {
			b.markUnreadable()
			return
		}
		geomEnd := off + geomSize
		if geomEnd > len(wkb) {
			b.markUnreadable()
			return
		}
		_ = b.addWKB(wkb[off:geomEnd], depth+1)
		off = geomEnd
	}
}

// AddWKB recursively processes WKB data to extract all coordinate points
func (b *BoundingBoxCalculator) AddWKB(wkb []byte) error {
	return b.addWKB(wkb, 0)
}

func (b *BoundingBoxCalculator) addWKB(wkb []byte, depth int) error {
	if len(wkb) < 5 {
		b.markUnreadable()
		return nil
	}

	gType, be, ok := readWKBHeader(wkb)
	if !ok {
		// A header the format does not define says nothing about what follows it.
		b.markUnreadable()
		return nil
	}
	off := 5

	// The Parquet bounding box is x and y, so any dimension contributes its first two
	// ordinates and the rest are stepped over.
	const noRound = -1
	switch gType % 1000 {
	case WKBPoint:
		coords, _, ok := parsePoint(wkb, be, off, noRound, gType)
		if !ok || len(coords) < 2 {
			b.markUnreadable()
			return nil
		}
		b.AddPoint(coords[0], coords[1])
	case WKBLineString:
		coords, _, ok := parseLineString(wkb, be, off, noRound, gType)
		if !ok {
			b.markUnreadable()
			return nil
		}
		b.addPointsFromCoords(coords)
	case WKBPolygon:
		coords, _, ok := parsePolygon(wkb, be, off, noRound, gType)
		if !ok {
			b.markUnreadable()
			return nil
		}
		b.addPointsFromRings(coords)
	case WKBMultiPoint:
		b.addMultiPointWKB(wkb, off, be, gType)
	case WKBMultiLineString:
		b.addMultiLineStringWKB(wkb, off, be, gType)
	case WKBMultiPolygon:
		b.addMultiPolygonWKB(wkb, off, be, gType)
	case WKBGeometryCollection:
		if depth >= maxGeometryDepth {
			b.markUnreadable()
			return nil
		}
		b.addGeometryCollectionWKB(wkb, off, be, depth)
	default:
		b.markUnreadable()
	}
	return nil
}
