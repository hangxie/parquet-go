package types

import (
	"encoding/binary"
	"math"
)

// parsePoint parses a WKB Point and returns the coordinates and bytes consumed
func parsePoint(b []byte, be bool, off, precision int, gType uint32) ([]float64, int, bool) {
	ordinates := wkbOrdinates(gType)
	if off+ordinates*8 > len(b) {
		return nil, 0, false
	}
	read := func(at int) float64 {
		if be {
			return math.Float64frombits(binary.BigEndian.Uint64(b[at : at+8]))
		}
		return math.Float64frombits(binary.LittleEndian.Uint64(b[at : at+8]))
	}
	// A position carries x, y and an elevation; a measure stays in the bytes.
	position := []float64{roundCoordinate(read(off), precision), roundCoordinate(read(off+8), precision)}
	if wkbHasZ(gType) {
		position = append(position, roundCoordinate(read(off+16), precision))
	}
	return position, off + ordinates*8, true
}

// parseLineString parses a WKB LineString and returns the coordinates and bytes consumed
func parseLineString(b []byte, be bool, off, precision int, gType uint32) ([][]float64, int, bool) {
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

	if off+int(numPoints)*wkbOrdinates(gType)*8 > len(b) {
		return nil, 0, false
	}

	coords := make([][]float64, 0, numPoints)
	for i := uint32(0); i < numPoints; i++ {
		point, newOff, ok := parsePoint(b, be, off, precision, gType)
		if !ok {
			return nil, 0, false
		}
		coords = append(coords, point)
		off = newOff
	}
	return coords, off, true
}

// parsePolygon parses a WKB Polygon and returns the coordinates and bytes consumed
func parsePolygon(b []byte, be bool, off, precision int, gType uint32) ([][][]float64, int, bool) {
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

	rings := make([][][]float64, 0, cappedCap(numRings, len(b)-off))
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

		if off+int(numPoints)*wkbOrdinates(gType)*8 > len(b) {
			return nil, 0, false
		}

		ring := make([][]float64, 0, numPoints)
		for i := uint32(0); i < numPoints; i++ {
			point, newOff, ok := parsePoint(b, be, off, precision, gType)
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
	// wkbEnd reads each member's own header and checks its type and dimension, where this
	// walk assumed a fixed member size for MultiPoint and stepped over the other Multi*
	// member headers unread.
	end, _, state := wkbEnd(b, 0, 0)
	// An opaque body has no measurable structure at all.
	if state != wkbMeasured {
		return 0, false
	}
	return end, true
}
