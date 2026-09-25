package types

// WKBGeometryType reads the geometry type a WKB value declares, reporting false when the
// bytes do not open with a header the format defines.
func WKBGeometryType(b []byte) (int32, bool) {
	gType, _, ok := readWKBHeader(b)
	if !ok {
		return 0, false
	}
	return int32(gType), true
}

// readWKBHeader reads the byte order and geometry type a WKB value opens with.
func readWKBHeader(b []byte) (gType uint32, bigEndian, ok bool) {
	// Refused here is any header the format does not define: a byte order other than 0 or 1,
	// a base type outside the standardized code space, or a dimension beyond ZM. Bytes that
	// are not WKB read as a type number all the same, so every reader checks this before
	// trusting what follows.
	if len(b) < 5 {
		return 0, false, false
	}
	switch b[0] {
	case 0:
		bigEndian = true
	case 1:
	default:
		return 0, false, false
	}
	gType, _ = u32(b, 1, bigEndian)
	base := gType % 1000
	if base < 1 || base > wkbMaxGeometryType || gType-base > 3000 {
		return 0, false, false
	}
	return gType, bigEndian, true
}

// wkbHasZ reports whether the type declares an elevation, which is the third ordinate.
func wkbHasZ(gType uint32) bool {
	dimension := gType / 1000
	return dimension == 1 || dimension == 3
}

// wkbHasM reports whether the type declares a measure, which GeoJSON has no place for.
func wkbHasM(gType uint32) bool {
	dimension := gType / 1000
	return dimension == 2 || dimension == 3
}

// wkbMemberType names the member a container of this type holds, at the same dimension.
func wkbMemberType(gType, base uint32) uint32 {
	return gType - gType%1000 + base
}

// readSubGeomHeader reads a sub-geometry's own byte order and type from the buffer.
func readSubGeomHeader(b []byte, off int, expectedType uint32) (bool, int, bool) {
	// Each member of a Multi* or collection carries its own byte order byte, so the type
	// after it is read with that byte order, not the outer geometry's. Reading it with the
	// outer one turned a legal mixed-endianness value into a parse failure.
	if off >= len(b) {
		return false, 0, false
	}
	var subBE bool
	switch b[off] {
	case 0:
		subBE = true
	case 1:
	default:
		return false, 0, false
	}
	off++
	gType, ok := u32(b, off, subBE)
	if !ok || gType != expectedType {
		return false, 0, false
	}
	return subBE, off + 4, true
}

// cappedCap returns min(int(n), remaining) to prevent huge pre-allocations from
// untrusted count fields when the available bytes can't possibly hold that many elements.
func cappedCap(n uint32, remaining int) int {
	if remaining < 0 {
		remaining = 0
	}
	if int(n) > remaining {
		return remaining
	}
	return int(n)
}
