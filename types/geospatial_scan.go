package types

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// checkGeospatialColumn reports a geospatial annotation on a column that cannot hold WKB.
// Unreachable through either writer, which validate the schema first, but the conversion
// helpers are exported and take the annotation as given.
func checkGeospatialColumn(typeName string, pT parquet.Type) error {
	if pT != parquet.Type_BYTE_ARRAY {
		return fmt.Errorf("%s requires a BYTE_ARRAY column, not %v", typeName, pT)
	}
	return nil
}

// geospatialMode picks the mode the column is rendered under, which is the mode its
// values are written in: the write path takes whatever shape the read path emits.
func geospatialMode(typeName string, cfg *GeospatialConfig) GeospatialJSONMode {
	if cfg == nil {
		cfg = defaultGeospatialConfig
	}
	if typeName == "GEOGRAPHY" {
		return cfg.GeographyJSONMode
	}
	return cfg.GeometryJSONMode
}

// geoMapField reads one string field from a rendered geospatial map.
func geoMapField(m map[string]any, key string) (string, bool) {
	s, ok := m[key].(string)
	return s, ok && s != ""
}

// wkbOrdinates reports how many doubles a position carries, which the thousands digit of
// the ISO type code names: XY, XYZ, XYM, XYZM.
func wkbOrdinates(gType uint32) int {
	switch gType / 1000 {
	case 1, 2:
		return 3
	case 3:
		return 4
	default:
		return 2
	}
}

// wkbPositions measures a counted run of positions. The bound is computed in uint64 and
// against the bytes that remain, so a count that would overflow int on a 32-bit build is
// refused before it is multiplied rather than wrapping into a negative offset.
func wkbPositions(b []byte, off int, be bool, ordinates int) (int, bool) {
	count, ok := u32(b, off, be)
	if !ok {
		return 0, false
	}
	off += 4
	need := uint64(count) * uint64(ordinates) * 8
	if need > uint64(len(b)-off) {
		return 0, false
	}
	return off + int(need), true
}

type wkbMeasure int

const (
	wkbInvalid wkbMeasure = iota
	wkbMeasured
	wkbOpaque
)

// wkbEnd measures structure in every dimension, or reports an invalid or opaque body.
func wkbEnd(b []byte, off int) (int, uint32, wkbMeasure) {
	if off < 0 || off > len(b) {
		return 0, 0, wkbInvalid
	}
	gType, be, ok := readWKBHeader(b[off:])
	if !ok {
		return 0, 0, wkbInvalid
	}
	off += 5
	ordinates := wkbOrdinates(gType)

	switch gType % 1000 {
	case WKBPoint:
		off += ordinates * 8
		if off > len(b) {
			return 0, 0, wkbInvalid
		}
		return off, gType, wkbMeasured
	case WKBLineString:
		end, ok := wkbPositions(b, off, be, ordinates)
		if !ok {
			return 0, 0, wkbInvalid
		}
		return end, gType, wkbMeasured
	case WKBPolygon:
		rings, ok := u32(b, off, be)
		if !ok {
			return 0, 0, wkbInvalid
		}
		off += 4
		// A ring is at least four bytes, so a count past the remaining length is a lie.
		if uint64(rings) > uint64(len(b)-off) {
			return 0, 0, wkbInvalid
		}
		for range rings {
			if off, ok = wkbPositions(b, off, be, ordinates); !ok {
				return 0, 0, wkbInvalid
			}
		}
		return off, gType, wkbMeasured
	case WKBMultiPoint:
		return wkbMembers(b, off, be, gType, WKBPoint)
	case WKBMultiLineString:
		return wkbMembers(b, off, be, gType, WKBLineString)
	case WKBMultiPolygon:
		return wkbMembers(b, off, be, gType, WKBPolygon)
	case WKBGeometryCollection:
		return wkbMembers(b, off, be, gType, 0)
	}
	return 0, gType, wkbOpaque
}

// wkbMembers checks member types and dimensions, propagating opaque bodies.
func wkbMembers(b []byte, off int, be bool, gType, want uint32) (int, uint32, wkbMeasure) {
	// Multi* members must match want; collections use zero to accept any base type.
	// All members share the container's dimension but carry their own byte order.
	dim := gType / 1000
	members, ok := u32(b, off, be)
	if !ok {
		return 0, 0, wkbInvalid
	}
	off += 4
	// A member is at least five bytes, so a count past the remaining length is a lie.
	if uint64(members) > uint64(len(b)-off) {
		return 0, 0, wkbInvalid
	}
	for range members {
		var member uint32
		var state wkbMeasure
		if off, member, state = wkbEnd(b, off); state == wkbInvalid {
			return 0, 0, wkbInvalid
		}
		if member/1000 != dim || (want != 0 && member%1000 != want) {
			return 0, 0, wkbInvalid
		}
		if state == wkbOpaque {
			return 0, gType, wkbOpaque
		}
	}
	return off, gType, wkbMeasured
}

// checkWKB validates measurable structure, preserving unsupported standardized bodies.
func checkWKB(b []byte, typeName string) error {
	end, _, state := wkbEnd(b, 0)
	if state == wkbInvalid {
		return fmt.Errorf("%s value is not WKB this library can read", typeName)
	}
	// An opaque body prevents locating later members or trailing bytes. Preserve it
	// under the same policy whether it appears at the root or inside a collection.
	if state == wkbMeasured && end != len(b) {
		return fmt.Errorf("%s geometry is %d bytes, value carries %d", typeName, end, len(b))
	}
	return nil
}

// geospatialWKBFromMap reads the WKB a rendered map carries, under the keys the mode uses.
// GeoJSON reaches here only for the renderer's own hex fallback, so it takes either key
// while still naming itself in the error: nobody configured hybrid.
func geospatialWKBFromMap(m map[string]any, typeName string, mode GeospatialJSONMode) ([]byte, error) {
	if s, ok := geoMapField(m, "wkb_hex"); ok && mode != GeospatialModeBase64 {
		b, err := hex.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("%s wkb_hex is not hex: %w", typeName, err)
		}
		return b, nil
	}
	if s, ok := geoMapField(m, "wkb_b64"); ok && mode != GeospatialModeHex {
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("%s wkb_b64 is not base64: %w", typeName, err)
		}
		return b, nil
	}
	return nil, fmt.Errorf("%s in %s mode takes %s", typeName, mode, wkbFormFor(mode))
}

// wkbFormFor names the form a mode's rendering takes, for the error that reports one it
// does not recognise.
func wkbFormFor(mode GeospatialJSONMode) string {
	switch mode {
	case GeospatialModeBase64:
		return "an object carrying wkb_b64"
	case GeospatialModeHybrid:
		return "an object carrying wkb_hex or wkb_b64"
	case GeospatialModeGeoJSON:
		return "a geometry with a type, or the wkb_hex object it falls back to"
	default:
		return "an object carrying wkb_hex"
	}
}

// geoJSONGeometry unwraps the Feature the GeoJSON mode wraps a geometry in.
func geoJSONGeometry(m map[string]any) map[string]any {
	if t, _ := m["type"].(string); t != "Feature" {
		return m
	}
	if g, ok := m["geometry"].(map[string]any); ok {
		return g
	}
	return m
}

// geospatialFromValue scans a geospatial value written in the form its mode renders, and
// returns the WKB the column stores. Symmetry is by mode rather than by shape: whatever a
// GeospatialConfig emits on the read path, the same config accepts here.
func geospatialFromValue(val any, typeName string, cfg *GeospatialConfig) (string, error) {
	m, ok := val.(map[string]any)
	if !ok {
		return "", fmt.Errorf("%s takes the object its rendering produces, got %T", typeName, val)
	}

	mode := geospatialMode(typeName, cfg)
	var b []byte
	var err error
	// A GeoJSON geometry always carries a type; without one, a GeoJSON-mode value is the
	// wkb_hex object the renderer falls back to when it cannot read the bytes, and taking
	// that back is what makes the reader's own output writable.
	gjType, _ := m["type"].(string)
	if mode == GeospatialModeGeoJSON && gjType != "" {
		if b, err = geoJSONToWKB(geoJSONGeometry(m)); err != nil {
			return "", fmt.Errorf("%s: %w", typeName, err)
		}
	} else if b, err = geospatialWKBFromMap(m, typeName, mode); err != nil {
		return "", err
	}

	if err := checkWKB(b, typeName); err != nil {
		return "", err
	}
	return string(b), nil
}

// strToGeospatial scans the JSON text of a rendered geospatial value, which is the only
// way CSVWriter can carry one: every mode renders an object, not a scalar.
func strToGeospatial(s, typeName string, pT parquet.Type, cfg *GeospatialConfig) (any, error) {
	if err := checkGeospatialColumn(typeName, pT); err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, fmt.Errorf("%s takes the JSON text of its rendering: %w", typeName, err)
	}
	return geospatialFromValue(m, typeName, cfg)
}
