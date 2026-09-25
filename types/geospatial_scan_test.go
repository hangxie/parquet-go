package types

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// geoSE builds a schema element for a geospatial column under the given annotation.
func geoSE(t *testing.T, typeName string) (*parquet.SchemaElement, *parquet.LogicalType) {
	t.Helper()
	lT := createGeometryLogicalType("OGC:CRS84")
	if typeName == "GEOGRAPHY" {
		lT = createGeographyLogicalType("OGC:CRS84", parquet.EdgeInterpolationAlgorithm_SPHERICAL)
	}
	return readSE(parquet.Type_BYTE_ARRAY, nil, lT, 0), lT
}

// geoWriteBack renders a value under cfg and scans the rendering back through both write
// entry points, which is the symmetry the mode convention promises.
func geoWriteBack(t *testing.T, typeName string, wkb []byte, cfg *GeospatialConfig) (string, string) {
	t.Helper()
	se, lT := geoSE(t, typeName)
	opt := WithGeospatialConfig(cfg)

	rendered, err := ConvertValue(string(wkb), se, opt)
	require.NoError(t, err)
	fromNative, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf(rendered),
		parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil, lT, 0, 0, opt)
	require.NoError(t, err)
	text, err := json.Marshal(rendered)
	require.NoError(t, err)

	// JSONWriter hands over the decoded object; CSVWriter its text.
	var decoded any
	require.NoError(t, json.Unmarshal(text, &decoded))
	fromJSON, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf(decoded),
		parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil, lT, 0, 0, opt)
	require.NoError(t, err)
	fromCSV, err := StrToParquetTypeWithLogical(string(text),
		parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil, lT, 0, 0, opt)
	require.NoError(t, err)
	require.Equal(t, fromJSON, fromNative)
	return fromJSON.(string), fromCSV.(string)
}

// TestGeospatialModeRoundTrip is the property the mode convention exists for: whatever a
// GeospatialConfig renders, the same config scans back. Exact for the three modes that
// carry WKB; GeoJSON is the exception TestGeospatialFormatLimits covers.
func TestGeospatialModeRoundTrip(t *testing.T) {
	// POINT(1.5 2.25): exactly representable, so even GeoJSON's rounding keeps it.
	wkb := createSimpleWKBPoint(1.5, 2.25, true)

	for _, typeName := range []string{"GEOMETRY", "GEOGRAPHY"} {
		for _, mode := range []GeospatialJSONMode{
			GeospatialModeHex, GeospatialModeBase64, GeospatialModeHybrid, GeospatialModeGeoJSON,
		} {
			t.Run(typeName+"/"+mode.String(), func(t *testing.T) {
				cfg := NewGeospatialConfig(
					WithGeometryJSONMode(mode), WithGeographyJSONMode(mode),
				)
				fromJSON, fromCSV := geoWriteBack(t, typeName, wkb, cfg)
				require.Equal(t, string(wkb), fromJSON)
				require.Equal(t, string(wkb), fromCSV)
			})
		}
	}
}

// TestGeospatialCarriesEveryDimension pins the symmetry that matters most for the modes
// that carry WKB: the read path renders any bytes in them, including the Z, M and ZM
// geometries Parquet permits and this library's 2D parser cannot read, so the write path
// takes them back untouched rather than running that parser over them.
func TestGeospatialCarriesEveryDimension(t *testing.T) {
	isoPoint := func(code uint32, ords ...float64) string {
		b := []byte{0x01}
		b = binary.LittleEndian.AppendUint32(b, code)
		for _, o := range ords {
			b = binary.LittleEndian.AppendUint64(b, math.Float64bits(o))
		}
		return string(b)
	}
	// The ISO type codes for Point: XY, Z, M and ZM.
	dims := map[string]string{
		"XY": isoPoint(1, 1, 2),
		"Z":  isoPoint(1001, 1, 2, 3),
		"M":  isoPoint(2001, 1, 2, 3),
		"ZM": isoPoint(3001, 1, 2, 3, 4),
	}

	// Hex and base64 only: hybrid and GeoJSON render a geometry alongside the bytes, so
	// they refuse a Z or M value on the read path already (#439) and never produce one
	// for the write path to take.
	for _, mode := range []GeospatialJSONMode{GeospatialModeHex, GeospatialModeBase64} {
		for dim, wkb := range dims {
			t.Run(mode.String()+"/"+dim, func(t *testing.T) {
				cfg := NewGeospatialConfig(WithGeometryJSONMode(mode))
				fromJSON, fromCSV := geoWriteBack(t, "GEOMETRY", []byte(wkb), cfg)
				require.Equal(t, wkb, fromJSON)
				require.Equal(t, wkb, fromCSV)
			})
		}
	}
}

// isoWKB builds a little-endian geometry of any dimension from its type code and raw
// ordinates, which is the only way to get a Z, M or ZM value past a 2D helper.
func isoWKB(code uint32, ords ...float64) []byte {
	b := []byte{0x01}
	b = binary.LittleEndian.AppendUint32(b, code)
	for _, o := range ords {
		b = binary.LittleEndian.AppendUint64(b, math.Float64bits(o))
	}
	return b
}

// TestGeospatialChecksEveryDimension is the other half of the pass-through: a Z, M or ZM
// geometry is carried untouched, but only when it is whole. Measuring structure without
// reading ordinates is what makes both true at once.
func TestGeospatialChecksEveryDimension(t *testing.T) {
	pointZ := isoWKB(1001, 1, 2, 3)
	tests := []struct {
		name   string
		wkb    []byte
		errMsg string
	}{
		{"Point Z", pointZ, ""},
		{"Point M", isoWKB(2001, 1, 2, 3), ""},
		{"Point ZM", isoWKB(3001, 1, 2, 3, 4), ""},
		{"Point Z truncated to its header", isoWKB(1001), "not WKB this library can read"},
		{"Point Z missing an ordinate", isoWKB(1001, 1, 2), "not WKB this library can read"},
		{"Point ZM missing an ordinate", isoWKB(3001, 1, 2, 3), "not WKB this library can read"},
		{"Point Z plus junk", append(append([]byte{}, pointZ...), 0xff, 0xff), "value carries"},
		{
			"LineString Z with a count past the bytes",
			append(isoWKB(1002), 0xff, 0xff, 0xff, 0x7f),
			"not WKB this library can read",
		},
		{"LineString Z", append(isoWKB(1002, 1, 2, 3, 4, 5, 6)[:5],
			append([]byte{0x02, 0x00, 0x00, 0x00}, isoWKB(0, 1, 2, 3, 4, 5, 6)[5:]...)...), ""},
		{
			"LineString Z truncated mid-position",
			append(isoWKB(1002)[:5], append([]byte{0x02, 0x00, 0x00, 0x00},
				isoWKB(0, 1, 2, 3)[5:]...)...),
			"not WKB this library can read",
		},
		{"Polygon Z", polygonZ(), ""},
		{
			"Polygon Z with a ring count past the bytes",
			append(isoWKB(1003), 0xff, 0xff, 0xff, 0x7f),
			"not WKB this library can read",
		},
		{
			"Polygon Z truncated mid-ring",
			append(isoWKB(1003)[:5], 0x01, 0x00, 0x00, 0x00),
			"not WKB this library can read",
		},
		{"MultiPoint Z", multiPointZ(), ""},
		{
			"MultiPoint Z with a member count past the bytes",
			append(isoWKB(1004), 0xff, 0xff, 0xff, 0x7f),
			"not WKB this library can read",
		},
		{
			"MultiPoint Z whose member is truncated",
			append(append(isoWKB(1004)[:5], 0x01, 0x00, 0x00, 0x00), isoWKB(1001, 1, 2)...),
			"not WKB this library can read",
		},
		// ISO WKB gives a container and its members one dimension: a MultiPointZ holds
		// PointZ, a plain MultiPoint holds plain Points, and mixing them is malformed
		// however well formed each half is alone.
		{"GeometryCollection Z holding a Point Z", multiOf(1007, isoWKB(1001, 1, 2, 3)), ""},
		{
			"GeometryCollection XY holding a Z member",
			multiOf(WKBGeometryCollection, isoWKB(1001, 1, 2, 3)),
			"not WKB this library can read",
		},
		{
			"GeometryCollection Z holding an XY member",
			multiOf(1007, isoWKB(WKBPoint, 1, 2)),
			"not WKB this library can read",
		},
		{
			"MultiPoint XY holding a Point Z",
			multiOf(WKBMultiPoint, isoWKB(1001, 1, 2, 3)),
			"not WKB this library can read",
		},
		{
			"MultiPoint Z holding a Point XY",
			multiOf(1004, isoWKB(WKBPoint, 1, 2)),
			"not WKB this library can read",
		},
		{
			"MultiPoint M holding a Point Z",
			multiOf(2004, isoWKB(1001, 1, 2, 3)),
			"not WKB this library can read",
		},
		// Standardized codes above GeometryCollection have no body reader here, so they
		// pass on their header: the read path renders them untouched and refusing them
		// here would make its own output unwritable. TestGeospatialCarriesRecognizedTypes
		// covers the round trip.
		{"CircularString", isoWKB(8, 1, 2), ""},
		{"CircularString Z", isoWKB(1008, 1, 2, 3), ""},
		{"MultiPoint holding an opaque type", multiOf(WKBMultiPoint, isoWKB(8)), "not WKB this library can read"},
		{"collection holding an opaque member of another dimension", multiOf(WKBGeometryCollection, isoWKB(1008)), "not WKB this library can read"},
		{"collection holding an unknown type", multiOf(WKBGeometryCollection, isoWKB(18)), "not WKB this library can read"},
		{"nested collection holding an unknown type", multiOf(WKBGeometryCollection, multiOf(WKBGeometryCollection, isoWKB(18))), "not WKB this library can read"},
		{"opaque collection with unmeasurable trailing bytes", append(multiOf(WKBGeometryCollection, isoWKB(8)), 0xff), ""},

		{
			"GeometryCollection holding a CircularString",
			multiOf(WKBGeometryCollection, isoWKB(8, 1, 2)),
			"",
		},
		// A Multi* is homogeneous in the OGC model, so a member of another type is not
		// the geometry the container declares, however well formed it is on its own.
		{
			"MultiPoint holding a LineString",
			multiOf(WKBMultiPoint, append(isoWKB(WKBLineString)[:5], 0x00, 0x00, 0x00, 0x00)),
			"not WKB this library can read",
		},
		{
			"MultiLineString holding a Point",
			multiOf(WKBMultiLineString, isoWKB(WKBPoint, 1, 2)),
			"not WKB this library can read",
		},
		{
			"MultiPolygon holding a LineString",
			multiOf(WKBMultiPolygon, append(isoWKB(WKBLineString)[:5], 0x00, 0x00, 0x00, 0x00)),
			"not WKB this library can read",
		},
		{"MultiPoint Z holding a Point Z", multiOf(1004, isoWKB(1001, 1, 2, 3)), ""},
		{
			// readWKBHeader already bounds the dimension at ZM, so a code above 3000 is
			// refused before wkbOrdinates could read it as 2D.
			"a dimension code above ZM", isoWKB(4001, 1, 2, 3),
			"not WKB this library can read",
		},
		// The counted fields themselves, each truncated so the count cannot be read.
		{
			"Polygon Z with no ring count", append(isoWKB(1003)[:5], 0x00, 0x00),
			"not WKB this library can read",
		},
		{
			"Polygon Z whose ring has no position count",
			append(append(isoWKB(1003)[:5], 0x01, 0x00, 0x00, 0x00), 0x00, 0x00),
			"not WKB this library can read",
		},
		{
			"MultiPoint with no member count", append(isoWKB(WKBMultiPoint)[:5], 0x00, 0x00),
			"not WKB this library can read",
		},
		{"big-endian Point", bigEndianPoint(), ""},
		{
			// Byte order is per geometry, so a little-endian container may hold a
			// big-endian member; wkbEnd reads each header on its own terms.
			"MultiPoint holding a big-endian member", multiOf(WKBMultiPoint, bigEndianPoint()), "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeHex))
			got, err := geospatialFromValue(
				map[string]any{"wkb_hex": hex.EncodeToString(tt.wkb)}, "GEOMETRY", cfg,
			)
			if tt.errMsg != "" {
				require.ErrorContains(t, err, tt.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, string(tt.wkb), got)
		})
	}
}

// multiOf wraps one member in a container of the given type.
func multiOf(container uint32, member []byte) []byte {
	b := append(isoWKB(container)[:5], 0x01, 0x00, 0x00, 0x00)
	return append(b, member...)
}

// membersOf spells a container of any number of members, where multiOf spells one.
func membersOf(container uint32, members ...[]byte) []byte {
	b := binary.LittleEndian.AppendUint32([]byte{1}, container)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(members)))
	for _, member := range members {
		b = append(b, member...)
	}
	return b
}

// bigEndianPoint is POINT(1 2) with the byte order flag and every field reversed.
func bigEndianPoint() []byte {
	b := []byte{0x00}
	b = binary.BigEndian.AppendUint32(b, WKBPoint)
	b = binary.BigEndian.AppendUint64(b, math.Float64bits(1))
	return binary.BigEndian.AppendUint64(b, math.Float64bits(2))
}

// polygonZ is one Z ring of three positions.
func polygonZ() []byte {
	b := append(isoWKB(1003)[:5], 0x01, 0x00, 0x00, 0x00) // one ring
	b = append(b, 0x03, 0x00, 0x00, 0x00)                 // three positions
	return append(b, isoWKB(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)[5:]...)
}

// multiPointZ is one Z member, which carries its own header.
func multiPointZ() []byte {
	b := append(isoWKB(1004)[:5], 0x01, 0x00, 0x00, 0x00)
	return append(b, isoWKB(1001, 1, 2, 3)...)
}

// TestWKBEndRejectsAnOffsetPastTheValue covers the bound at wkbEnd's entry. No caller can
// reach it — checkWKB starts at zero and wkbMembers recurses on a measured offset — but
// the invariant is maintained by hand across a recursion, so the guard stays and is
// tested where it can be reached at all.
func TestWKBEndRejectsAnOffsetPastTheValue(t *testing.T) {
	b := createSimpleWKBPoint(1, 2, true)
	for _, off := range []int{-1, len(b) + 1} {
		_, _, state := wkbEnd(b, off, 0)
		require.Equal(t, wkbInvalid, state, "offset %d", off)
	}
}

// TestGeospatialCarriesRecognizedTypes covers the standardized WKB codes above the seven
// basic geometries. The hex and base64 renderings pass their bytes through, so the write
// path has to take them back; there is no body reader for them, so the header is all
// either side checks.
func TestGeospatialCarriesRecognizedTypes(t *testing.T) {
	// CircularString, PolyhedralSurface, TIN and Triangle, the top of the code space.
	for _, code := range []uint32{8, 15, 16, 17} {
		wkb := isoWKB(code, 1, 2)
		for _, mode := range []GeospatialJSONMode{GeospatialModeHex, GeospatialModeBase64} {
			cfg := NewGeospatialConfig(WithGeometryJSONMode(mode))
			fromJSON, fromCSV := geoWriteBack(t, "GEOMETRY", wkb, cfg)
			require.Equal(t, string(wkb), fromJSON, "type %d in %s", code, mode)
			require.Equal(t, string(wkb), fromCSV, "type %d in %s", code, mode)
		}
	}
	// Above the code space is still refused, by the header check both sides share.
	_, err := geospatialFromValue(
		map[string]any{"wkb_hex": hex.EncodeToString(isoWKB(18, 1, 2))}, "GEOMETRY",
		NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeHex)),
	)
	require.ErrorContains(t, err, "not WKB this library can read")
}

// TestGeospatialGeoJSONFallback checks that the renderer's hex fallback remains writable.
func TestGeospatialGeoJSONFallback(t *testing.T) {
	// An M geometry, whose measure a GeoJSON position has no place for, so the rendering
	// refuses it and falls back to hex.
	wkb := []byte{0x01}
	wkb = binary.LittleEndian.AppendUint32(wkb, 2001)
	for _, o := range []float64{1, 2, 3} {
		wkb = binary.LittleEndian.AppendUint64(wkb, math.Float64bits(o))
	}
	se, lT := geoSE(t, "GEOMETRY")
	opt := WithGeospatialConfig(NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeGeoJSON)))

	//nolint:staticcheck // the deprecated path is where the fallback rendering survives
	rendered := ConvertToJSONType(string(wkb), se, opt)
	require.Contains(t, rendered, "wkb_hex")

	text, err := json.Marshal(rendered)
	require.NoError(t, err)
	back, err := StrToParquetTypeWithLogical(string(text),
		parquet.TypePtr(parquet.Type_BYTE_ARRAY), nil, lT, 0, 0, opt)
	require.NoError(t, err)
	require.Equal(t, string(wkb), back)
}

// TestGeospatialHybridBase64 covers HybridUseBase64, the one rendering switch the mode
// round trip does not reach: hybrid defaults to the hex half, so the base64 half is only
// exercised here end to end rather than through the scanner alone.
func TestGeospatialHybridBase64(t *testing.T) {
	wkb := createSimpleWKBPoint(7, 8, true)
	cfg := NewGeospatialConfig(
		WithGeometryJSONMode(GeospatialModeHybrid),
		WithGeospatialHybridRawBase64(true),
	)
	fromJSON, fromCSV := geoWriteBack(t, "GEOMETRY", wkb, cfg)
	require.Equal(t, string(wkb), fromJSON)
	require.Equal(t, string(wkb), fromCSV)
}

// TestGeospatialColumnMustBeByteArray covers the annotation on a column that cannot hold
// WKB. Both writers validate the schema first, so this is reachable only by calling the
// conversion helpers directly, as the matching BSON case is.
func TestGeospatialColumnMustBeByteArray(t *testing.T) {
	lT := createGeometryLogicalType("OGC:CRS84")
	i32 := parquet.TypePtr(parquet.Type_INT32)
	value := `{"wkb_hex":"0101000000000000000000f03f0000000000000040"}`

	_, err := StrToParquetTypeWithLogical(value, i32, nil, lT, 0, 0)
	require.ErrorContains(t, err, "GEOMETRY requires a BYTE_ARRAY column, not INT32")

	_, err = JSONTypeToParquetTypeWithLogical(reflect.ValueOf(map[string]any{}), i32, nil, lT, 0, 0)
	require.ErrorContains(t, err, "GEOMETRY requires a BYTE_ARRAY column, not INT32")
}

// TestGeospatialGeoJSONFeature covers the Feature wrapper, which is on by default: the
// geometry has to be found inside it rather than taken for one.
func TestGeospatialGeoJSONFeature(t *testing.T) {
	wkb := createSimpleWKBPoint(3, 4, true)
	for _, asFeature := range []bool{true, false} {
		cfg := NewGeospatialConfig(
			WithGeometryJSONMode(GeospatialModeGeoJSON),
			WithGeospatialGeoJSONAsFeature(asFeature),
		)
		fromJSON, fromCSV := geoWriteBack(t, "GEOMETRY", wkb, cfg)
		require.Equal(t, string(wkb), fromJSON, "asFeature=%v", asFeature)
		require.Equal(t, string(wkb), fromCSV, "asFeature=%v", asFeature)
	}
}

// TestGeospatialFormatLimits pins where GeoJSON cannot carry the column's bytes. None is
// an error: the caller chose GeoJSON output, and the three WKB modes are exact.
func TestGeospatialFormatLimits(t *testing.T) {
	t.Run("coordinates are rounded to CoordPrecision", func(t *testing.T) {
		wkb := createSimpleWKBPoint(1.23456789, 2, true)
		cfg := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeGeoJSON))
		fromJSON, fromCSV := geoWriteBack(t, "GEOMETRY", wkb, cfg)
		require.NotEqual(t, string(wkb), fromJSON, "default precision is 6, so this rounds")
		require.Equal(t, fromJSON, fromCSV)
		require.Equal(t, string(createSimpleWKBPoint(1.234568, 2, true)), fromJSON)

		// The rounding is the config's, not the format's: turning it off restores the
		// exact round trip, which is the lever a caller has.
		exact := NewGeospatialConfig(
			WithGeometryJSONMode(GeospatialModeGeoJSON),
			WithGeospatialCoordinatePrecision(-1),
		)
		fromJSON, _ = geoWriteBack(t, "GEOMETRY", wkb, exact)
		require.Equal(t, string(wkb), fromJSON)
	})

	t.Run("a third ordinate is an elevation, a fourth is refused", func(t *testing.T) {
		encoded, err := geoJSONToWKB(map[string]any{
			"type": "Point", "coordinates": []any{1.0, 2.0, 3.0},
		}, 0)
		require.NoError(t, err)
		gType, _, ok := readWKBHeader(encoded)
		require.True(t, ok)
		require.Equal(t, uint32(1001), gType)

		_, err = geoJSONToWKB(map[string]any{
			"type": "Point", "coordinates": []any{1.0, 2.0, 3.0, 4.0},
		}, 0)
		require.ErrorContains(t, err, "need 2 or 3")
	})
}

// TestGeospatialScanRejects covers values that are not the form the mode renders.
func TestGeospatialScanRejects(t *testing.T) {
	tests := []struct {
		name   string
		mode   GeospatialJSONMode
		val    any
		errMsg string
	}{
		{"WKT text", GeospatialModeHex, "POINT (1 2)", "takes the object"},
		{"a bare string in GeoJSON mode", GeospatialModeGeoJSON, "POINT (1 2)", "takes the object"},
		{
			"hex mode given GeoJSON", GeospatialModeHex,
			map[string]any{"type": "Point", "coordinates": []any{1.0, 2.0}},
			"takes an object carrying wkb_hex",
		},
		{
			"hex mode given base64", GeospatialModeHex,
			map[string]any{"wkb_b64": "AQEAAAA="},
			"takes an object carrying wkb_hex",
		},
		{
			"wkb_hex is not hex", GeospatialModeHex,
			map[string]any{"wkb_hex": "zzzz"},
			"not hex",
		},
		{
			"wkb_b64 is not base64", GeospatialModeBase64,
			map[string]any{"wkb_b64": "!!!!"},
			"not base64",
		},
		{
			"GeoJSON mode given a geometry it cannot encode", GeospatialModeGeoJSON,
			map[string]any{"type": "Circle", "coordinates": []any{1.0, 2.0}},
			"not one this library writes",
		},
		{
			// #418's guard: bytes that are not WKB must not reach a column claiming to
			// hold it. The header check is dimension-agnostic, so this survives the
			// pass-through the Z and M geometries need.
			"hex decodes to something that is not WKB", GeospatialModeHex,
			map[string]any{"wkb_hex": "deadbeef"},
			"not WKB this library can read",
		},
		{
			"hex carries a 2D geometry plus junk", GeospatialModeHex,
			map[string]any{"wkb_hex": "0101000000000000000000f03f0000000000000040ffff"},
			"geometry is 21 bytes, value carries 23",
		},
		{
			"GeoJSON mode given neither a geometry nor the fallback", GeospatialModeGeoJSON,
			map[string]any{"crs": "OGC:CRS84"},
			"in GeoJSON mode takes a geometry with a type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewGeospatialConfig(WithGeometryJSONMode(tt.mode))
			_, err := geospatialFromValue(tt.val, "GEOMETRY", cfg)
			require.ErrorContains(t, err, tt.errMsg)
		})
	}
}

// TestGeospatialModeDefaults pins which mode each annotation writes in with no config,
// since the two differ and the difference decides whether a write is exact.
func TestGeospatialModeDefaults(t *testing.T) {
	require.Equal(t, GeospatialModeHex, geospatialMode("GEOMETRY", nil))
	require.Equal(t, GeospatialModeGeoJSON, geospatialMode("GEOGRAPHY", nil))
}

// TestGeospatialHelpers covers the small readers each mode leans on, whose branches the
// round trips reach only for the mode they belong to.
func TestGeospatialHelpers(t *testing.T) {
	require.Contains(t, wkbFormFor(GeospatialModeHex), "wkb_hex")
	require.Contains(t, wkbFormFor(GeospatialModeBase64), "wkb_b64")
	require.Contains(t, wkbFormFor(GeospatialModeHybrid), "wkb_hex or wkb_b64")
	require.Contains(t, wkbFormFor(GeospatialModeGeoJSON), "geometry with a type")

	require.Equal(t, "GeospatialJSONMode(9)", GeospatialJSONMode(9).String())

	// A Feature with no geometry is passed through as itself, so the error names the
	// geometry that is missing rather than the wrapper.
	bare := map[string]any{"type": "Feature"}
	require.Equal(t, bare, geoJSONGeometry(bare))

	// Hybrid takes either half, whichever HybridUseBase64 put there.
	cfg := NewGeospatialConfig(WithGeometryJSONMode(GeospatialModeHybrid))
	for _, m := range []map[string]any{
		{"wkb_hex": "0101000000000000000000f03f0000000000000040"},
		{"wkb_b64": "AQEAAAAAAAAAAADwPwAAAAAAAABA"},
	} {
		got, err := geospatialFromValue(m, "GEOMETRY", cfg)
		require.NoError(t, err)
		require.Equal(t, string(createSimpleWKBPoint(1, 2, true)), got)
	}
}

// BenchmarkGeospatial measures each mode's rendering against raw mode, which README's
// Geospatial Values points at as the cheaper reading.
func BenchmarkGeospatial(b *testing.B) {
	wkb := string(createSimpleWKBPoint(1.23456789, 2, true))
	lT := createGeometryLogicalType("OGC:CRS84")
	se := readSE(parquet.Type_BYTE_ARRAY, nil, lT, 0)

	cases := []struct {
		name string
		opts []ValueOption
	}{{"raw", []ValueOption{WithValueMode(ValueModeRaw)}}}
	for _, m := range []GeospatialJSONMode{
		GeospatialModeHex, GeospatialModeBase64, GeospatialModeHybrid, GeospatialModeGeoJSON,
	} {
		cases = append(cases, struct {
			name string
			opts []ValueOption
		}{m.String(), []ValueOption{WithGeospatialConfig(NewGeospatialConfig(WithGeometryJSONMode(m)))}})
	}

	for _, c := range cases {
		b.Run("read/"+c.name, func(b *testing.B) {
			for b.Loop() {
				if _, err := ConvertValue(wkb, se, c.opts...); err != nil {
					b.Fatal(err)
				}
			}
		})
		if c.name == "raw" {
			continue
		}
		rendered, err := ConvertValue(wkb, se, c.opts...)
		require.NoError(b, err)
		text, err := json.Marshal(rendered)
		require.NoError(b, err)
		pT := parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		b.Run("write/"+c.name, func(b *testing.B) {
			for b.Loop() {
				if _, err := StrToParquetTypeWithLogical(string(text), pT, nil, lT, 0, 0, c.opts...); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestGeospatialOpaqueCollections(t *testing.T) {
	curve := wkbWithOrdinates(8, []uint32{3}, [][]float64{{0, 0}, {1, 1}, {2, 0}})
	collection := multiOf(WKBGeometryCollection, curve)
	for _, tc := range []struct {
		name string
		wkb  []byte
	}{
		{"collection", collection},
		{"nested", multiOf(WKBGeometryCollection, collection)},
	} {
		for _, mode := range []GeospatialJSONMode{GeospatialModeHex, GeospatialModeBase64} {
			t.Run(tc.name+"/"+mode.String(), func(t *testing.T) {
				for _, annotation := range []string{"GEOMETRY", "GEOGRAPHY"} {
					cfg := NewGeospatialConfig(WithGeometryJSONMode(mode), WithGeographyJSONMode(mode))
					fromJSON, fromCSV := geoWriteBack(t, annotation, tc.wkb, cfg)
					require.Equal(t, string(tc.wkb), fromJSON)
					require.Equal(t, string(tc.wkb), fromCSV)
				}
			})
		}
	}
}

// FuzzGeospatialWKBWriteBack checks mode agreement and byte preservation on arbitrary WKB.
func FuzzGeospatialWKBWriteBack(f *testing.F) {
	for _, b := range [][]byte{
		{},
		{1, 1},
		isoWKB(18),
		isoWKB(1, 1, 2), bigEndianPoint(), isoWKB(1001, 1, 2, 3),
		isoWKB(2001, 1, 2, 3), isoWKB(3001, 1, 2, 3, 4),
		polygonZ(), multiPointZ(),
		multiOf(WKBGeometryCollection, multiOf(WKBGeometryCollection, isoWKB(8))),
		wkbWithOrdinates(2, []uint32{2}, [][]float64{{0, 0}, {1, 1}}),
		append(isoWKB(2), 0xff, 0xff, 0xff, 0xff),
		// Dimensions the GeoJSON rendering treats differently: Z carried through, a
		// measure refused, and the empty shapes that declare a dimension they cannot show.
		wkbWithOrdinates(1002, []uint32{2}, [][]float64{{1, 2, 9}, {3, 4, 8}}),
		wkbWithOrdinates(1002, []uint32{0}, nil),
		multiOf(1007, wkbWithOrdinates(1002, []uint32{0}, nil)),
		membersOf(1007, isoWKB(1001, 1, 2, 3), isoWKB(1, 3, 4)),
		membersOf(1005, wkbWithOrdinates(1002, []uint32{0}, nil), wkbWithOrdinates(1002, []uint32{1}, [][]float64{{1, 2, 9}})),
		membersOf(1007, wkbWithOrdinates(1002, []uint32{0}, nil), isoWKB(1001, 1, 2, 3)),
	} {
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		se, lT := geoSE(t, "GEOMETRY")
		var hexAccepted bool
		for _, mode := range []GeospatialJSONMode{GeospatialModeHex, GeospatialModeBase64} {
			opt := WithGeospatialConfig(NewGeospatialConfig(WithGeometryJSONMode(mode)))
			rendered, err := ConvertValue(string(b), se, opt)
			require.NoError(t, err)
			native, nativeErr := JSONTypeToParquetTypeWithLogical(reflect.ValueOf(rendered), se.Type, nil, lT, 0, 0, opt)
			text, err := json.Marshal(rendered)
			require.NoError(t, err)
			fromText, textErr := StrToParquetTypeWithLogical(string(text), se.Type, nil, lT, 0, 0, opt)
			require.Equal(t, nativeErr == nil, textErr == nil)
			if mode == GeospatialModeHex {
				hexAccepted = nativeErr == nil
			} else {
				require.Equal(t, hexAccepted, nativeErr == nil)
			}
			if nativeErr == nil {
				require.Equal(t, string(b), native)
				require.Equal(t, native, fromText)
			}
		}

		// GeoJSON is the one mode that reads the geometry rather than carrying its bytes,
		// so a value it renders must write back to the bytes it was rendered from. A value
		// it cannot render keeps the substitute, which the byte modes above cover.
		geoOpt := WithGeospatialConfig(NewGeospatialConfig(
			WithGeometryJSONMode(GeospatialModeGeoJSON), WithGeospatialCoordinatePrecision(-1),
		))
		rendered, err := ConvertValue(string(b), se, geoOpt)
		if err != nil {
			return
		}
		if geo, ok := rendered.(map[string]any); !ok || geo["wkb_hex"] != nil {
			return
		}
		back, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf(rendered), se.Type, nil, lT, 0, 0, geoOpt)
		require.NoError(t, err, "a rendering GeoJSON accepted must write back")

		// The writer spells every value little-endian, so the bytes of a big-endian one
		// legitimately differ. What must survive is the type code, dimension included,
		// and the geometry it renders as.
		written, ok := back.(string)
		require.True(t, ok)
		writtenType, _, ok := readWKBHeader([]byte(written))
		require.True(t, ok)
		originalType, _, ok := readWKBHeader(b)
		require.True(t, ok)
		require.Equal(t, originalType, writtenType)

		// The rendering itself is not compared: a coordinate may be NaN, which is never
		// equal to itself. Writing it a second time must reach the same bytes, which says
		// the rendering carries everything the writer needs.
		reRendered, err := ConvertValue(written, se, geoOpt)
		require.NoError(t, err)
		rewritten, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf(reRendered), se.Type, nil, lT, 0, 0, geoOpt)
		require.NoError(t, err)
		require.Equal(t, written, rewritten)
	})
}

func TestCheckWKB_NestingDepth(t *testing.T) {
	for _, nested := range []func(int) []byte{nestedCollectionWKB, nestedEmptyCollectionWKB} {
		require.NoError(t, checkWKB(nested(maxGeometryDepth), "GEOMETRY"))
		require.Error(t, checkWKB(nested(maxGeometryDepth+1), "GEOMETRY"))
	}
}

func TestCheckWKB_MultiMemberChain(t *testing.T) {
	// A member's type is checked before it is measured, so a chain of Multi* members is
	// refused where it starts rather than recursing to its end at an unchanging depth.
	require.Error(t, checkWKB(nestedMultiPointWKB(100000), "GEOMETRY"))
}
