package types

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// jsonFuzzCase builds one (value, schema element) pair from the fuzz input. The selector picks a
// column type; the raw bytes and the integer supply a value of the shape that column stores. The
// last result reports whether the converted value is required to be JSON encodable: BSON and
// GeoJSON decode into nested structures, and nonFiniteFloatToJSONString only rewrites a top-level
// float, so a NaN inside a BSON document or a GeoJSON coordinate still reaches encoding/json.
func jsonFuzzCase(sel uint8, b []byte, n int64) (any, *parquet.SchemaElement, bool) {
	se := parquet.NewSchemaElement()
	// unsigned, so the value never drives a negative modulus into a size or bit width
	u := uint64(n)
	switch sel % 16 {
	case 0:
		se.Type = parquet.TypePtr(parquet.Type_FLOAT)
		return math.Float32frombits(uint32(n)), se, true
	case 1:
		se.Type = parquet.TypePtr(parquet.Type_DOUBLE)
		return math.Float64frombits(uint64(n)), se, true
	case 2:
		se.Type = parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
		se.LogicalType = parquet.NewLogicalType()
		se.LogicalType.FLOAT16 = parquet.NewFloat16Type()
		return string(b), se, true
	case 3:
		se.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		se.LogicalType = createDecimalLogicalType(int32(u%40), int32(u%10))
		return string(b), se, true
	case 4:
		se.Type = parquet.TypePtr(parquet.Type_INT32)
		se.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)
		precision, scale := int32(u%18), int32(u%6)
		se.Precision, se.Scale = &precision, &scale
		return int32(n), se, true
	case 5:
		se.Type = parquet.TypePtr(parquet.Type_INT96)
		return string(b), se, true
	case 6:
		se.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		return string(b), se, true
	case 7:
		se.Type = parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
		se.LogicalType = parquet.NewLogicalType()
		se.LogicalType.UUID = parquet.NewUUIDType()
		return string(b), se, true
	case 8:
		se.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		se.LogicalType = createGeometryLogicalType("OGC:CRS84")
		return string(b), se, true
	case 9:
		se.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		se.LogicalType = createGeographyLogicalType("OGC:CRS84", parquet.EdgeInterpolationAlgorithm_SPHERICAL)
		return string(b), se, false
	case 10:
		se.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
		se.LogicalType = parquet.NewLogicalType()
		se.LogicalType.BSON = parquet.NewBsonType()
		return string(b), se, false
	case 11:
		se.Type = parquet.TypePtr(parquet.Type_INT64)
		se.LogicalType = createTimestampLogicalType(u%3 == 0, u%3 == 1, u%3 == 2, u%2 == 0)
		return n, se, true
	case 12:
		se.Type = parquet.TypePtr(parquet.Type_INT64)
		se.LogicalType = createTimeLogicalType(u%3 == 0, u%3 == 1, u%3 == 2)
		return n, se, true
	case 13:
		se.Type = parquet.TypePtr(parquet.Type_INT32)
		se.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_DATE)
		return int32(n), se, true
	case 14:
		se.Type = parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
		se.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INTERVAL)
		return string(b), se, true
	default:
		se.Type = parquet.TypePtr(parquet.Type_INT32)
		se.LogicalType = createIntegerLogicalType(int8(1<<(u%5)), u%2 == 0)
		return int32(n), se, true
	}
}

// FuzzConvertToJSONType checks the contract every caller relies on: whatever a column holds, the
// converted value can be handed to encoding/json. Column types that decode into a nested structure
// are still converted, to keep them covered against panics, but are exempt from that assertion.
func FuzzConvertToJSONType(f *testing.F) {
	f.Add(uint8(0), []byte{}, int64(math.Float32bits(float32(math.NaN()))))
	f.Add(uint8(0), []byte{}, int64(math.Float32bits(float32(math.Inf(-1)))))
	f.Add(uint8(1), []byte{}, int64(math.Float64bits(math.NaN())))
	f.Add(uint8(1), []byte{}, int64(math.Float64bits(math.Inf(1))))
	f.Add(uint8(2), []byte{0x01, 0x7C}, int64(0)) // FLOAT16 NaN
	f.Add(uint8(2), []byte{0x00, 0xFC}, int64(0)) // FLOAT16 -Inf
	f.Add(uint8(3), []byte{0x00, 0x01}, int64(5))
	f.Add(uint8(8), []byte{}, int64(0))
	f.Add(uint8(11), []byte{}, int64(1))

	f.Fuzz(func(t *testing.T, sel uint8, b []byte, n int64) {
		val, se, encodable := jsonFuzzCase(sel, b, n)
		converted := ConvertToJSONType(val, se)
		if !encodable {
			return
		}
		_, err := json.Marshal(converted)
		require.NoError(t, err, "converted %T is not JSON encodable", converted)
	})
}
