package schema

import (
	"bytes"
	"fmt"

	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// DecodeStatisticsMinMax decodes the plain-encoded MinValue and MaxValue bytes
// from a parquet Statistics object into typed Go values. The physical type is
// taken from se. Returns (nil, nil, nil) when statistics or schema element is
// absent, or when MinValue and MaxValue are both empty.
//
// The returned values follow the same type mapping as parquet-go's reader:
//
//	BOOLEAN               → bool
//	INT32                 → int32
//	INT64                 → int64
//	INT96                 → string (12-byte little-endian)
//	FLOAT                 → float32
//	DOUBLE                → float64
//	BYTE_ARRAY            → string
//	FIXED_LEN_BYTE_ARRAY  → string
func DecodeStatisticsMinMax(se *parquet.SchemaElement, stats *parquet.Statistics) (min, max any, err error) {
	if se == nil || se.Type == nil || stats == nil {
		return nil, nil, nil
	}
	if len(stats.MinValue) == 0 && len(stats.MaxValue) == 0 {
		return nil, nil, nil
	}

	pT := *se.Type
	decode := func(data []byte) (any, error) {
		if len(data) == 0 {
			return nil, nil
		}
		// BYTE_ARRAY statistics are stored without the 4-byte length prefix
		// that WritePlain normally prepends.
		if pT == parquet.Type_BYTE_ARRAY {
			return string(data), nil
		}
		// For FIXED_LEN_BYTE_ARRAY, bitWidth carries the element length.
		bitWidth := uint64(0)
		if pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			bitWidth = uint64(se.GetTypeLength())
		}
		vals, err := encoding.ReadPlain(bytes.NewReader(data), pT, 1, bitWidth)
		if err != nil {
			return nil, err
		}
		if len(vals) == 0 {
			return nil, nil
		}
		return vals[0], nil
	}

	min, err = decode(stats.MinValue)
	if err != nil {
		return nil, nil, fmt.Errorf("decode min value: %w", err)
	}
	max, err = decode(stats.MaxValue)
	if err != nil {
		return nil, nil, fmt.Errorf("decode max value: %w", err)
	}
	return min, max, nil
}
