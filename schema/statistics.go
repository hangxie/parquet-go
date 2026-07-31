package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// DecodeStatisticsMinMax decodes the plain-encoded MinValue and MaxValue bytes
// from a parquet Statistics object into typed Go values. The physical type is
// taken from se. Returns (nil, nil, nil) when statistics or schema element is
// absent, when MinValue and MaxValue are both absent, or for each bound that is
// not a valid physical or logical value of the column.
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
	if stats.MinValue == nil && stats.MaxValue == nil {
		return nil, nil, nil
	}

	pT := *se.Type
	decode := func(data []byte) (any, error) {
		if data == nil {
			return nil, nil
		}
		if !validStatisticsValue(se, data) {
			return nil, nil
		}
		// BYTE_ARRAY statistics are stored without the 4-byte length prefix
		// that WritePlain normally prepends.
		if pT == parquet.Type_BYTE_ARRAY || pT == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			return string(data), nil
		}
		vals, err := encoding.ReadPlain(bytes.NewReader(data), pT, 1, 0)
		if err != nil {
			return nil, fmt.Errorf("decode statistic value: %w", err)
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

func validStatisticsValue(se *parquet.SchemaElement, data []byte) bool {
	if !validPhysicalStatisticsValue(se, data) {
		return false
	}
	if se.LogicalType != nil && !validLogicalStatisticsValue(se.LogicalType, data) {
		return false
	}
	return se.ConvertedType == nil || validConvertedStatisticsValue(*se.ConvertedType, data)
}

func validPhysicalStatisticsValue(se *parquet.SchemaElement, data []byte) bool {
	switch se.GetType() {
	case parquet.Type_BOOLEAN:
		return len(data) == 1
	case parquet.Type_INT32, parquet.Type_FLOAT:
		return len(data) == 4
	case parquet.Type_INT64, parquet.Type_DOUBLE:
		return len(data) == 8
	case parquet.Type_INT96:
		return len(data) == 12
	case parquet.Type_BYTE_ARRAY:
		return true
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		length := int(se.GetTypeLength())
		if length <= 0 || len(data) > length {
			return false
		}
		// Only unannotated fixed-width byte arrays may use shortened raw bounds.
		if (se.LogicalType != nil || se.ConvertedType != nil) && len(data) != length {
			return false
		}
		return true
	default:
		return false
	}
}

func validLogicalStatisticsValue(logicalType *parquet.LogicalType, data []byte) bool {
	if logicalType.CountSetFieldsLogicalType() != 1 {
		return false
	}
	switch {
	case logicalType.IsSetSTRING(), logicalType.IsSetENUM():
		return utf8.Valid(data)
	case logicalType.IsSetJSON():
		return json.Valid(data)
	case logicalType.IsSetBSON():
		return bson.Raw(data).Validate() == nil
	case logicalType.IsSetUUID():
		return len(data) == 16
	case logicalType.IsSetFLOAT16():
		return len(data) == 2
	case logicalType.IsSetDECIMAL():
		return len(data) > 0
	case logicalType.IsSetINTEGER(), logicalType.IsSetDATE(), logicalType.IsSetTIME(), logicalType.IsSetTIMESTAMP():
		// Physical validation is sufficient for these scalar annotations.
		return true
	default:
		return false
	}
}

func validConvertedStatisticsValue(convertedType parquet.ConvertedType, data []byte) bool {
	switch convertedType {
	case parquet.ConvertedType_UTF8, parquet.ConvertedType_ENUM:
		return utf8.Valid(data)
	case parquet.ConvertedType_JSON:
		return json.Valid(data)
	case parquet.ConvertedType_BSON:
		return bson.Raw(data).Validate() == nil
	case parquet.ConvertedType_DECIMAL:
		return len(data) > 0
	case parquet.ConvertedType_DATE,
		parquet.ConvertedType_TIME_MILLIS, parquet.ConvertedType_TIME_MICROS,
		parquet.ConvertedType_TIMESTAMP_MILLIS, parquet.ConvertedType_TIMESTAMP_MICROS,
		parquet.ConvertedType_UINT_8, parquet.ConvertedType_UINT_16,
		parquet.ConvertedType_UINT_32, parquet.ConvertedType_UINT_64,
		parquet.ConvertedType_INT_8, parquet.ConvertedType_INT_16,
		parquet.ConvertedType_INT_32, parquet.ConvertedType_INT_64:
		// Physical validation is sufficient for these scalar annotations.
		return true
	default:
		return false
	}
}
