package types

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// wrapScanErr returns a contextualized parse error or nil when err is nil.
func wrapScanErr(typeName, s string, err error) error {
	if err != nil {
		return fmt.Errorf("parse %s %q: %w", typeName, s, err)
	}
	return nil
}

// strToInterval scans the human-readable interval form, falling back to the legacy
// unsigned little-endian integer form.
func strToInterval(s string) (any, error) {
	res, err := ParseIntervalString(s)
	if err == nil {
		return res, nil
	}
	// Anything that is not a bare unsigned integer is a real parse failure: StrIntToBinary
	// used to swallow it, storing zeros for "garbage" and a partial value for input it
	// could scan a leading number out of.
	num, ok := new(big.Int).SetString(s, 10)
	if !ok || num.Sign() < 0 {
		return nil, wrapScanErr("INTERVAL", s, err)
	}
	if num.BitLen() > common.IntervalByteLen*8 {
		return nil, fmt.Errorf("INTERVAL %q exceeds %d bytes", s, common.IntervalByteLen)
	}
	return StrIntToBinary(s, "LittleEndian", common.IntervalByteLen, false), nil
}

// Scan a string to parquet value; length and scale just for decimal
func StrToParquetType(s string, pT *parquet.Type, cT *parquet.ConvertedType, length, scale int) (any, error) {
	if cT == nil {
		switch *pT {
		case parquet.Type_BOOLEAN:
			var v bool
			_, err := fmt.Sscanf(s, "%t", &v)
			return v, wrapScanErr("BOOLEAN", s, err)
		case parquet.Type_INT32:
			var v int32
			_, err := fmt.Sscanf(s, "%d", &v)
			return v, wrapScanErr("INT32", s, err)
		case parquet.Type_INT64:
			var v int64
			_, err := fmt.Sscanf(s, "%d", &v)
			return v, wrapScanErr("INT64", s, err)
		case parquet.Type_INT96:
			if res, err := ParseINT96String(s); err == nil {
				return res, nil
			}
			res := StrIntToBinary(s, "LittleEndian", 12, true)
			return res, nil
		case parquet.Type_FLOAT:
			var v float32
			_, err := fmt.Sscanf(s, "%f", &v)
			return v, wrapScanErr("FLOAT", s, err)
		case parquet.Type_DOUBLE:
			var v float64
			_, err := fmt.Sscanf(s, "%f", &v)
			return v, wrapScanErr("DOUBLE", s, err)
		case parquet.Type_BYTE_ARRAY:
			if decoded, err := base64.StdEncoding.DecodeString(s); err == nil {
				return string(decoded), nil
			}
			return s, nil
		case parquet.Type_FIXED_LEN_BYTE_ARRAY:
			if decoded, err := base64.StdEncoding.DecodeString(s); err == nil {
				return string(decoded), nil
			}
			return s, nil
		default:
			return nil, nil
		}
	}

	switch *cT {
	case parquet.ConvertedType_UTF8:
		return s, nil
	case parquet.ConvertedType_INT_8:
		var v int8
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), wrapScanErr("INT_8", s, err)
	case parquet.ConvertedType_INT_16:
		var v int16
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), wrapScanErr("INT_16", s, err)
	case parquet.ConvertedType_INT_32:
		var v int32
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), wrapScanErr("INT_32", s, err)
	case parquet.ConvertedType_UINT_8:
		var v uint8
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), wrapScanErr("UINT_8", s, err)
	case parquet.ConvertedType_UINT_16:
		var v uint16
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), wrapScanErr("UINT_16", s, err)
	case parquet.ConvertedType_UINT_32:
		var v uint32
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), wrapScanErr("UINT_32", s, err)
	case parquet.ConvertedType_DATE:
		if v, err := ParseDateString(s); err == nil {
			return v, nil
		}
		var v int32
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), wrapScanErr("DATE", s, err)
	case parquet.ConvertedType_TIME_MILLIS:
		if nanos, err := ParseTimeString(s); err == nil {
			return int32(nanos / int64(time.Millisecond)), nil
		}
		var v int32
		_, err := fmt.Sscanf(s, "%d", &v)
		return int32(v), wrapScanErr("TIME_MILLIS", s, err)
	case parquet.ConvertedType_UINT_64:
		var vt uint64
		_, err := fmt.Sscanf(s, "%d", &vt)
		return int64(vt), wrapScanErr("UINT_64", s, err)
	case parquet.ConvertedType_INT_64:
		var v int64
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, wrapScanErr("INT_64", s, err)
	case parquet.ConvertedType_TIME_MICROS:
		if nanos, err := ParseTimeString(s); err == nil {
			return nanos / int64(time.Microsecond), nil
		}
		var v int64
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, wrapScanErr("TIME_MICROS", s, err)
	case parquet.ConvertedType_TIMESTAMP_MILLIS:
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UnixNano() / int64(time.Millisecond), nil
		}
		var v int64
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, wrapScanErr("TIMESTAMP_MILLIS", s, err)
	case parquet.ConvertedType_TIMESTAMP_MICROS:
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UnixNano() / int64(time.Microsecond), nil
		}
		var v int64
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, wrapScanErr("TIMESTAMP_MICROS", s, err)
	case parquet.ConvertedType_INTERVAL:
		return strToInterval(s)
	case parquet.ConvertedType_DECIMAL:
		// A ConvertedType DECIMAL column carries its precision in the schema element
		// rather than here, so the digit count goes unchecked on this path.
		return strToDecimal(s, pT, 0, length, scale)
	case parquet.ConvertedType_BSON, parquet.ConvertedType_JSON, parquet.ConvertedType_ENUM:
		// These are BYTE_ARRAY types that should preserve the string value as-is
		return s, nil
	default:
		return nil, nil
	}
}

func strToLogicalType(s string, lT *parquet.LogicalType, pT *parquet.Type, length int) (any, bool, error) {
	if lT.IsSetFLOAT16() {
		if length != common.Float16ByteLen {
			return s, true, fmt.Errorf("FLOAT16 requires length %d, got %d", common.Float16ByteLen, length)
		}
		v, err := ParseFloat16String(s)
		if err != nil {
			return v, true, fmt.Errorf("parse FLOAT16 %q: %w", s, err)
		}
		return v, true, nil
	}
	if lT.IsSetUUID() {
		if length != common.UUIDByteLen {
			return s, true, fmt.Errorf("UUID requires length %d, got %d", common.UUIDByteLen, length)
		}
		// uuid.Parse skips the wrapping characters of the {...} form, so "[...]" and any
		// other 38-byte wrapper would pass; uuid.Validate checks them, and it applies
		// every delimiter and hex check Parse does, so Parse cannot fail after it.
		if err := uuid.Validate(s); err != nil {
			return s, true, fmt.Errorf("parse UUID %q: %w", s, err)
		}
		u, _ := uuid.Parse(s)
		return string(u[:]), true, nil
	}
	if lT.IsSetTIMESTAMP() {
		v, err := strToTimestampLogical(s, lT.GetTIMESTAMP())
		return v, err == nil, err
	}
	if lT.IsSetTIME() {
		v, err := strToTimeLogical(s, lT.GetTIME())
		return v, err == nil, err
	}
	if lT.IsSetDATE() {
		if v, err := ParseDateString(s); err == nil {
			return v, true, nil
		}
		var v int32
		if _, err := fmt.Sscanf(s, "%d", &v); err != nil {
			return int32(v), true, fmt.Errorf("parse DATE %q: %w", s, err)
		}
		return int32(v), true, nil
	}
	if lT.IsSetDECIMAL() {
		dec := lT.GetDECIMAL()
		v, err := strToDecimal(s, pT, int(dec.GetPrecision()), length, int(dec.GetScale()))
		return v, true, err
	}
	return nil, false, nil
}

// StrToParquetTypeWithLogical scans a string to a parquet value, honoring the logical type.
// UUID requires length 16 and a textual form uuid.Parse accepts (dashed, undashed hex,
// braced, or urn:uuid: prefixed); any other length or string, raw binary included, errors.
// FLOAT16 likewise requires length 2; releases up to v3.8.2 ignored both lengths.
func StrToParquetTypeWithLogical(s string, pT *parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, length, scale int) (any, error) {
	if lT != nil {
		if v, handled, err := strToLogicalType(s, lT, pT, length); handled {
			return v, err
		}
	}

	return StrToParquetType(s, pT, cT, length, scale)
}
