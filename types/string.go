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

// strToINT96 scans the timestamp form, falling back to the legacy signed integer form.
func strToINT96(s string) (string, error) {
	res, err := ParseINT96String(s)
	if err == nil {
		return res, nil
	}
	// Anything that is not a bare integer is a real parse failure. The fallback used to
	// swallow it, so a timestamp the parser rejected was stored as whatever leading digits
	// it happened to start with: "5874898-06-04T00:00:00Z" became the number 5874898.
	if _, ok := new(big.Int).SetString(s, 10); !ok {
		return "", err
	}
	return StrIntToBinary(s, "LittleEndian", int96ByteLength, true), nil
}

// strToFixedLenByteArray reads s as the bytes of a FIXED_LEN_BYTE_ARRAY column.
func strToFixedLenByteArray(s string, length int) (string, error) {
	// A string that decodes as base64 can still be meant literally, so the column
	// width picks the reading rather than the encoding.
	decoded, decodeErr := base64.StdEncoding.DecodeString(s)
	switch {
	case length <= 0:
		// No width to match, as when a caller passes 0 for a column it did not look
		// up: keep the historical base64-first reading.
		if decodeErr == nil {
			return string(decoded), nil
		}
		return s, nil
	case decodeErr == nil && len(decoded) == length:
		return string(decoded), nil
	case len(s) == length:
		return s, nil
	case decodeErr == nil && len(decoded) != len(s):
		// Reported here so the message names the string the caller supplied.
		return "", fmt.Errorf("FIXED_LEN_BYTE_ARRAY %q is %d bytes raw and %d base64-decoded, neither matches column length %d", s, len(s), len(decoded), length)
	default:
		return "", fmt.Errorf("FIXED_LEN_BYTE_ARRAY %q is %d bytes, column length is %d", s, len(s), length)
	}
}

// StrToParquetType scans a string to a parquet value; length and scale are only used by
// DECIMAL. A nil physical type is reported, since every branch below needs one.
func StrToParquetType(s string, pT *parquet.Type, cT *parquet.ConvertedType, length, scale int) (any, error) {
	if pT == nil {
		return nil, errNoPhysicalType(s)
	}
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
			return strToINT96(s)
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
			return strToFixedLenByteArray(s, length)
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
		v, err := parseTimeOfDay(s, "TIME_MILLIS", time.Millisecond)
		return int32(v), err
	case parquet.ConvertedType_UINT_64:
		var vt uint64
		_, err := fmt.Sscanf(s, "%d", &vt)
		return int64(vt), wrapScanErr("UINT_64", s, err)
	case parquet.ConvertedType_INT_64:
		var v int64
		_, err := fmt.Sscanf(s, "%d", &v)
		return v, wrapScanErr("INT_64", s, err)
	case parquet.ConvertedType_TIME_MICROS:
		return parseTimeOfDay(s, "TIME_MICROS", time.Microsecond)
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
		// Claim the value even when it fails: falling through to the physical INT32/INT64
		// scan would take "25:00:00.000" as 25 rather than report the out-of-range TIME.
		v, err := strToTimeLogical(s, lT.GetTIME())
		return v, true, err
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
	if lT.IsSetINTEGER() {
		return strToIntegerLogical(s, lT.GetINTEGER(), pT)
	}
	return nil, false, nil
}

// StrToParquetTypeWithLogical scans a string to a parquet value, honoring the logical type.
// UUID requires length 16 and a textual form uuid.Parse accepts (dashed, undashed hex,
// braced, or urn:uuid: prefixed); any other length or string, raw binary included, errors.
// FLOAT16 likewise requires length 2; releases up to v3.8.2 ignored both lengths.
// An INTEGER annotation is scanned at its declared width and must spell a whole number,
// surrounding whitespace aside. Releases up to v3.8.3 scanned INT_*/UINT_* columns with
// fmt.Sscanf, which stopped at the first character it could not use and so read "42abc"
// as 42; schema builders backfill INTEGER for those converted types, so the stricter scan
// applies to them as well.
func StrToParquetTypeWithLogical(s string, pT *parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, length, scale int) (any, error) {
	if pT == nil {
		return nil, errNoPhysicalType(s)
	}

	if lT != nil {
		if v, handled, err := strToLogicalType(s, lT, pT, length); handled {
			return v, err
		}
	}

	return StrToParquetType(s, pT, cT, length, scale)
}
