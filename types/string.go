package types

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// maxScanErrInput caps how much of a rejected value an error quotes; a float in plain
// decimal can otherwise put 300 digits in the message.
const maxScanErrInput = 64

// strToDayCount scans the bare day count a DATE column also accepts, over the whole field.
func strToDayCount(s, typeName string) (any, error) {
	v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 32)
	return int32(v), wrapScanErr(typeName, s, err)
}

// strToTickCount scans the bare tick count a TIMESTAMP column also accepts.
func strToTickCount(s, typeName string) (any, error) {
	v, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return v, wrapScanErr(typeName, s, err)
}

// wrapScanErr returns a contextualized parse error or nil when err is nil.
func wrapScanErr(typeName, s string, err error) error {
	if err == nil {
		return nil
	}
	if len(s) > maxScanErrInput {
		// strconv repeats the whole input; keep its sentinel, drop the repeat.
		var numErr *strconv.NumError
		if errors.As(err, &numErr) {
			err = numErr.Err
		}
		return fmt.Errorf("parse %s %q...(%d bytes): %w", typeName, s[:maxScanErrInput], len(s), err)
	}
	return fmt.Errorf("parse %s %q: %w", typeName, s, err)
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

// StrToParquetType scans a string to a parquet value; length and scale are only used by
// DECIMAL. A nil physical type is reported, since every branch below needs one.
func StrToParquetType(s string, pT *parquet.Type, cT *parquet.ConvertedType, length, scale int) (any, error) {
	if pT == nil {
		return nil, errNoPhysicalType(s)
	}
	if cT == nil {
		// INT96 is the one physical type with a text form of its own.
		if *pT == parquet.Type_INT96 {
			return strToINT96(s)
		}
		return physicalStrToParquetType(s, *pT, length)
	}

	// Scanned at the width and signedness it declares, like the INTEGER type it stands for.
	if it := convertedIntegerType(*cT); it != nil {
		if v, handled, err := strToIntegerLogical(s, it, pT); handled {
			return v, err
		}
		// The annotation does not describe this column's range; the physical scan decides.
		return physicalStrToParquetType(s, *pT, length)
	}

	switch *cT {
	case parquet.ConvertedType_UTF8:
		return s, nil
	case parquet.ConvertedType_DATE:
		if v, err := ParseDateString(s); err == nil {
			return v, nil
		}
		return strToDayCount(s, "DATE")
	case parquet.ConvertedType_TIME_MILLIS:
		v, err := parseTimeOfDay(s, "TIME_MILLIS", time.Millisecond)
		return int32(v), err
	case parquet.ConvertedType_TIME_MICROS:
		return parseTimeOfDay(s, "TIME_MICROS", time.Microsecond)
	case parquet.ConvertedType_TIMESTAMP_MILLIS:
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UnixMilli(), nil
		}
		return strToTickCount(s, "TIMESTAMP_MILLIS")
	case parquet.ConvertedType_TIMESTAMP_MICROS:
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UnixMicro(), nil
		}
		return strToTickCount(s, "TIMESTAMP_MICROS")
	case parquet.ConvertedType_INTERVAL:
		return strToInterval(s)
	case parquet.ConvertedType_DECIMAL:
		// A ConvertedType DECIMAL column carries its precision in the schema element
		// rather than here, so the digit count goes unchecked on this path.
		return strToDecimal(s, pT, 0, length, scale)
	case parquet.ConvertedType_BSON:
		// Not text on the wire, unlike the two below: the string is Extended JSON and
		// the column holds the document it describes.
		return strToBSON(s, pT)
	case parquet.ConvertedType_JSON, parquet.ConvertedType_ENUM:
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
		ts := lT.GetTIMESTAMP()
		if !hasTimestampUnit(ts) {
			// No unit means the annotation says nothing; the column's scan decides.
			return nil, false, nil
		}
		// Otherwise claim it even on failure, as TIME does: the bare tick count is read
		// here too, so falling through would report a bad value as a failed INT64.
		v, err := strToTimestampLogical(s, ts)
		return v, true, err
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
		v, err := strToDayCount(s, "DATE")
		return v, true, err
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
// The value mode picks the grammar: interpreted (the default) reads each logical type's
// canonical text, raw reads the physical value as base64 where byte-backed. Interpreted
// mode requires length 16 for UUID and 2 for FLOAT16, and refuses GEOMETRY and GEOGRAPHY,
// which have no write form yet. See README's Value Modes for the full grammar.
func StrToParquetTypeWithLogical(s string, pT *parquet.Type, cT *parquet.ConvertedType, lT *parquet.LogicalType, length, scale int, opts ...ValueOption) (any, error) {
	if pT == nil {
		return nil, errNoPhysicalType(s)
	}

	mode := resolveValueConfig(opts).Mode
	if !mode.IsValid() {
		return nil, fmt.Errorf("%w %d", ErrUnsupportedValueMode, int(mode))
	}
	if mode == ValueModeRaw {
		return rawStrToParquetType(s, pT, cT, lT, length)
	}

	// Text in both modes. StrToParquetType keeps the converted spelling verbatim too, but
	// a schema may carry only the logical one, and the physical scan reads text as base64.
	if isTextAnnotated(cT, lT) {
		return s, nil
	}
	if typeName := interpretedWriteUnsupported(cT, lT); typeName != "" {
		return nil, errInterpretedWrite(typeName)
	}
	// Ahead of both scanners below: either annotation spells the same Extended JSON.
	if isBSONAnnotated(cT, lT) {
		return strToBSON(s, pT)
	}
	if lT != nil {
		if v, handled, err := strToLogicalType(s, lT, pT, length); handled {
			return v, err
		}
	}

	return StrToParquetType(s, pT, cT, length, scale)
}
