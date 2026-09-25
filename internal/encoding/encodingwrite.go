package encoding

import (
	"encoding/binary"
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func WriteRLE(vals []any, bitWidth int32, pt parquet.Type) ([]byte, error) {
	intVals := make([]int64, len(vals))
	for i, val := range vals {
		switch pt {
		case parquet.Type_BOOLEAN:
			boolVal, ok := val.(bool)
			if !ok {
				return nil, fmt.Errorf("WriteRLE: value %d has type %T, expected bool", i, val)
			}
			if boolVal {
				intVals[i] = 1
			}
		case parquet.Type_INT32:
			intVal, ok := val.(int32)
			if !ok {
				return nil, fmt.Errorf("WriteRLE: value %d has type %T, expected int32", i, val)
			}
			intVals[i] = int64(intVal)
		case parquet.Type_INT64:
			intVal, ok := val.(int64)
			if !ok {
				return nil, fmt.Errorf("WriteRLE: value %d has type %T, expected int64", i, val)
			}
			intVals[i] = intVal
		default:
			return nil, fmt.Errorf("WriteRLE: unsupported parquet type %v", pt)
		}
	}
	return writeRLEInt64(intVals, bitWidth), nil
}

func WriteRLEBitPackedHybrid(vals []any, bitWidths int32, pt parquet.Type) ([]byte, error) {
	rleBuf, err := WriteRLE(vals, bitWidths, pt)
	if err != nil {
		return nil, err
	}
	res := make([]byte, 0)
	lenBuf, err := WritePlain([]any{int32(len(rleBuf))}, parquet.Type_INT32)
	if err != nil {
		return nil, err
	}
	res = append(res, lenBuf...)
	res = append(res, rleBuf...)
	return res, nil
}

func WriteRLEInt32(vals []int32, bitWidth int32) []byte {
	intVals := make([]int64, len(vals))
	for i := range vals {
		intVals[i] = int64(vals[i])
	}
	return writeRLEInt64(intVals, bitWidth)
}

func WriteRLEBitPackedHybridInt32(vals []int32, bitWidths int32) ([]byte, error) {
	rleBuf := WriteRLEInt32(vals, bitWidths)
	res := make([]byte, 0)
	lenBuf, err := WritePlain([]any{int32(len(rleBuf))}, parquet.Type_INT32)
	if err != nil {
		return nil, err
	}
	res = append(res, lenBuf...)
	res = append(res, rleBuf...)
	return res, nil
}

func WriteBitPacked(vals []any, bitWidth int64, ifHeader bool) []byte {
	ln := len(vals)
	if ln <= 0 {
		return nil
	}
	return writeBitPackedInt64(ToInt64(vals), bitWidth, ifHeader)
}

const (
	rleRunThreshold = 8
	bitPackedGroup  = 8
)

func writeRLEInt64(vals []int64, bitWidth int32) []byte {
	res := make([]byte, 0)
	literalStart := 0

	for i := 0; i < len(vals); {
		runEnd := i + 1
		for runEnd < len(vals) && vals[runEnd] == vals[i] {
			runEnd++
		}

		runStart := i
		runLength := runEnd - runStart
		if runLength >= rleRunThreshold {
			literalLength := runStart - literalStart
			alignment := (bitPackedGroup - literalLength%bitPackedGroup) % bitPackedGroup
			if runLength-alignment >= rleRunThreshold {
				if alignment > 0 {
					runStart += alignment
					runLength -= alignment
				}
				res = appendBitPackedRun(res, vals[literalStart:runStart], bitWidth)
				res = appendRLERun(res, vals[runStart], runLength, bitWidth)
				literalStart = runEnd
			}
		}
		i = runEnd
	}

	return appendBitPackedRun(res, vals[literalStart:], bitWidth)
}

func appendBitPackedRun(dst []byte, vals []int64, bitWidth int32) []byte {
	if len(vals) == 0 {
		return dst
	}

	paddedLength := (len(vals) + bitPackedGroup - 1) / bitPackedGroup * bitPackedGroup
	padded := make([]int64, paddedLength)
	copy(padded, vals)
	return append(dst, writeBitPackedInt64(padded, int64(bitWidth), true)...)
}

func appendRLERun(dst []byte, val int64, runLength int, bitWidth int32) []byte {
	dst = append(dst, WriteUnsignedVarInt(uint64(runLength<<1))...)

	var valBuf [8]byte
	binary.LittleEndian.PutUint64(valBuf[:], uint64(val))
	byteCount := (bitWidth + 7) / 8
	return append(dst, valBuf[:byteCount]...)
}

func writeBitPackedInt64(vals []int64, bitWidth int64, withHeader bool) []byte {
	res := make([]byte, 0, (len(vals)*int(bitWidth)+7)/8+1)
	if withHeader {
		header := (len(vals)/bitPackedGroup)<<1 | 1
		res = append(res, WriteUnsignedVarInt(uint64(header))...)
	}
	if bitWidth == 0 {
		return res
	}

	packed := make([]byte, (len(vals)*int(bitWidth)+7)/8)
	for i, val := range vals {
		value := uint64(val)
		bitOffset := i * int(bitWidth)
		for valueBits := int(bitWidth); valueBits > 0; {
			byteOffset := bitOffset / 8
			offsetInByte := bitOffset % 8
			bitsToWrite := min(8-offsetInByte, valueBits)
			mask := uint64(1<<bitsToWrite) - 1
			valueOffset := int(bitWidth) - valueBits
			packed[byteOffset] |= byte((value>>valueOffset)&mask) << offsetInByte
			bitOffset += bitsToWrite
			valueBits -= bitsToWrite
		}
	}
	return append(res, packed...)
}
