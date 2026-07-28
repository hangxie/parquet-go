package encoding

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"reflect"

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

func WriteDelta(nums []any) ([]byte, error) {
	ln := len(nums)
	if ln <= 0 {
		// If empty, we default to treating it as INT32 for the sake of writing an empty header.
		// The type doesn't matter much for an empty block as long as the header is valid.
		return WriteDeltaINT32(nums), nil
	}

	if _, ok := nums[0].(int32); ok {
		return WriteDeltaINT32(nums), nil
	} else if _, ok := nums[0].(int64); ok {
		return WriteDeltaINT64(nums), nil
	} else {
		return nil, fmt.Errorf("WriteDelta: unsupported type %T, expected int32 or int64", nums[0])
	}
}

func WriteDeltaINT32(nums []any) []byte {
	totalNumValues := uint64(len(nums))
	if totalNumValues == 0 {
		var blockSize uint64 = 128
		var numMiniBlocksInBlock uint64 = 4
		res := make([]byte, 0)
		res = append(res, WriteUnsignedVarInt(blockSize)...)
		res = append(res, WriteUnsignedVarInt(numMiniBlocksInBlock)...)
		res = append(res, WriteUnsignedVarInt(totalNumValues)...)
		res = append(res, WriteUnsignedVarInt(0)...) // firstValue
		return res
	}
	res := make([]byte, 0)
	var blockSize uint64 = 128
	var numMiniBlocksInBlock uint64 = 4
	var numValuesInMiniBlock uint64 = 32

	num := nums[0].(int32)
	firstValue := uint64((num >> 31) ^ (num << 1))

	res = append(res, WriteUnsignedVarInt(blockSize)...)
	res = append(res, WriteUnsignedVarInt(numMiniBlocksInBlock)...)
	res = append(res, WriteUnsignedVarInt(totalNumValues)...)
	res = append(res, WriteUnsignedVarInt(firstValue)...)

	i := 1
	for i < len(nums) {
		blockBuf := make([]any, 0)
		var minDelta int32 = 0x7FFFFFFF

		for i < len(nums) && uint64(len(blockBuf)) < blockSize {
			delta := nums[i].(int32) - nums[i-1].(int32)
			blockBuf = append(blockBuf, delta)
			if delta < minDelta {
				minDelta = delta
			}
			i++
		}

		for uint64(len(blockBuf)) < blockSize {
			blockBuf = append(blockBuf, minDelta)
		}

		bitWidths := make([]byte, numMiniBlocksInBlock)

		for j := range numMiniBlocksInBlock {
			var maxValue int32 = 0
			for k := uint64(j) * numValuesInMiniBlock; k < uint64(j+1)*numValuesInMiniBlock; k++ {
				blockBuf[k] = blockBuf[k].(int32) - minDelta
				if blockBuf[k].(int32) > maxValue {
					maxValue = blockBuf[k].(int32)
				}
			}
			bitWidths[j] = byte(bits.Len32(uint32(maxValue)))
		}

		minDeltaZigZag := uint64((minDelta >> 31) ^ (minDelta << 1))
		res = append(res, WriteUnsignedVarInt(minDeltaZigZag)...)
		res = append(res, bitWidths...)

		for j := range numMiniBlocksInBlock {
			res = append(res, WriteBitPacked(blockBuf[uint64(j)*numValuesInMiniBlock:uint64(j+1)*numValuesInMiniBlock], int64(bitWidths[j]), false)...)
		}

	}
	return res
}

func WriteDeltaINT64(nums []any) []byte {
	totalNumValues := uint64(len(nums))
	if totalNumValues == 0 {
		var blockSize uint64 = 128
		var numMiniBlocksInBlock uint64 = 4
		res := make([]byte, 0)
		res = append(res, WriteUnsignedVarInt(blockSize)...)
		res = append(res, WriteUnsignedVarInt(numMiniBlocksInBlock)...)
		res = append(res, WriteUnsignedVarInt(totalNumValues)...)
		res = append(res, WriteUnsignedVarInt(0)...) // firstValue
		return res
	}
	res := make([]byte, 0)
	var blockSize uint64 = 128
	var numMiniBlocksInBlock uint64 = 4
	var numValuesInMiniBlock uint64 = 32

	num := nums[0].(int64)
	firstValue := uint64((num >> 63) ^ (num << 1))

	res = append(res, WriteUnsignedVarInt(blockSize)...)
	res = append(res, WriteUnsignedVarInt(numMiniBlocksInBlock)...)
	res = append(res, WriteUnsignedVarInt(totalNumValues)...)
	res = append(res, WriteUnsignedVarInt(firstValue)...)

	i := 1
	for i < len(nums) {
		blockBuf := make([]any, 0)
		var minDelta int64 = 0x7FFFFFFFFFFFFFFF

		for i < len(nums) && uint64(len(blockBuf)) < blockSize {
			delta := nums[i].(int64) - nums[i-1].(int64)
			blockBuf = append(blockBuf, delta)
			if delta < minDelta {
				minDelta = delta
			}
			i++
		}

		for uint64(len(blockBuf)) < blockSize {
			blockBuf = append(blockBuf, minDelta)
		}

		bitWidths := make([]byte, numMiniBlocksInBlock)

		for j := range numMiniBlocksInBlock {
			var maxValue int64 = 0
			for k := uint64(j) * numValuesInMiniBlock; k < uint64(j+1)*numValuesInMiniBlock; k++ {
				blockBuf[k] = blockBuf[k].(int64) - minDelta
				if blockBuf[k].(int64) > maxValue {
					maxValue = blockBuf[k].(int64)
				}
			}
			bitWidths[j] = byte(bits.Len64(uint64(maxValue)))
		}

		minDeltaZigZag := uint64((minDelta >> 63) ^ (minDelta << 1))
		res = append(res, WriteUnsignedVarInt(minDeltaZigZag)...)
		res = append(res, bitWidths...)

		for j := range numMiniBlocksInBlock {
			res = append(res, WriteBitPacked(blockBuf[uint64(j)*numValuesInMiniBlock:uint64(j+1)*numValuesInMiniBlock], int64(bitWidths[j]), false)...)
		}

	}
	return res
}

func WriteDeltaLengthByteArray(arrays []any) []byte {
	ln := len(arrays)
	if ln <= 0 {
		return WriteDeltaINT32([]any{})
	}
	lengthArray := make([]any, ln)
	for i := range ln {
		array := reflect.ValueOf(arrays[i]).String()
		lengthArray[i] = int32(len(array))
	}

	res := WriteDeltaINT32(lengthArray)

	for i := range ln {
		array := reflect.ValueOf(arrays[i]).String()
		res = append(res, array...)
	}
	return res
}

func WriteDeltaByteArray(arrays []any) []byte {
	ln := len(arrays)
	if ln <= 0 {
		// Prepare empty inputs for prefix lengths and suffixes to generate valid headers
		prefixBuf := WriteDeltaINT32([]any{})
		suffixBuf := WriteDeltaLengthByteArray([]any{})
		res := make([]byte, 0)
		res = append(res, prefixBuf...)
		res = append(res, suffixBuf...)
		return res
	}

	prefixLengths := make([]any, ln)
	suffixes := make([]any, ln)
	prefixLengths[0] = int32(0)
	suffixes[0] = arrays[0]

	for i := 1; i < ln; i++ {
		s1 := reflect.ValueOf(arrays[i-1]).String()
		s2 := reflect.ValueOf(arrays[i]).String()
		l1 := len(s1)
		l2 := len(s2)
		j := 0
		for j < l1 && j < l2 {
			if s1[j] != s2[j] {
				break
			}
			j++
		}
		prefixLengths[i] = int32(j)
		suffixes[i] = s2[j:]
	}

	prefixBuf := WriteDeltaINT32(prefixLengths)
	suffixBuf := WriteDeltaLengthByteArray(suffixes)

	res := make([]byte, 0)
	res = append(res, prefixBuf...)
	res = append(res, suffixBuf...)
	return res
}

func WriteByteStreamSplit(nums []any) []byte {
	ln := len(nums)
	if ln <= 0 {
		return []byte{}
	}

	switch nums[0].(type) {
	case float32:
		return WriteByteStreamSplitFloat32(nums)
	case float64:
		return WriteByteStreamSplitFloat64(nums)
	case int32:
		return WriteByteStreamSplitINT32(nums)
	case int64:
		return WriteByteStreamSplitINT64(nums)
	case string, []byte:
		return WriteByteStreamSplitFixedLenByteArray(nums)
	default:
		return []byte{}
	}
}

func WriteByteStreamSplitFloat32(vals []any) []byte {
	ln := len(vals)
	if ln <= 0 {
		return []byte{}
	}
	buf := make([]byte, ln*4)
	for i, n := range vals {
		v := math.Float32bits(n.(float32))
		buf[i] = byte(v)
		buf[ln+i] = byte(v >> 8)
		buf[ln*2+i] = byte(v >> 16)
		buf[ln*3+i] = byte(v >> 24)
	}
	return buf
}

func WriteByteStreamSplitFloat64(vals []any) []byte {
	ln := len(vals)
	if ln <= 0 {
		return []byte{}
	}

	buf := make([]byte, ln*8)
	for i, n := range vals {
		v := math.Float64bits(n.(float64))
		buf[i] = byte(v)
		buf[ln+i] = byte(v >> 8)
		buf[ln*2+i] = byte(v >> 16)
		buf[ln*3+i] = byte(v >> 24)
		buf[ln*4+i] = byte(v >> 32)
		buf[ln*5+i] = byte(v >> 40)
		buf[ln*6+i] = byte(v >> 48)
		buf[ln*7+i] = byte(v >> 56)
	}
	return buf
}

func WriteByteStreamSplitINT32(vals []any) []byte {
	ln := len(vals)
	if ln <= 0 {
		return []byte{}
	}
	buf := make([]byte, ln*4)
	for i, n := range vals {
		v := uint32(n.(int32))
		buf[i] = byte(v)
		buf[ln+i] = byte(v >> 8)
		buf[ln*2+i] = byte(v >> 16)
		buf[ln*3+i] = byte(v >> 24)
	}
	return buf
}

func WriteByteStreamSplitINT64(vals []any) []byte {
	ln := len(vals)
	if ln <= 0 {
		return []byte{}
	}
	buf := make([]byte, ln*8)
	for i, n := range vals {
		v := uint64(n.(int64))
		buf[i] = byte(v)
		buf[ln+i] = byte(v >> 8)
		buf[ln*2+i] = byte(v >> 16)
		buf[ln*3+i] = byte(v >> 24)
		buf[ln*4+i] = byte(v >> 32)
		buf[ln*5+i] = byte(v >> 40)
		buf[ln*6+i] = byte(v >> 48)
		buf[ln*7+i] = byte(v >> 56)
	}
	return buf
}

func WriteByteStreamSplitFixedLenByteArray(vals []any) []byte {
	ln := len(vals)
	if ln <= 0 {
		return []byte{}
	}
	// Get element size from first value
	first, ok := plainByteValue(vals[0])
	if !ok {
		return []byte{}
	}
	elemSize := len(first)
	if elemSize <= 0 {
		return []byte{}
	}
	buf := make([]byte, ln*elemSize)
	for i, n := range vals {
		s, ok := plainByteValue(n)
		if !ok || len(s) != elemSize {
			return []byte{}
		}
		for j := 0; j < elemSize; j++ {
			buf[ln*j+i] = s[j]
		}
	}
	return buf
}
