package encoding

import (
	"fmt"
	"math/bits"
	"reflect"
)

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
