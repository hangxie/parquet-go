package encoding

import (
	"fmt"
	"math/bits"
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

// byteArrayStrings returns string and []byte values as strings, copying any []byte.
func byteArrayStrings(arrays []any) ([]string, error) {
	strs := make([]string, len(arrays))
	for i, v := range arrays {
		switch value := v.(type) {
		case string:
			strs[i] = value
		case []byte:
			strs[i] = string(value)
		default:
			return nil, fmt.Errorf("[%v] is not a string or []byte", v)
		}
	}
	return strs, nil
}

func WriteDeltaLengthByteArray(arrays []any) ([]byte, error) {
	strs, err := byteArrayStrings(arrays)
	if err != nil {
		return nil, err
	}
	return writeDeltaLengthStrings(strs)
}

func writeDeltaLengthStrings(strs []string) ([]byte, error) {
	lengthArray := make([]any, len(strs))
	for i, str := range strs {
		length, err := Int32Length(len(str), "DELTA_LENGTH_BYTE_ARRAY length")
		if err != nil {
			return nil, err
		}
		lengthArray[i] = length
	}

	res := WriteDeltaINT32(lengthArray)
	for _, str := range strs {
		res = append(res, str...)
	}
	return res, nil
}

func WriteDeltaByteArray(arrays []any) ([]byte, error) {
	strs, err := byteArrayStrings(arrays)
	if err != nil {
		return nil, err
	}

	prefixLengths := make([]any, len(strs))
	suffixes := make([]string, len(strs))
	for i, s2 := range strs {
		j := 0
		if i > 0 {
			s1 := strs[i-1]
			for j < len(s1) && j < len(s2) && s1[j] == s2[j] {
				j++
			}
		}
		prefixLength, err := Int32Length(j, "DELTA_BYTE_ARRAY prefix length")
		if err != nil {
			return nil, err
		}
		prefixLengths[i] = prefixLength
		suffixes[i] = s2[j:]
	}

	prefixBuf := WriteDeltaINT32(prefixLengths)
	suffixBuf, err := writeDeltaLengthStrings(suffixes)
	if err != nil {
		return nil, err
	}
	return append(prefixBuf, suffixBuf...), nil
}
