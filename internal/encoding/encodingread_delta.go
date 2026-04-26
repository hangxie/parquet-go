package encoding

import (
	"bytes"
	"fmt"
)

func ReadDeltaBinaryPackedINT32(bytesReader *bytes.Reader) ([]any, error) {
	var (
		err error
		res []any
	)

	blockSize, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}
	numMiniblocksInBlock, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}
	numValues, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}
	firstValueZigZag, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}

	fv32 := int32(firstValueZigZag)
	firstValue := int32(uint32(fv32)>>1) ^ -(fv32 & 1)
	numValuesInMiniBlock := blockSize / numMiniblocksInBlock

	res = make([]any, 0)
	res = append(res, firstValue)
	for uint64(len(res)) < numValues {
		minDeltaZigZag, err := ReadUnsignedVarInt(bytesReader)
		if err != nil {
			return res, err
		}

		md32 := int32(minDeltaZigZag)
		minDelta := int32(uint32(md32)>>1) ^ -(md32 & 1)
		bitWidths := make([]uint64, numMiniblocksInBlock)
		for i := range numMiniblocksInBlock {
			b, err := bytesReader.ReadByte()
			if err != nil {
				return res, err
			}
			bitWidths[i] = uint64(b)
		}
		for i := 0; uint64(i) < numMiniblocksInBlock && uint64(len(res)) < numValues; i++ {
			cur, err := ReadBitPacked(bytesReader, (numValuesInMiniBlock/8)<<1, bitWidths[i])
			if err != nil {
				return res, err
			}
			for j := 0; j < len(cur) && len(res) < int(numValues); j++ {
				res = append(res, int32(res[len(res)-1].(int32)+int32(cur[j].(int64))+minDelta))
			}
		}
	}
	return res[:numValues], err
}

// res is INT64
func ReadDeltaBinaryPackedINT64(bytesReader *bytes.Reader) ([]any, error) {
	var (
		err error
		res []any
	)

	blockSize, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}
	numMiniblocksInBlock, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}
	numValues, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}
	firstValueZigZag, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return res, err
	}
	firstValue := int64(firstValueZigZag>>1) ^ -(int64(firstValueZigZag) & 1)

	numValuesInMiniBlock := blockSize / numMiniblocksInBlock

	res = make([]any, 0)
	res = append(res, int64(firstValue))
	for uint64(len(res)) < numValues {
		minDeltaZigZag, err := ReadUnsignedVarInt(bytesReader)
		if err != nil {
			return res, err
		}
		minDelta := int64(minDeltaZigZag>>1) ^ -(int64(minDeltaZigZag) & 1)
		bitWidths := make([]uint64, numMiniblocksInBlock)
		for i := range numMiniblocksInBlock {
			b, err := bytesReader.ReadByte()
			if err != nil {
				return res, err
			}
			bitWidths[i] = uint64(b)
		}

		for i := 0; uint64(i) < numMiniblocksInBlock && uint64(len(res)) < numValues; i++ {
			cur, err := ReadBitPacked(bytesReader, (numValuesInMiniBlock/8)<<1, bitWidths[i])
			if err != nil {
				return res, err
			}
			for j := range cur {
				res = append(res, (res[len(res)-1].(int64) + cur[j].(int64) + minDelta))
			}
		}
	}
	return res[:numValues], err
}

func ReadDeltaLengthByteArray(bytesReader *bytes.Reader) ([]any, error) {
	var (
		res []any
		err error
	)

	lengths, err := ReadDeltaBinaryPackedINT64(bytesReader)
	if err != nil {
		return res, err
	}
	res = make([]any, len(lengths))
	for i := range lengths {
		res[i] = ""
		l := lengths[i].(int64)
		if l < 0 {
			return res, fmt.Errorf("ReadDeltaLengthByteArray: invalid negative length %d at index %d", l, i)
		}
		length := uint64(l)
		if length > 0 {
			cur, err := ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader, 1, length)
			if err != nil {
				return res, err
			}
			res[i] = cur[0]
		}
	}

	return res, err
}

func ReadDeltaByteArray(bytesReader *bytes.Reader) ([]any, error) {
	var (
		res []any
		err error
	)

	prefixLengths, err := ReadDeltaBinaryPackedINT64(bytesReader)
	if err != nil {
		return res, err
	}
	suffixes, err := ReadDeltaLengthByteArray(bytesReader)
	if err != nil {
		return res, err
	}
	if len(prefixLengths) == 0 {
		return []any{}, nil
	}
	res = make([]any, len(prefixLengths))

	res[0] = suffixes[0]
	for i := 1; i < len(prefixLengths); i++ {
		prefixLength := prefixLengths[i].(int64)
		prefix := res[i-1].(string)[:prefixLength]
		suffix := suffixes[i].(string)
		res[i] = prefix + suffix
	}
	return res, err
}
