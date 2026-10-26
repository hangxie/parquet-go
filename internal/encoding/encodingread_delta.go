package encoding

import (
	"bytes"
	"fmt"
	"io"
)

// deltaHeader holds the fields that open a DELTA_BINARY_PACKED stream.
type deltaHeader struct {
	numMiniblocks        uint64
	numValues            uint64
	numValuesInMiniblock uint64
	firstValueZigZag     uint64
}

// readDeltaHeader parses and validates a delta header holding at most maxValues values.
func readDeltaHeader(bytesReader *bytes.Reader, maxValues uint64) (deltaHeader, error) {
	var h deltaHeader
	blockSize, err := ReadUnsignedVarInt(bytesReader)
	if err != nil {
		return h, err
	}
	if h.numMiniblocks, err = ReadUnsignedVarInt(bytesReader); err != nil {
		return h, err
	}
	if h.numValues, err = ReadUnsignedVarInt(bytesReader); err != nil {
		return h, err
	}
	if h.firstValueZigZag, err = ReadUnsignedVarInt(bytesReader); err != nil {
		return h, err
	}
	if h.numMiniblocks == 0 {
		return h, fmt.Errorf("numMiniblocksInBlock is zero")
	}
	// Spec-required multiples guarantee each miniblock holds values, so decoding progresses.
	if blockSize == 0 || blockSize%128 != 0 {
		return h, fmt.Errorf("block size %d is not a positive multiple of 128", blockSize)
	}
	if err := validateCount(blockSize); err != nil {
		return h, err
	}
	if blockSize%h.numMiniblocks != 0 || (blockSize/h.numMiniblocks)%32 != 0 {
		return h, fmt.Errorf("values per miniblock (block size %d / %d miniblocks) is not a multiple of 32", blockSize, h.numMiniblocks)
	}
	if h.numValues > maxValues {
		return h, fmt.Errorf("value count %d exceeds expected %d", h.numValues, maxValues)
	}
	h.numValuesInMiniblock = blockSize / h.numMiniblocks
	return h, nil
}

// readBitWidths reads a block's miniblock widths, checking the input holds them before allocating.
func readBitWidths(bytesReader *bytes.Reader, numMiniblocks uint64) ([]byte, error) {
	if remaining := uint64(bytesReader.Len()); numMiniblocks > remaining {
		return nil, fmt.Errorf("miniblock count %d exceeds remaining data %d", numMiniblocks, remaining)
	}
	bitWidths := make([]byte, numMiniblocks)
	if _, err := io.ReadFull(bytesReader, bitWidths); err != nil {
		return nil, err
	}
	return bitWidths, nil
}

// readMiniblock returns at most needed deltas, never allocating by the header's miniblock size.
func readMiniblock(bytesReader *bytes.Reader, numValuesInMiniblock, needed uint64, bitWidth byte) ([]any, error) {
	n := min(numValuesInMiniblock, needed)
	if bitWidth == 0 {
		cur := make([]any, n)
		for i := range cur {
			cur[i] = int64(0)
		}
		return cur, nil
	}
	if bitWidth > 64 {
		return nil, fmt.Errorf("bit width %d exceeds 64", bitWidth)
	}
	// The spec's full padding is what locates the next stream, e.g. DELTA_BYTE_ARRAY suffixes.
	width := uint64(bitWidth)
	full := numValuesInMiniblock * width / 8
	if remaining := uint64(bytesReader.Len()); full > remaining {
		return nil, fmt.Errorf("miniblock needs %d bytes but %d remain", full, remaining)
	}
	cur, err := ReadBitPackedCount(bytesReader, n, width)
	if err != nil {
		return nil, err
	}
	if _, err := bytesReader.Seek(int64(full-(n*width+7)/8), io.SeekCurrent); err != nil {
		return nil, err
	}
	return cur, nil
}

// ReadDeltaBinaryPackedINT32 decodes at most maxValues INT32 values.
func ReadDeltaBinaryPackedINT32(bytesReader *bytes.Reader, maxValues uint64) ([]any, error) {
	var res []any
	h, err := readDeltaHeader(bytesReader, maxValues)
	if err != nil {
		return res, fmt.Errorf("ReadDeltaBinaryPackedINT32: %w", err)
	}

	fv32 := int32(h.firstValueZigZag)
	firstValue := int32(uint32(fv32)>>1) ^ -(fv32 & 1)
	numValues := h.numValues

	res = make([]any, 0)
	res = append(res, firstValue)
	for uint64(len(res)) < numValues {
		minDeltaZigZag, err := ReadUnsignedVarInt(bytesReader)
		if err != nil {
			return res, err
		}

		md32 := int32(minDeltaZigZag)
		minDelta := int32(uint32(md32)>>1) ^ -(md32 & 1)
		bitWidths, err := readBitWidths(bytesReader, h.numMiniblocks)
		if err != nil {
			return res, fmt.Errorf("ReadDeltaBinaryPackedINT32: %w", err)
		}
		for i := 0; uint64(i) < h.numMiniblocks && uint64(len(res)) < numValues; i++ {
			cur, err := readMiniblock(bytesReader, h.numValuesInMiniblock, numValues-uint64(len(res)), bitWidths[i])
			if err != nil {
				return res, fmt.Errorf("ReadDeltaBinaryPackedINT32: %w", err)
			}
			for j := range cur {
				res = append(res, int32(res[len(res)-1].(int32)+int32(cur[j].(int64))+minDelta))
			}
		}
	}
	return res[:numValues], err
}

// ReadDeltaBinaryPackedINT64 decodes at most maxValues INT64 values.
func ReadDeltaBinaryPackedINT64(bytesReader *bytes.Reader, maxValues uint64) ([]any, error) {
	var res []any
	h, err := readDeltaHeader(bytesReader, maxValues)
	if err != nil {
		return res, fmt.Errorf("ReadDeltaBinaryPackedINT64: %w", err)
	}

	firstValue := int64(h.firstValueZigZag>>1) ^ -(int64(h.firstValueZigZag) & 1)
	numValues := h.numValues

	res = make([]any, 0)
	res = append(res, int64(firstValue))
	for uint64(len(res)) < numValues {
		minDeltaZigZag, err := ReadUnsignedVarInt(bytesReader)
		if err != nil {
			return res, err
		}
		minDelta := int64(minDeltaZigZag>>1) ^ -(int64(minDeltaZigZag) & 1)
		bitWidths, err := readBitWidths(bytesReader, h.numMiniblocks)
		if err != nil {
			return res, fmt.Errorf("ReadDeltaBinaryPackedINT64: %w", err)
		}

		for i := 0; uint64(i) < h.numMiniblocks && uint64(len(res)) < numValues; i++ {
			cur, err := readMiniblock(bytesReader, h.numValuesInMiniblock, numValues-uint64(len(res)), bitWidths[i])
			if err != nil {
				return res, fmt.Errorf("ReadDeltaBinaryPackedINT64: %w", err)
			}
			for j := range cur {
				res = append(res, (res[len(res)-1].(int64) + cur[j].(int64) + minDelta))
			}
		}
	}
	return res[:numValues], err
}

// ReadDeltaLengthByteArray decodes at most maxValues byte arrays.
func ReadDeltaLengthByteArray(bytesReader *bytes.Reader, maxValues uint64) ([]any, error) {
	var (
		res []any
		err error
	)

	lengths, err := ReadDeltaBinaryPackedINT64(bytesReader, maxValues)
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

// ReadDeltaByteArray decodes at most maxValues byte arrays.
func ReadDeltaByteArray(bytesReader *bytes.Reader, maxValues uint64) ([]any, error) {
	var (
		res []any
		err error
	)

	prefixLengths, err := ReadDeltaBinaryPackedINT64(bytesReader, maxValues)
	if err != nil {
		return res, err
	}
	suffixes, err := ReadDeltaLengthByteArray(bytesReader, maxValues)
	if err != nil {
		return res, err
	}
	if len(prefixLengths) == 0 {
		return []any{}, nil
	}
	// prefixLengths and suffixes are decoded independently; a corrupted stream
	// can produce mismatched lengths, so require one suffix per prefix length.
	if len(suffixes) != len(prefixLengths) {
		return res, fmt.Errorf("ReadDeltaByteArray: suffix count %d does not match prefix count %d", len(suffixes), len(prefixLengths))
	}
	res = make([]any, len(prefixLengths))

	res[0] = suffixes[0]
	for i := 1; i < len(prefixLengths); i++ {
		prefixLength := prefixLengths[i].(int64)
		prev := res[i-1].(string)
		if prefixLength < 0 || prefixLength > int64(len(prev)) {
			return res, fmt.Errorf("ReadDeltaByteArray: invalid prefix length %d at index %d (previous value length %d)", prefixLength, i, len(prev))
		}
		prefix := prev[:prefixLength]
		suffix := suffixes[i].(string)
		res[i] = prefix + suffix
	}
	return res, err
}
