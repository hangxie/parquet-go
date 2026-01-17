package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/hangxie/parquet-go/v2/parquet"
)

// maxAllowedCount represents the largest count value allowed for slice allocation.
// This matches the int32 limit defined in the Parquet thrift specification for
// num_values in DataPageHeader, DataPageHeaderV2, and DictionaryPageHeader.
// Using int32 max instead of int max provides:
// - Spec compliance with Parquet format (num_values is defined as i32)
// - Protection against malicious files with corrupted metadata
// - Compatibility with 32-bit systems
// - Defense against zip-bomb and DoS attacks (CVE-2021-41561)
const maxAllowedCount = uint64(2147483647) // 2^31 - 1 (max int32)

// validateCount checks if a count value is reasonable for slice allocation
func validateCount(cnt uint64) error {
	// Check if cnt is larger than maxAllowedCount (max int32 per Parquet spec)
	// This prevents panics from makeslice and defends against corrupted/malicious files
	if cnt > maxAllowedCount {
		return fmt.Errorf("invalid count: %d (exceeds maximum allowed: %d)", cnt, maxAllowedCount)
	}
	return nil
}

func ReadPlain(bytesReader *bytes.Reader, dataType parquet.Type, cnt, bitWidth uint64) ([]any, error) {
	switch dataType {
	case parquet.Type_BOOLEAN:
		return ReadPlainBOOLEAN(bytesReader, cnt)
	case parquet.Type_INT32:
		return ReadPlainINT32(bytesReader, cnt)
	case parquet.Type_INT64:
		return ReadPlainINT64(bytesReader, cnt)
	case parquet.Type_INT96:
		return ReadPlainINT96(bytesReader, cnt)
	case parquet.Type_FLOAT:
		return ReadPlainFLOAT(bytesReader, cnt)
	case parquet.Type_DOUBLE:
		return ReadPlainDOUBLE(bytesReader, cnt)
	case parquet.Type_BYTE_ARRAY:
		return ReadPlainBYTE_ARRAY(bytesReader, cnt)
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader, cnt, bitWidth)
	default:
		return nil, fmt.Errorf("unknown parquet type: %v", dataType)
	}
}

func ReadPlainBOOLEAN(bytesReader *bytes.Reader, cnt uint64) ([]any, error) {
	var (
		res []any
		err error
	)

	if err := validateCount(cnt); err != nil {
		return nil, fmt.Errorf("ReadPlainBOOLEAN: %w", err)
	}
	res = make([]any, cnt)
	resInt, err := ReadBitPacked(bytesReader, uint64(cnt<<1), 1)
	if err != nil {
		return res, err
	}

	for i := range int(cnt) {
		if resInt[i].(int64) > 0 {
			res[i] = true
		} else {
			res[i] = false
		}
	}
	return res, err
}

func ReadPlainINT32(bytesReader *bytes.Reader, cnt uint64) ([]any, error) {
	if err := validateCount(cnt); err != nil {
		return nil, fmt.Errorf("ReadPlainINT32: %w", err)
	}
	var err error
	res := make([]any, cnt)
	err = BinaryReadINT32(bytesReader, res)
	return res, err
}

func ReadPlainINT64(bytesReader *bytes.Reader, cnt uint64) ([]any, error) {
	if err := validateCount(cnt); err != nil {
		return nil, fmt.Errorf("ReadPlainINT64: %w", err)
	}
	var err error
	res := make([]any, cnt)
	err = BinaryReadINT64(bytesReader, res)
	return res, err
}

func ReadPlainINT96(bytesReader *bytes.Reader, cnt uint64) ([]any, error) {
	if err := validateCount(cnt); err != nil {
		return nil, fmt.Errorf("ReadPlainINT96: %w", err)
	}
	var err error
	res := make([]any, cnt)
	cur := make([]byte, 12)
	for i := range int(cnt) {
		if _, err = bytesReader.Read(cur); err != nil {
			break
		}
		res[i] = string(cur[:12])
	}
	return res, err
}

func ReadPlainFLOAT(bytesReader *bytes.Reader, cnt uint64) ([]any, error) {
	if err := validateCount(cnt); err != nil {
		return nil, fmt.Errorf("ReadPlainFLOAT: %w", err)
	}
	var err error
	res := make([]any, cnt)
	err = BinaryReadFLOAT32(bytesReader, res)
	return res, err
}

func ReadPlainDOUBLE(bytesReader *bytes.Reader, cnt uint64) ([]any, error) {
	if err := validateCount(cnt); err != nil {
		return nil, fmt.Errorf("ReadPlainDOUBLE: %w", err)
	}
	var err error
	res := make([]any, cnt)
	err = BinaryReadFLOAT64(bytesReader, res)
	return res, err
}

func ReadPlainBYTE_ARRAY(bytesReader *bytes.Reader, cnt uint64) ([]any, error) {
	if err := validateCount(cnt); err != nil {
		return nil, fmt.Errorf("ReadPlainBYTE_ARRAY: %w", err)
	}
	var err error
	res := make([]any, cnt)
	for i := range int(cnt) {
		buf := make([]byte, 4)
		if _, err = bytesReader.Read(buf); err != nil {
			break
		}
		ln := binary.LittleEndian.Uint32(buf)
		cur := make([]byte, ln)
		if _, err := bytesReader.Read(cur); err != nil {
			return nil, err
		}
		res[i] = string(cur)
	}
	return res, err
}

func ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader *bytes.Reader, cnt, fixedLength uint64) ([]any, error) {
	if err := validateCount(cnt); err != nil {
		return nil, fmt.Errorf("ReadPlainFIXED_LEN_BYTE_ARRAY: %w", err)
	}
	var err error
	res := make([]any, cnt)
	for i := range int(cnt) {
		cur := make([]byte, fixedLength)
		if _, err = bytesReader.Read(cur); err != nil {
			break
		}
		res[i] = string(cur)
	}
	return res, err
}

func ReadUnsignedVarInt(bytesReader *bytes.Reader) (uint64, error) {
	var res uint64 = 0
	var shift uint64 = 0
	for {
		b, err := bytesReader.ReadByte()
		if err != nil {
			return res, err
		}
		res |= ((uint64(b) & uint64(0x7F)) << uint64(shift))
		if (b & 0x80) == 0 {
			break
		}
		shift += 7
	}
	return res, nil
}

// RLE return res is []INT64
func ReadRLE(bytesReader *bytes.Reader, header, bitWidth uint64) ([]any, error) {
	var err error
	var res []any
	cnt := header >> 1
	width := (bitWidth + 7) / 8
	data := make([]byte, width)
	if width > 0 {
		if _, err = bytesReader.Read(data); err != nil {
			return res, err
		}
	}

	var val int64
	if bitWidth > 32 {
		// For INT64 or larger, use Uint64
		for len(data) < 8 {
			data = append(data, byte(0))
		}
		val = int64(binary.LittleEndian.Uint64(data))
	} else {
		// For INT32, BOOLEAN, or smaller, use Uint32
		for len(data) < 4 {
			data = append(data, byte(0))
		}
		val = int64(binary.LittleEndian.Uint32(data))
	}
	res = make([]any, cnt)

	for i := range int(cnt) {
		res[i] = val
	}
	return res, err
}

// ReadBitPacked reads bit-packed values using LSB-first bit order.
// The header encodes the count: cnt = (header >> 1) * 8
// Returns []any containing int64 values.
func ReadBitPacked(bytesReader *bytes.Reader, header, bitWidth uint64) ([]any, error) {
	cnt := (header >> 1) * 8
	return ReadBitPackedCount(bytesReader, cnt, bitWidth)
}

// ReadBitPackedCount reads cnt bit-packed values directly using LSB-first bit order.
// Used when count is known in advance (e.g., deprecated BIT_PACKED encoding).
// Returns []any containing int64 values.
func ReadBitPackedCount(bytesReader *bytes.Reader, cnt, bitWidth uint64) ([]any, error) {
	res := make([]any, 0, cnt)
	if cnt == 0 {
		return res, nil
	}
	if bitWidth == 0 {
		for range int(cnt) {
			res = append(res, int64(0))
		}
		return res, nil
	}

	// Calculate how many bytes we need to read (round up)
	byteCnt := (cnt*bitWidth + 7) / 8

	bytesBuf := make([]byte, byteCnt)
	if _, err := bytesReader.Read(bytesBuf); err != nil {
		return res, err
	}

	// Unpack bits using LSB-first order
	i := 0
	var resCur uint64 = 0
	var resCurNeedBits uint64 = bitWidth
	var used uint64 = 0
	var left uint64 = 8 - used
	b := bytesBuf[i]

	for uint64(len(res)) < cnt {
		if left >= resCurNeedBits {
			resCur |= uint64(((uint64(b) >> uint64(used)) & ((1 << uint64(resCurNeedBits)) - 1)) << uint64(bitWidth-resCurNeedBits))
			res = append(res, int64(resCur))
			left -= resCurNeedBits
			used += resCurNeedBits

			resCurNeedBits = bitWidth
			resCur = 0

			if left <= 0 && i+1 < len(bytesBuf) {
				i++
				b = bytesBuf[i]
				left = 8
				used = 0
			}
		} else {
			resCur |= uint64((uint64(b) >> uint64(used)) << uint64(bitWidth-resCurNeedBits))
			i++
			if i < len(bytesBuf) {
				b = bytesBuf[i]
			}
			resCurNeedBits -= left
			left = 8
			used = 0
		}
	}

	return res, nil
}

// res is INT64
func ReadRLEBitPackedHybrid(bytesReader *bytes.Reader, bitWidth, length uint64) ([]any, error) {
	res := make([]any, 0)
	if length <= 0 {
		lb, err := ReadPlainINT32(bytesReader, 1)
		if err != nil {
			return res, err
		}
		length = uint64(lb[0].(int32))
	}

	buf := make([]byte, length)
	if _, err := bytesReader.Read(buf); err != nil {
		return res, err
	}

	newReader := bytes.NewReader(buf)
	for newReader.Len() > 0 {
		header, err := ReadUnsignedVarInt(newReader)
		if err != nil {
			return res, err
		}
		if header&1 == 0 {
			buf, err := ReadRLE(newReader, header, bitWidth)
			if err != nil {
				return res, err
			}
			res = append(res, buf...)

		} else {
			buf, err := ReadBitPacked(newReader, header, bitWidth)
			if err != nil {
				return res, err
			}
			res = append(res, buf...)
		}
	}
	return res, nil
}

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
	var firstValue int32 = int32(uint32(fv32)>>1) ^ -(fv32 & 1)
	numValuesInMiniBlock := blockSize / numMiniblocksInBlock

	res = make([]any, 0)
	res = append(res, firstValue)
	for uint64(len(res)) < numValues {
		minDeltaZigZag, err := ReadUnsignedVarInt(bytesReader)
		if err != nil {
			return res, err
		}

		md32 := int32(minDeltaZigZag)
		var minDelta int32 = int32(uint32(md32)>>1) ^ -(md32 & 1)
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
	var firstValue int64 = int64(firstValueZigZag>>1) ^ -(int64(firstValueZigZag) & 1)

	numValuesInMiniBlock := blockSize / numMiniblocksInBlock

	res = make([]any, 0)
	res = append(res, int64(firstValue))
	for uint64(len(res)) < numValues {
		minDeltaZigZag, err := ReadUnsignedVarInt(bytesReader)
		if err != nil {
			return res, err
		}
		var minDelta int64 = int64(minDeltaZigZag>>1) ^ -(int64(minDeltaZigZag) & 1)
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
		length := uint64(lengths[i].(int64))
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

func ReadByteStreamSplitFloat32(bytesReader *bytes.Reader, cnt uint64) ([]any, error) {
	if err := validateCount(cnt); err != nil {
		return nil, fmt.Errorf("ReadByteStreamSplitFloat32: %w", err)
	}
	res := make([]any, cnt)
	buf := make([]byte, cnt*4)

	n, err := io.ReadFull(bytesReader, buf)
	if err != nil {
		return res, err
	}
	if cnt*4 != uint64(n) {
		return res, io.ErrUnexpectedEOF
	}

	for i := range cnt {
		res[i] = math.Float32frombits(uint32(buf[i]) |
			uint32(buf[cnt+i])<<8 |
			uint32(buf[cnt*2+i])<<16 |
			uint32(buf[cnt*3+i])<<24)
	}

	return res, err
}

func ReadByteStreamSplitFloat64(bytesReader *bytes.Reader, cnt uint64) ([]any, error) {
	if err := validateCount(cnt); err != nil {
		return nil, fmt.Errorf("ReadByteStreamSplitFloat64: %w", err)
	}
	res := make([]any, cnt)
	buf := make([]byte, cnt*8)

	n, err := io.ReadFull(bytesReader, buf)
	if err != nil {
		return res, err
	}
	if cnt*8 != uint64(n) {
		return res, io.ErrUnexpectedEOF
	}

	for i := range cnt {
		res[i] = math.Float64frombits(uint64(buf[i]) |
			uint64(buf[cnt+i])<<8 |
			uint64(buf[cnt*2+i])<<16 |
			uint64(buf[cnt*3+i])<<24 |
			uint64(buf[cnt*4+i])<<32 |
			uint64(buf[cnt*5+i])<<40 |
			uint64(buf[cnt*6+i])<<48 |
			uint64(buf[cnt*7+i])<<56)
	}

	return res, err
}

func ReadByteStreamSplitINT32(bytesReader *bytes.Reader, cnt uint64) ([]any, error) {
	if err := validateCount(cnt); err != nil {
		return nil, fmt.Errorf("ReadByteStreamSplitINT32: %w", err)
	}
	res := make([]any, cnt)
	buf := make([]byte, cnt*4)

	n, err := io.ReadFull(bytesReader, buf)
	if err != nil {
		return res, err
	}
	if cnt*4 != uint64(n) {
		return res, io.ErrUnexpectedEOF
	}

	for i := range cnt {
		res[i] = int32(uint32(buf[i]) |
			uint32(buf[cnt+i])<<8 |
			uint32(buf[cnt*2+i])<<16 |
			uint32(buf[cnt*3+i])<<24)
	}

	return res, err
}

func ReadByteStreamSplitINT64(bytesReader *bytes.Reader, cnt uint64) ([]any, error) {
	if err := validateCount(cnt); err != nil {
		return nil, fmt.Errorf("ReadByteStreamSplitINT64: %w", err)
	}
	res := make([]any, cnt)
	buf := make([]byte, cnt*8)

	n, err := io.ReadFull(bytesReader, buf)
	if err != nil {
		return res, err
	}
	if cnt*8 != uint64(n) {
		return res, io.ErrUnexpectedEOF
	}

	for i := range cnt {
		res[i] = int64(uint64(buf[i]) |
			uint64(buf[cnt+i])<<8 |
			uint64(buf[cnt*2+i])<<16 |
			uint64(buf[cnt*3+i])<<24 |
			uint64(buf[cnt*4+i])<<32 |
			uint64(buf[cnt*5+i])<<40 |
			uint64(buf[cnt*6+i])<<48 |
			uint64(buf[cnt*7+i])<<56)
	}

	return res, err
}

func ReadByteStreamSplitFixedLenByteArray(bytesReader *bytes.Reader, cnt, elemSize uint64) ([]any, error) {
	if err := validateCount(cnt); err != nil {
		return nil, fmt.Errorf("ReadByteStreamSplitFixedLenByteArray: %w", err)
	}
	if elemSize == 0 {
		return nil, fmt.Errorf("ReadByteStreamSplitFixedLenByteArray: element size must be > 0")
	}
	res := make([]any, cnt)
	buf := make([]byte, cnt*elemSize)

	n, err := io.ReadFull(bytesReader, buf)
	if err != nil {
		return res, err
	}
	if cnt*elemSize != uint64(n) {
		return res, io.ErrUnexpectedEOF
	}

	for i := range cnt {
		elem := make([]byte, elemSize)
		for j := range elemSize {
			elem[j] = buf[cnt*j+i]
		}
		res[i] = string(elem)
	}

	return res, err
}
