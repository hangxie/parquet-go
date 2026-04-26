package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	resCurNeedBits := bitWidth
	var used uint64 = 0
	left := 8 - used
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
