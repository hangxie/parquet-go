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
// ReadRLE decodes a single RLE run. maxCount, when > 0, bounds the run length to
// the caller's expected value count: an RLE run encodes its length in a few bytes
// regardless of how many values it represents, so a corrupted/malicious header can
// otherwise claim a run far larger than the page could legitimately contain,
// forcing a huge allocation. Pass 0 to disable the bound (trusted input only).
func ReadRLE(bytesReader *bytes.Reader, header, bitWidth, maxCount uint64) ([]any, error) {
	var err error
	var res []any
	cnt := header >> 1
	if err := validateCount(cnt); err != nil {
		return res, fmt.Errorf("ReadRLE: %w", err)
	}
	if maxCount > 0 && cnt > maxCount {
		return res, fmt.Errorf("ReadRLE: run count %d exceeds expected value count %d", cnt, maxCount)
	}
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
	if err := validateCount(cnt); err != nil {
		return nil, fmt.Errorf("ReadBitPackedCount: %w", err)
	}
	if cnt == 0 {
		return []any{}, nil
	}
	res := make([]any, 0, cnt)
	if bitWidth == 0 {
		for range int(cnt) {
			res = append(res, int64(0))
		}
		return res, nil
	}

	// Calculate how many bytes we need to read (round up). The packed data must
	// be present in the reader (a short read zero-fills the tail), so reject
	// counts that imply far more bytes than remain to avoid huge allocations
	// from corrupted/malicious input. ReadPlainBOOLEAN over-reads by up to 8x,
	// so allow that margin before rejecting.
	byteCnt := (cnt*bitWidth + 7) / 8
	if remaining := uint64(bytesReader.Len()); byteCnt > remaining*8+8 {
		return res, fmt.Errorf("ReadBitPackedCount: byte count %d exceeds remaining data size %d", byteCnt, remaining)
	}

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
// ReadRLEBitPackedHybrid decodes an RLE/bit-packed hybrid buffer. maxCount, when
// > 0, is the caller's expected number of values: decoding stops once that many
// values have been produced and individual RLE runs are bounded to it, which
// prevents a malformed buffer from amplifying a few bytes into a huge allocation.
// Pass 0 to disable the bound (trusted input only).
func ReadRLEBitPackedHybrid(bytesReader *bytes.Reader, bitWidth, length, maxCount uint64) ([]any, error) {
	res := make([]any, 0)
	if length <= 0 {
		lb, err := ReadPlainINT32(bytesReader, 1)
		if err != nil {
			return res, err
		}
		length = uint64(lb[0].(int32))
	}

	// The RLE section must be fully present in the reader; a declared length
	// larger than what remains is corrupt and would otherwise allocate a buffer
	// sized by an untrusted field.
	if remaining := uint64(bytesReader.Len()); length > remaining {
		return res, fmt.Errorf("ReadRLEBitPackedHybrid: declared length %d exceeds remaining data %d", length, remaining)
	}

	buf := make([]byte, length)
	if _, err := bytesReader.Read(buf); err != nil {
		return res, err
	}

	newReader := bytes.NewReader(buf)
	for newReader.Len() > 0 {
		if maxCount > 0 && uint64(len(res)) >= maxCount {
			break
		}
		header, err := ReadUnsignedVarInt(newReader)
		if err != nil {
			return res, err
		}
		if header&1 == 0 {
			buf, err := ReadRLE(newReader, header, bitWidth, maxCount)
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
