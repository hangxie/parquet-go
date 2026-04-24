package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/hangxie/parquet-go/v3/parquet"
)

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
	res := make([]any, cnt)
	buf := make([]byte, 4)
	for i := range int(cnt) {
		if _, err := io.ReadFull(bytesReader, buf); err != nil {
			return nil, fmt.Errorf("ReadPlainBYTE_ARRAY: read length prefix at index %d: %w", i, err)
		}
		ln := binary.LittleEndian.Uint32(buf)
		if uint64(ln) > uint64(bytesReader.Len()) {
			return nil, fmt.Errorf("ReadPlainBYTE_ARRAY: length prefix %d exceeds remaining data size %d", ln, bytesReader.Len())
		}
		cur := make([]byte, ln)
		if ln > 0 {
			if _, err := io.ReadFull(bytesReader, cur); err != nil {
				return nil, fmt.Errorf("ReadPlainBYTE_ARRAY: read value at index %d: %w", i, err)
			}
		}
		res[i] = string(cur)
	}
	return res, nil
}

func ReadPlainFIXED_LEN_BYTE_ARRAY(bytesReader *bytes.Reader, cnt, fixedLength uint64) ([]any, error) {
	if err := validateCount(cnt); err != nil {
		return nil, fmt.Errorf("ReadPlainFIXED_LEN_BYTE_ARRAY: %w", err)
	}
	if fixedLength > uint64(bytesReader.Len()) {
		return nil, fmt.Errorf("ReadPlainFIXED_LEN_BYTE_ARRAY: fixed length %d exceeds remaining data size %d", fixedLength, bytesReader.Len())
	}
	res := make([]any, cnt)
	for i := range int(cnt) {
		cur := make([]byte, fixedLength)
		if _, err := io.ReadFull(bytesReader, cur); err != nil {
			return nil, fmt.Errorf("ReadPlainFIXED_LEN_BYTE_ARRAY: read value at index %d: %w", i, err)
		}
		res[i] = string(cur)
	}
	return res, nil
}
