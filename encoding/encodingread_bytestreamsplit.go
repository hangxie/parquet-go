package encoding

import (
	"bytes"
	"fmt"
	"io"
	"math"
)

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
