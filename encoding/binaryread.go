package encoding

import (
	"fmt"
	"io"
	"math"
)

// LittleEndian
func BinaryReadINT32(r io.Reader, nums []any) error {
	buf := make([]byte, len(nums)*4)
	n, err := io.ReadFull(r, buf)
	if err != nil {
		return fmt.Errorf("failed to read INT32 binary data: %w", err)
	}
	if len(nums)*4 != n {
		return io.ErrUnexpectedEOF
	}

	for i := range nums {
		nums[i] = int32(uint32(buf[i*4+0]) |
			uint32(buf[i*4+1])<<8 |
			uint32(buf[i*4+2])<<16 |
			uint32(buf[i*4+3])<<24)
	}
	return nil
}

func BinaryReadINT64(r io.Reader, nums []any) error {
	buf := make([]byte, len(nums)*8)
	n, err := io.ReadFull(r, buf)
	if err != nil {
		return fmt.Errorf("failed to read INT64 binary data: %w", err)
	}
	if len(nums)*8 != n {
		return io.ErrUnexpectedEOF
	}

	for i := range nums {
		nums[i] = int64(uint64(buf[i*8+0]) |
			uint64(buf[i*8+1])<<8 |
			uint64(buf[i*8+2])<<16 |
			uint64(buf[i*8+3])<<24 |
			uint64(buf[i*8+4])<<32 |
			uint64(buf[i*8+5])<<40 |
			uint64(buf[i*8+6])<<48 |
			uint64(buf[i*8+7])<<56)
	}
	return nil
}

func BinaryReadFLOAT32(r io.Reader, nums []any) error {
	buf := make([]byte, len(nums)*4)
	n, err := io.ReadFull(r, buf)
	if err != nil {
		return fmt.Errorf("failed to read FLOAT32 binary data: %w", err)
	}
	if len(nums)*4 != n {
		return io.ErrUnexpectedEOF
	}

	for i := range nums {
		nums[i] = math.Float32frombits(uint32(buf[i*4+0]) |
			uint32(buf[i*4+1])<<8 |
			uint32(buf[i*4+2])<<16 |
			uint32(buf[i*4+3])<<24)
	}
	return nil
}

func BinaryReadFLOAT64(r io.Reader, nums []any) error {
	buf := make([]byte, len(nums)*8)
	n, err := io.ReadFull(r, buf)
	if err != nil {
		return fmt.Errorf("failed to read FLOAT64 binary data: %w", err)
	}
	if len(nums)*8 != n {
		return io.ErrUnexpectedEOF
	}

	for i := range nums {
		nums[i] = math.Float64frombits(uint64(buf[i*8+0]) |
			uint64(buf[i*8+1])<<8 |
			uint64(buf[i*8+2])<<16 |
			uint64(buf[i*8+3])<<24 |
			uint64(buf[i*8+4])<<32 |
			uint64(buf[i*8+5])<<40 |
			uint64(buf[i*8+6])<<48 |
			uint64(buf[i*8+7])<<56)
	}
	return nil
}
