// Package bloomfilter implements the Split Block Bloom Filter as specified
// in the Apache Parquet format specification.
package bloomfilter

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"

	"github.com/hangxie/parquet-go/v2/parquet"
)

const (
	// blockSize is 32 bytes (8 x uint32 words) per the Parquet spec.
	blockSize = 32
	// minBytes is the minimum bloom filter size (1 block).
	minBytes = blockSize
	// maxBytes is the maximum bloom filter size (128 MB).
	maxBytes = 128 * 1024 * 1024
	// DefaultNumBytes is the default bloom filter size in bytes.
	DefaultNumBytes = 1024
)

// SALT constants used by the Split Block Bloom Filter algorithm.
var salt = [8]uint32{
	0x47b6137b,
	0x44974d91,
	0x8824ad5b,
	0xa2b7289d,
	0x705495c7,
	0x2df1424b,
	0x9efc4947,
	0x5c6bfb31,
}

// Filter is a Split Block Bloom Filter.
type Filter struct {
	bitset    []byte
	numBlocks uint32
}

// New creates a new bloom filter with the given number of bytes.
// The size is rounded up to the nearest power of 2 and clamped to [32, 128MB].
func New(numBytes int) *Filter {
	if numBytes < minBytes {
		numBytes = minBytes
	}
	if numBytes > maxBytes {
		numBytes = maxBytes
	}
	// Round up to next power of 2
	numBytes = int(nextPowerOf2(uint32(numBytes)))
	return &Filter{
		bitset:    make([]byte, numBytes),
		numBlocks: uint32(numBytes / blockSize),
	}
}

// optimalNumBytes computes the optimal bloom filter size in bytes for the
// given number of distinct values and desired false positive probability.
func optimalNumBytes(numDistinct int, fpp float64) int {
	if numDistinct <= 0 || fpp <= 0 || fpp >= 1 {
		return 0
	}
	// m = -n * ln(fpp) / (ln2)^2  (in bits)
	m := -float64(numDistinct) * math.Log(fpp) / (math.Ln2 * math.Ln2)
	numBytes := int(math.Ceil(m / 8.0))
	return max(numBytes, minBytes)
}

// Insert adds a hash value to the bloom filter.
func (f *Filter) Insert(hash uint64) {
	blockIndex := f.blockIndex(hash)
	block := f.getBlock(blockIndex)
	key := uint32(hash)
	for i := range 8 {
		mask := mask(key, salt[i])
		block[i] |= mask
	}
	f.putBlock(blockIndex, block)
}

// Check tests whether a hash value might be in the bloom filter.
// Returns true if the value might be present, false if it is definitely not present.
func (f *Filter) Check(hash uint64) bool {
	blockIndex := f.blockIndex(hash)
	block := f.getBlock(blockIndex)
	key := uint32(hash)
	for i := range 8 {
		mask := mask(key, salt[i])
		if block[i]&mask == 0 {
			return false
		}
	}
	return true
}

// Bitset returns the raw bitset bytes.
func (f *Filter) Bitset() []byte {
	return f.bitset
}

// NumBytes returns the size of the bitset in bytes.
func (f *Filter) NumBytes() int32 {
	return int32(len(f.bitset))
}

// Header creates a BloomFilterHeader for serialization.
func (f *Filter) Header() *parquet.BloomFilterHeader {
	return &parquet.BloomFilterHeader{
		NumBytes: f.NumBytes(),
		Algorithm: &parquet.BloomFilterAlgorithm{
			BLOCK: parquet.NewSplitBlockAlgorithm(),
		},
		Hash: &parquet.BloomFilterHash{
			XXHASH: parquet.NewXxHash(),
		},
		Compression: &parquet.BloomFilterCompression{
			UNCOMPRESSED: parquet.NewUncompressed(),
		},
	}
}

// FromBitset reconstructs a bloom filter from raw bitset bytes.
func FromBitset(data []byte) (*Filter, error) {
	n := len(data)
	if n < minBytes {
		return nil, fmt.Errorf("bloom filter bitset too small: %d bytes (minimum %d)", n, minBytes)
	}
	if n%blockSize != 0 {
		return nil, fmt.Errorf("bloom filter bitset size %d is not a multiple of block size %d", n, blockSize)
	}
	if !isPowerOf2(uint32(n)) {
		return nil, fmt.Errorf("bloom filter bitset size %d is not a power of 2", n)
	}
	bitset := make([]byte, n)
	copy(bitset, data)
	return &Filter{
		bitset:    bitset,
		numBlocks: uint32(n / blockSize),
	}, nil
}

// blockIndex computes the block index from the upper 32 bits of the hash.
func (f *Filter) blockIndex(hash uint64) uint32 {
	return uint32(((hash >> 32) * uint64(f.numBlocks)) >> 32)
}

// getBlock reads a 32-byte block as 8 uint32 words.
func (f *Filter) getBlock(blockIndex uint32) [8]uint32 {
	var block [8]uint32
	offset := blockIndex * blockSize
	for i := range 8 {
		block[i] = binary.LittleEndian.Uint32(f.bitset[offset+uint32(i)*4:])
	}
	return block
}

// putBlock writes 8 uint32 words back as a 32-byte block.
func (f *Filter) putBlock(blockIndex uint32, block [8]uint32) {
	offset := blockIndex * blockSize
	for i := range 8 {
		binary.LittleEndian.PutUint32(f.bitset[offset+uint32(i)*4:], block[i])
	}
}

// mask computes a single-bit mask from a key and salt value.
func mask(key, salt uint32) uint32 {
	return 1 << ((key * salt) >> 27)
}

// nextPowerOf2 rounds up to the next power of 2.
func nextPowerOf2(v uint32) uint32 {
	if v == 0 {
		return 1
	}
	if isPowerOf2(v) {
		return v
	}
	return 1 << bits.Len32(v)
}

// isPowerOf2 checks if a value is a power of 2.
func isPowerOf2(v uint32) bool {
	return v > 0 && (v&(v-1)) == 0
}
