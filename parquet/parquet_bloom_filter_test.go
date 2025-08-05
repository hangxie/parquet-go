package parquet

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_SplitBlockAlgorithm(t *testing.T) {
	sba := NewSplitBlockAlgorithm()
	require.NotNil(t, sba)
}

func Test_BloomFilterAlgorithm(t *testing.T) {
	bfa := NewBloomFilterAlgorithm()
	require.NotNil(t, bfa)

	block := NewSplitBlockAlgorithm()
	bfa.BLOCK = block
	require.Equal(t, block, bfa.GetBLOCK())
	require.True(t, bfa.IsSetBLOCK())

	count := bfa.CountSetFieldsBloomFilterAlgorithm()
	require.Greater(t, count, 0)
}

func Test_XxHash(t *testing.T) {
	xxh := NewXxHash()
	require.NotNil(t, xxh)
}

func Test_BloomFilterHash(t *testing.T) {
	bfh := NewBloomFilterHash()
	require.NotNil(t, bfh)

	xxhash := NewXxHash()
	bfh.XXHASH = xxhash
	require.Equal(t, xxhash, bfh.GetXXHASH())
	require.True(t, bfh.IsSetXXHASH())

	count := bfh.CountSetFieldsBloomFilterHash()
	require.Greater(t, count, 0)
}

func Test_Uncompressed(t *testing.T) {
	uc := NewUncompressed()
	require.NotNil(t, uc)
}

func Test_BloomFilterCompression(t *testing.T) {
	bfc := NewBloomFilterCompression()
	require.NotNil(t, bfc)

	uncompressed := NewUncompressed()
	bfc.UNCOMPRESSED = uncompressed
	require.Equal(t, uncompressed, bfc.GetUNCOMPRESSED())
	require.True(t, bfc.IsSetUNCOMPRESSED())

	count := bfc.CountSetFieldsBloomFilterCompression()
	require.Greater(t, count, 0)
}

func Test_BloomFilterHeader(t *testing.T) {
	bfh := NewBloomFilterHeader()
	require.NotNil(t, bfh)

	numBytes := int32(1024)
	bfh.NumBytes = numBytes
	require.Equal(t, numBytes, bfh.GetNumBytes())

	algorithm := NewBloomFilterAlgorithm()
	bfh.Algorithm = algorithm
	require.Equal(t, algorithm, bfh.GetAlgorithm())
	require.True(t, bfh.IsSetAlgorithm())

	hash := NewBloomFilterHash()
	bfh.Hash = hash
	require.Equal(t, hash, bfh.GetHash())
	require.True(t, bfh.IsSetHash())

	compression := NewBloomFilterCompression()
	bfh.Compression = compression
	require.Equal(t, compression, bfh.GetCompression())
	require.True(t, bfh.IsSetCompression())
}

func Test_BloomFilterHeader_Equals(t *testing.T) {
	bfh1 := NewBloomFilterHeader()
	bfh1.NumBytes = 1024
	algo := NewBloomFilterAlgorithm()
	algo.BLOCK = NewSplitBlockAlgorithm()
	bfh1.Algorithm = algo

	hash := NewBloomFilterHash()
	hash.XXHASH = NewXxHash()
	bfh1.Hash = hash

	compression := NewBloomFilterCompression()
	compression.UNCOMPRESSED = NewUncompressed()
	bfh1.Compression = compression

	bfh2 := NewBloomFilterHeader()
	bfh2.NumBytes = 1024
	algo2 := NewBloomFilterAlgorithm()
	algo2.BLOCK = NewSplitBlockAlgorithm()
	bfh2.Algorithm = algo2

	hash2 := NewBloomFilterHash()
	hash2.XXHASH = NewXxHash()
	bfh2.Hash = hash2

	compression2 := NewBloomFilterCompression()
	compression2.UNCOMPRESSED = NewUncompressed()
	bfh2.Compression = compression2

	assert.True(t, bfh1.Equals(bfh2))
	assert.False(t, bfh1.Equals(nil))

	// Test different values
	bfh3 := NewBloomFilterHeader()
	bfh3.NumBytes = 2048
	assert.False(t, bfh1.Equals(bfh3))
}
