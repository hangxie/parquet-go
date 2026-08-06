package compress

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"sync"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestLz4CompressionLevel(t *testing.T) {
	t.Run("valid level round-trip", func(t *testing.T) {
		c, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_LZ4, 4))
		require.NoError(t, err)

		input := []byte("test data for lz4 framed level testing, needs enough data to compress")
		compressed, err := c.Compress(input, parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.NotNil(t, compressed)

		output, err := c.Uncompress(compressed, parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.Equal(t, input, output)
	})

	t.Run("invalid level returns error", func(t *testing.T) {
		_, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_LZ4, 10))
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid lz4 compression level")
	})

	t.Run("pool creates writers with correct level", func(t *testing.T) {
		c, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_LZ4, 4))
		require.NoError(t, err)

		input := []byte("repeating data for pool exercise repeating data for pool exercise")

		// Compress multiple times to force the pool to create new writers,
		// exercising the sync.Pool New callback that applies the compression level.
		for range 5 {
			compressed, err := c.Compress(input, parquet.CompressionCodec_LZ4)
			require.NoError(t, err)
			require.NotNil(t, compressed)

			output, err := c.Uncompress(compressed, parquet.CompressionCodec_LZ4)
			require.NoError(t, err)
			require.Equal(t, input, output)
		}
	})
}

func TestLz4CompressWithLevelApplyError(t *testing.T) {
	// Call lz4CompressWithLevel directly with an invalid level to exercise
	// the error-return path that is unreachable through the public API.
	pool := sync.Pool{
		New: func() any {
			return lz4.NewWriter(nil)
		},
	}
	invalidCL := lz4.CompressionLevel(128) // invalid level
	compress := lz4CompressWithLevel(&pool, invalidCL)
	_, err := compress([]byte("test data"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "lz4 compress")
}

func TestCodec_LZ4(t *testing.T) {
	c := DefaultCompressor()
	raw := []byte{1, 2, 3}
	compressed := []byte{0x4, 0x22, 0x4d, 0x18, 0x64, 0x70, 0xb9, 0x3, 0x0, 0x0, 0x80, 0x1, 0x2, 0x3, 0x0, 0x0, 0x0, 0x0, 0xc4, 0x78, 0x9c, 0xf5}

	actual, err := c.Compress(raw, parquet.CompressionCodec_LZ4)
	require.NoError(t, err)
	require.Equal(t, compressed, actual)

	uncompressed, err := c.Uncompress(compressed, parquet.CompressionCodec_LZ4)
	require.NoError(t, err)
	require.Equal(t, raw, uncompressed)

	_, err = c.Uncompress([]byte{0}, parquet.CompressionCodec_LZ4)
	require.Contains(t, err.Error(), "unexpected EOF")
}

// hadoopLZ4Block builds one Hadoop-framed block from the given chunks.
func hadoopLZ4Block(t *testing.T, chunks ...[]byte) []byte {
	t.Helper()

	// The block's total uncompressed length, then every chunk prefixed by its own
	// compressed length. More than one chunk is what Hadoop emits past its buffer.
	var body []byte
	var uncompressedLen int
	for _, chunk := range chunks {
		compressed := make([]byte, lz4.CompressBlockBound(len(chunk)))
		var compressor lz4.Compressor
		n, err := compressor.CompressBlock(chunk, compressed)
		require.NoError(t, err)
		require.NotZero(t, n, "chunk did not compress, test data is too small")

		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, uint32(n))
		body = append(body, header...)
		body = append(body, compressed[:n]...)
		uncompressedLen += len(chunk)
	}

	res := make([]byte, 4)
	binary.BigEndian.PutUint32(res, uint32(uncompressedLen))
	return append(res, body...)
}

func TestCodec_LZ4_HadoopFraming(t *testing.T) {
	c := DefaultCompressor()
	first := bytes.Repeat([]byte("hadoop lz4 chunk one "), 16)
	second := bytes.Repeat([]byte("hadoop lz4 chunk two "), 16)
	both := append(append([]byte(nil), first...), second...)

	t.Run("single chunk block", func(t *testing.T) {
		got, err := c.Uncompress(hadoopLZ4Block(t, first), parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.Equal(t, first, got)
	})

	t.Run("multiple chunks in one block", func(t *testing.T) {
		// What Hadoop emits past its codec buffer: one block length covering
		// chunks that each carry only their own compressed length.
		got, err := c.Uncompress(hadoopLZ4Block(t, first, second), parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.Equal(t, both, got)
	})

	t.Run("multiple blocks concatenate", func(t *testing.T) {
		buf := append(hadoopLZ4Block(t, first), hadoopLZ4Block(t, second)...)

		got, err := c.Uncompress(buf, parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.Equal(t, both, got)
	})

	t.Run("empty block marker", func(t *testing.T) {
		// finish() writes a bare zero length when the compressor buffered nothing.
		got, err := c.Uncompress([]byte{0, 0, 0, 0}, parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.Empty(t, got)

		// BlockDecompressorStream reads a zero length as EOF, so blocks behind one
		// are bytes Hadoop would never return. Decoding them would disagree.
		buf := append([]byte{0, 0, 0, 0}, hadoopLZ4Block(t, first)...)
		_, err = c.Uncompress(buf, parquet.CompressionCodec_LZ4)
		require.Error(t, err)
		require.Contains(t, err.Error(), "after the zero block length")
	})

	t.Run("zero block terminates a stream that carried data", func(t *testing.T) {
		buf := append(hadoopLZ4Block(t, first), 0, 0, 0, 0)
		got, err := c.Uncompress(buf, parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.Equal(t, first, got)
	})

	t.Run("frame framing still round-trips", func(t *testing.T) {
		framed, err := c.Compress(first, parquet.CompressionCodec_LZ4)
		require.NoError(t, err)

		got, err := c.Uncompress(framed, parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.Equal(t, first, got)
	})

	t.Run("frame framing round-trips under a size limit", func(t *testing.T) {
		// Under 69356824, a frame's magic read as a Hadoop block length, where a
		// size error could wrongly skip the frame fallback.
		incompressible := make([]byte, 400_000)
		_, err := rand.Read(incompressible)
		require.NoError(t, err)

		for _, tc := range []struct {
			name string
			data []byte
		}{
			{"tiny", first},
			{"compressible", bytes.Repeat([]byte("large payload "), 20_000)},
			// Big enough to clear the expansion guard, leaving the magic as
			// the only thing that can reject it.
			{"incompressible", incompressible},
		} {
			t.Run(tc.name, func(t *testing.T) {
				limited, err := NewCompressor(WithMaxDecompressedSize(1 << 20))
				require.NoError(t, err)

				framed, err := limited.Compress(tc.data, parquet.CompressionCodec_LZ4)
				require.NoError(t, err)

				got, err := limited.Uncompress(framed, parquet.CompressionCodec_LZ4)
				require.NoError(t, err)
				require.Equal(t, tc.data, got)
			})
		}
	})

	t.Run("block length must match chunk contents", func(t *testing.T) {
		// A block larger than its chunks decode to runs out of payload, and is no
		// valid frame either, so it must fail rather than truncate.
		buf := hadoopLZ4Block(t, first)
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(first)+1))

		_, err := c.Uncompress(buf, parquet.CompressionCodec_LZ4)
		require.Error(t, err)
	})

	t.Run("chunk overrunning its block is rejected", func(t *testing.T) {
		buf := hadoopLZ4Block(t, first)
		binary.BigEndian.PutUint32(buf[0:4], uint32(len(first)-1))

		_, err := c.Uncompress(buf, parquet.CompressionCodec_LZ4)
		require.ErrorIs(t, err, errNotHadoopLZ4)
	})

	t.Run("a corrupt hadoop payload keeps its own diagnosis", func(t *testing.T) {
		// The frame error alone would name the framing this page was never in.
		buf := hadoopLZ4Block(t, first)

		_, err := c.Uncompress(buf[:len(buf)-4], parquet.CompressionCodec_LZ4)
		require.ErrorIs(t, err, errNotHadoopLZ4)
		require.Contains(t, err.Error(), "bad magic number", "frame error should still lead")
	})

	t.Run("empty payload is not hadoop framing", func(t *testing.T) {
		// No bytes at all is a truncated stream. Reaching this needs a direct
		// call, since lz4Uncompress decodes an empty payload as an empty frame.
		_, err := hadoopLZ4Uncompress(nil, 1<<20)
		require.ErrorIs(t, err, errNotHadoopLZ4)

		got, err := DefaultCompressor().Uncompress(nil, parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("size limit is enforced", func(t *testing.T) {
		limited, err := NewCompressor(WithMaxDecompressedSize(int64(len(first)) - 1))
		require.NoError(t, err)

		_, err = limited.Uncompress(hadoopLZ4Block(t, first), parquet.CompressionCodec_LZ4)
		require.ErrorIs(t, err, ErrDecompressedSizeExceeded)
	})
}

func TestHadoopLZ4Uncompress_Rejects(t *testing.T) {
	// These guards bound allocation and settle framing, so they are driven
	// directly rather than through payloads that happen to reach them.
	tests := []struct {
		name string
		buf  []byte
	}{
		{
			// More than the 255x a raw LZ4 block can manage, so no payload
			// could produce it.
			name: "block length beyond the maximum expansion",
			buf:  []byte{0x00, 0x10, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04},
		},
		{
			name: "zero chunk length",
			buf:  []byte{0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02},
		},
		{
			name: "chunk length past the end of the payload",
			buf:  []byte{0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x40, 0x01, 0x02},
		},
		{
			name: "truncated block length",
			buf:  []byte{0x00, 0x00},
		},
		{
			name: "block with no chunk at all",
			buf:  []byte{0x00, 0x00, 0x00, 0x08},
		},
		{
			name: "empty payload",
			buf:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := hadoopLZ4Uncompress(tt.buf, 1<<20)
			require.ErrorIs(t, err, errNotHadoopLZ4)
		})
	}
}

// TestHadoopLZ4Uncompress_NegativeLengths covers length prefixes with the high bit set.
func TestHadoopLZ4Uncompress_NegativeLengths(t *testing.T) {
	// Read unsigned, 0xffffffff becomes 4294967295 rather than Hadoop's -1, and
	// enough trailing bytes clear the expansion guard for it to be allocated.
	tests := []struct {
		name string
		buf  []byte
	}{
		{
			name: "negative block length",
			buf:  []byte{0xff, 0xff, 0xff, 0xff, 0x01, 0x02, 0x03, 0x04},
		},
		{
			name: "block length with only the high bit set",
			buf:  []byte{0x80, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04},
		},
		{
			name: "negative chunk length",
			buf:  []byte{0x00, 0x00, 0x00, 0x08, 0xff, 0xff, 0xff, 0xff, 0x01, 0x02},
		},
		{
			name: "chunk length with only the high bit set",
			buf:  []byte{0x00, 0x00, 0x00, 0x08, 0x80, 0x00, 0x00, 0x00, 0x01, 0x02},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// A disabled limit leaves the length as the only bound on the
			// allocation, so both are exercised.
			for _, maxSize := range []int64{1 << 20, 0} {
				_, err := hadoopLZ4Uncompress(tt.buf, maxSize)
				require.ErrorIs(t, err, errNotHadoopLZ4)
				require.Contains(t, err.Error(), "negative")
			}
		})
	}
}

func TestLZ4Uncompress_FrameSizeLimitSkipsHadoop(t *testing.T) {
	// A frame that breaches the limit must report that, not be handed to the
	// Hadoop decoder to reinterpret into a different complaint.
	c := DefaultCompressor()
	framed, err := c.Compress(bytes.Repeat([]byte("over the limit "), 4096), parquet.CompressionCodec_LZ4)
	require.NoError(t, err)

	_, err = lz4Uncompress(framed, 1024)
	require.ErrorIs(t, err, ErrDecompressedSizeExceeded)
	require.NotErrorIs(t, err, errNotHadoopLZ4)
}
