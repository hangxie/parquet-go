package compress

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strconv"
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

	t.Run("accepted levels", func(t *testing.T) {
		// 1 to 9 is what the frame writer took before compression moved to blocks,
		// since lz4.Level1 is 1<<9 and lz4.Level9 is 1<<17.
		require.Equal(t, lz4.Level1, lz4.CompressionLevel(1<<(8+1)))
		require.Equal(t, lz4.Level9, lz4.CompressionLevel(1<<(8+9)))

		input := bytes.Repeat([]byte("level boundary round-trip "), 64)
		for _, tt := range []struct {
			level int
			want  bool
		}{
			{-1, false},
			{0, false},
			{1, true},
			{8, true},
			{9, true},
			{10, false},
		} {
			t.Run(strconv.Itoa(tt.level), func(t *testing.T) {
				c, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_LZ4, tt.level))
				if !tt.want {
					require.Error(t, err)
					require.Contains(t, err.Error(), "invalid lz4 compression level")
					return
				}
				require.NoError(t, err)

				compressed, err := c.Compress(input, parquet.CompressionCodec_LZ4)
				require.NoError(t, err)
				got, err := c.Uncompress(compressed, parquet.CompressionCodec_LZ4)
				require.NoError(t, err)
				require.Equal(t, input, got)
			})
		}
	})

	t.Run("a leveled compressor is reusable", func(t *testing.T) {
		c, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_LZ4, 4))
		require.NoError(t, err)

		input := []byte("repeating data for pool exercise repeating data for pool exercise")

		// Compressors are pooled, so a second call can be handed one that already
		// compressed a block.
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

func TestHadoopLZ4Compress_ChunkFailures(t *testing.T) {
	// A bound-sized destination always compresses, so these are driven with a
	// stub rather than through payloads that cannot reach them.
	tests := []struct {
		name          string
		compressBlock func(src, dst []byte) (int, error)
		wantErr       string
	}{
		{
			name:          "compressor error",
			compressBlock: func(src, dst []byte) (int, error) { return 0, errors.New("no compressor") },
			wantErr:       "no compressor",
		},
		{
			name:          "chunk did not compress",
			compressBlock: func(src, dst []byte) (int, error) { return 0, nil },
			wantErr:       "did not compress",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := hadoopLZ4Compress(tt.compressBlock)([]byte("a chunk to compress"))
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// lz4Frame builds a standard framed LZ4 payload, the framing this library wrote before
// it moved to Hadoop framing, and the one older files carry under the LZ4 codec.
func lz4Frame(t *testing.T, data []byte) []byte {
	t.Helper()

	res := new(bytes.Buffer)
	writer := lz4.NewWriter(res)
	_, err := writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return res.Bytes()
}

func TestCodec_LZ4(t *testing.T) {
	c := DefaultCompressor()
	raw := []byte{1, 2, 3}
	// A block length of three, a chunk length of four, then the raw block.
	hadoop := []byte{0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x4, 0x30, 0x1, 0x2, 0x3}
	// What this library wrote for the same input before the move to Hadoop framing.
	framed := []byte{0x4, 0x22, 0x4d, 0x18, 0x64, 0x70, 0xb9, 0x3, 0x0, 0x0, 0x80, 0x1, 0x2, 0x3, 0x0, 0x0, 0x0, 0x0, 0xc4, 0x78, 0x9c, 0xf5}

	actual, err := c.Compress(raw, parquet.CompressionCodec_LZ4)
	require.NoError(t, err)
	require.Equal(t, hadoop, actual)

	for name, compressed := range map[string][]byte{"hadoop": hadoop, "framed": framed} {
		t.Run(name, func(t *testing.T) {
			uncompressed, err := c.Uncompress(compressed, parquet.CompressionCodec_LZ4)
			require.NoError(t, err)
			require.Equal(t, raw, uncompressed)
		})
	}

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

	t.Run("frame framing still reads", func(t *testing.T) {
		got, err := c.Uncompress(lz4Frame(t, first), parquet.CompressionCodec_LZ4)
		require.NoError(t, err)
		require.Equal(t, first, got)
	})

	t.Run("frame framing reads under a size limit", func(t *testing.T) {
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

				got, err := limited.Uncompress(lz4Frame(t, tc.data), parquet.CompressionCodec_LZ4)
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

func TestHadoopLZ4Uncompress_ParquetMRFixture(t *testing.T) {
	// Produced by org.apache.hadoop.io.compress.Lz4Codec 3.3.6, the codec parquet-mr
	// compresses with for the deprecated LZ4 codec id.
	const fixture = "000000290000002bf01a6861646f6f70206672616d6564206c7a34207061796c6f" +
		"61642066726f6d20706172717565742d6d72"

	payload, err := hex.DecodeString(fixture)
	require.NoError(t, err)

	got, err := DefaultCompressor().Uncompress(payload, parquet.CompressionCodec_LZ4)
	require.NoError(t, err)
	require.Equal(t, []byte("hadoop framed lz4 payload from parquet-mr"), got)
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
	framed := lz4Frame(t, bytes.Repeat([]byte("over the limit "), 4096))

	_, err := lz4Uncompress(framed, 1024)
	require.ErrorIs(t, err, ErrDecompressedSizeExceeded)
	require.NotErrorIs(t, err, errNotHadoopLZ4)
}

// hadoopLZ4Decode decodes buf the way Hadoop's BlockDecompressorStream does, and
// fails the test on anything parquet-mr's decompressor would refuse.
func hadoopLZ4Decode(t *testing.T, buf []byte) ([]byte, []int) {
	t.Helper()

	// What Hadoop's decompressor allocates for a chunk and for its output, spelled
	// out here rather than read from the writer's own constant.
	const hadoopBufferSize = 256 * 1024

	var res []byte
	var chunks []int
	for len(buf) > 0 {
		require.GreaterOrEqual(t, len(buf), 4, "truncated block length")
		blockLen := int(int32(binary.BigEndian.Uint32(buf)))
		buf = buf[4:]
		require.GreaterOrEqual(t, blockLen, 0, "block length must fit a Java int")

		blockStart := len(res)
		for filled := 0; filled < blockLen; {
			require.GreaterOrEqual(t, len(buf), 4, "truncated chunk length")
			chunkLen := int(int32(binary.BigEndian.Uint32(buf)))
			buf = buf[4:]
			require.Positive(t, chunkLen, "chunk length must fit a Java int")
			require.LessOrEqual(t, chunkLen, hadoopBufferSize, "chunk overflows the decompressor's input buffer")
			require.LessOrEqual(t, chunkLen, len(buf), "chunk length past the end of the payload")

			out := make([]byte, hadoopBufferSize)
			count, err := lz4.UncompressBlock(buf[:chunkLen], out)
			require.NoError(t, err, "chunk overflows the decompressor's output buffer")
			res = append(res, out[:count]...)
			chunks = append(chunks, count)
			filled += count
			buf = buf[chunkLen:]
		}
		require.Len(t, res[blockStart:], blockLen, "block length disagrees with its chunks")
	}
	return res, chunks
}

// arrowLZ4Decode decodes buf the way Arrow's Lz4HadoopCodec does, and fails the test
// on anything it would hand to its raw LZ4 fallback.
func arrowLZ4Decode(t *testing.T, buf []byte) []byte {
	t.Helper()

	// Arrow takes the two lengths as a pair describing one chunk, and abandons the
	// framing the moment a chunk decodes to anything but the length beside it.
	var res []byte
	for len(buf) >= 8 {
		decompressedLen := int(binary.BigEndian.Uint32(buf))
		compressedLen := int(binary.BigEndian.Uint32(buf[4:]))
		buf = buf[8:]
		require.LessOrEqual(t, compressedLen, len(buf), "frame runs past the end of the payload")

		out := make([]byte, decompressedLen)
		count, err := lz4.UncompressBlock(buf[:compressedLen], out)
		require.NoError(t, err)
		require.Equal(t, decompressedLen, count, "chunk decodes to something other than its declared size")
		res = append(res, out...)
		buf = buf[compressedLen:]
	}
	require.Empty(t, buf, "trailing bytes leave the payload unrecognized")
	return res
}

func TestCodec_LZ4_WritesHadoopFraming(t *testing.T) {
	leveled, err := NewCompressor(WithCompressionLevel(parquet.CompressionCodec_LZ4, 9))
	require.NoError(t, err)

	compressors := map[string]*Compressor{"default": DefaultCompressor(), "level 9": leveled}

	// One buffer's worth is what Hadoop compresses at a time, so past it the
	// writer has to split the block into chunks the way parquet-mr's does.
	large := bytes.Repeat([]byte("hadoop lz4 interop fixture "), 22223)

	for name, c := range compressors {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name string
				data []byte
				// The uncompressed size of each chunk. Hadoop's own compressor
				// split a payload of the large one's length the same way.
				wantChunks []int
			}{
				{"small", []byte("hadoop framed lz4"), []int{17}},
				{"repetitive", bytes.Repeat([]byte("hadoop framed lz4 "), 512), []int{9216}},
				{"past one codec buffer", large, []int{261100, 261100, 77821}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					compressed, err := c.Compress(tc.data, parquet.CompressionCodec_LZ4)
					require.NoError(t, err)
					require.Equal(t, uint32(tc.wantChunks[0]), binary.BigEndian.Uint32(compressed),
						"payload must open with the first block's uncompressed length")

					got, chunks := hadoopLZ4Decode(t, compressed)
					require.Equal(t, tc.data, got)
					require.Equal(t, tc.wantChunks, chunks)

					require.Equal(t, tc.data, arrowLZ4Decode(t, compressed))

					got, err = c.Uncompress(compressed, parquet.CompressionCodec_LZ4)
					require.NoError(t, err)
					require.Equal(t, tc.data, got)
				})
			}

			t.Run("empty page", func(t *testing.T) {
				// Hadoop leaves a bare zero block length here, which reads back as
				// empty everywhere but Arrow, so nothing is written instead.
				compressed, err := c.Compress(nil, parquet.CompressionCodec_LZ4)
				require.NoError(t, err)
				require.Empty(t, compressed)

				got, err := c.Uncompress(compressed, parquet.CompressionCodec_LZ4)
				require.NoError(t, err)
				require.Empty(t, got)
			})
		})
	}
}
