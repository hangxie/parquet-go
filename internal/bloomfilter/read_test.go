package bloomfilter

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net/http"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// serializeBloomFilter creates the on-disk bytes for a bloom filter (header + bitset).
func serializeBloomFilter(f *Filter) ([]byte, error) {
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	headerBuf, err := ts.Write(context.TODO(), f.Header())
	if err != nil {
		return nil, err
	}
	return append(headerBuf, f.Bitset()...), nil
}

func TestReadBloomFilter(t *testing.T) {
	t.Run("round-trip", func(t *testing.T) {
		original := New(1024)
		original.Insert(42)
		original.Insert(99)

		data, err := serializeBloomFilter(original)
		require.NoError(t, err)

		r := bytes.NewReader(data)
		restored, err := ReadBloomFilter(r, 0)
		require.NoError(t, err)
		require.True(t, restored.Check(42))
		require.True(t, restored.Check(99))
		require.Equal(t, original.NumBytes(), restored.NumBytes())
	})

	t.Run("non-zero-offset", func(t *testing.T) {
		original := New(64)
		original.Insert(7)

		data, err := serializeBloomFilter(original)
		require.NoError(t, err)

		// Pad with garbage before the bloom filter data
		padding := make([]byte, 100)
		for i := range padding {
			padding[i] = 0xFF
		}
		full := append(padding, data...)

		r := bytes.NewReader(full)
		restored, err := ReadBloomFilter(r, 100)
		require.NoError(t, err)
		require.True(t, restored.Check(7))
	})

	t.Run("seek-error", func(t *testing.T) {
		r := bytes.NewReader([]byte{})
		// Offset beyond the reader's range; Seek itself doesn't error on bytes.Reader,
		// but reading will fail. Use a custom ReadSeeker that fails on Seek.
		_, err := ReadBloomFilter(&failSeeker{}, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "seek to bloom filter offset")
		_ = r
	})

	t.Run("invalid-header", func(t *testing.T) {
		// Garbage data that cannot be parsed as a Thrift-encoded BloomFilterHeader
		data := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
		r := bytes.NewReader(data)
		_, err := ReadBloomFilter(r, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "read bloom filter header")
	})

	t.Run("zero-num-bytes", func(t *testing.T) {
		// Serialize a header with NumBytes = 0
		header := &parquet.BloomFilterHeader{
			NumBytes: 0,
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
		ts := thrift.NewTSerializer()
		ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
		data, err := ts.Write(context.TODO(), header)
		require.NoError(t, err)

		r := bytes.NewReader(data)
		_, err = ReadBloomFilter(r, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid bloom filter header: numBytes=0")
	})

	t.Run("negative-num-bytes", func(t *testing.T) {
		header := &parquet.BloomFilterHeader{
			NumBytes: -1,
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
		ts := thrift.NewTSerializer()
		ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
		data, err := ts.Write(context.TODO(), header)
		require.NoError(t, err)

		r := bytes.NewReader(data)
		_, err = ReadBloomFilter(r, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid bloom filter header: numBytes=-1")
	})

	t.Run("truncated-bitset", func(t *testing.T) {
		// Create a valid header claiming 1024 bytes but provide only a few bitset bytes
		original := New(1024)
		data, err := serializeBloomFilter(original)
		require.NoError(t, err)

		// Truncate the data to include header + only 10 bytes of bitset
		ts := thrift.NewTSerializer()
		ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
		headerBuf, err := ts.Write(context.TODO(), original.Header())
		require.NoError(t, err)
		truncated := data[:len(headerBuf)+10]

		r := bytes.NewReader(truncated)
		_, err = ReadBloomFilter(r, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "read bloom filter bitset")
	})

	t.Run("seek-to-bitset-error", func(t *testing.T) {
		// Use a reader that allows the initial seek and header read but fails on the second seek
		original := New(32)
		data, err := serializeBloomFilter(original)
		require.NoError(t, err)

		r := &failSecondSeek{Reader: bytes.NewReader(data)}
		_, err = ReadBloomFilter(r, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "seek to bloom filter bitset")
	})
}

func TestReadEncryptedBloomFilter(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	opt := ReadOptions{
		Key:             key,
		AADPrefix:       []byte("prefix"),
		AADFileUnique:   []byte("file-unique"),
		RowGroupOrdinal: 2,
		ColumnOrdinal:   3,
	}
	original := New(64)
	original.Insert(7)
	headerBuf, err := serializeBloomFilterHeader(original.Header())
	require.NoError(t, err)

	var data []byte
	data = append(data, encryptBloomModule(t, key, bloomAAD(opt, encryption.ModuleBloomFilterHeader), headerBuf)...)
	data = append(data, encryptBloomModule(t, key, bloomAAD(opt, encryption.ModuleBloomFilterBitset), original.Bitset())...)

	restored, err := ReadEncryptedBloomFilter(bytes.NewReader(data), 0, opt)
	require.NoError(t, err)
	require.True(t, restored.Check(7))
	require.Equal(t, original.NumBytes(), restored.NumBytes())

	badOpt := opt
	badOpt.AADPrefix = []byte("wrong")
	_, err = ReadEncryptedBloomFilter(bytes.NewReader(data), 0, badOpt)
	require.ErrorContains(t, err, "decrypt bloom filter header")
}

func TestReadEncryptedBloomFilterErrors(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	opt := ReadOptions{
		Key:             key,
		AADPrefix:       []byte("prefix"),
		AADFileUnique:   []byte("file-unique"),
		RowGroupOrdinal: 2,
		ColumnOrdinal:   3,
	}
	filter := New(64)
	headerBuf, err := serializeBloomFilterHeader(filter.Header())
	require.NoError(t, err)
	headerModule := encryptBloomModule(t, key, bloomAAD(opt, encryption.ModuleBloomFilterHeader), headerBuf)
	bitsetModule := encryptBloomModule(t, key, bloomAAD(opt, encryption.ModuleBloomFilterBitset), filter.Bitset())

	_, err = ReadEncryptedBloomFilter(&failSeeker{}, 0, opt)
	require.ErrorContains(t, err, "seek to bloom filter offset")

	_, err = ReadEncryptedBloomFilter(bytes.NewReader([]byte{1, 2}), 0, opt)
	require.ErrorContains(t, err, "read encrypted bloom filter header")

	invalidHeader := encryptBloomModule(t, key, bloomAAD(opt, encryption.ModuleBloomFilterHeader), []byte{0xff})
	_, err = ReadEncryptedBloomFilter(bytes.NewReader(invalidHeader), 0, opt)
	require.ErrorContains(t, err, "read bloom filter header")

	zeroHeader := &parquet.BloomFilterHeader{
		NumBytes:    0,
		Algorithm:   &parquet.BloomFilterAlgorithm{BLOCK: parquet.NewSplitBlockAlgorithm()},
		Hash:        &parquet.BloomFilterHash{XXHASH: parquet.NewXxHash()},
		Compression: &parquet.BloomFilterCompression{UNCOMPRESSED: parquet.NewUncompressed()},
	}
	zeroHeaderBuf, err := serializeBloomFilterHeader(zeroHeader)
	require.NoError(t, err)
	zeroHeaderModule := encryptBloomModule(t, key, bloomAAD(opt, encryption.ModuleBloomFilterHeader), zeroHeaderBuf)
	_, err = ReadEncryptedBloomFilter(bytes.NewReader(zeroHeaderModule), 0, opt)
	require.ErrorContains(t, err, "invalid bloom filter header")

	_, err = ReadEncryptedBloomFilter(bytes.NewReader(headerModule), 0, opt)
	require.ErrorContains(t, err, "read encrypted bloom filter bitset")

	wrongBitsetModule := encryptBloomModule(t, key, []byte("wrong aad"), filter.Bitset())
	_, err = ReadEncryptedBloomFilter(bytes.NewReader(append(append([]byte{}, headerModule...), wrongBitsetModule...)), 0, opt)
	require.ErrorContains(t, err, "decrypt bloom filter bitset")

	shortBitsetModule := encryptBloomModule(t, key, bloomAAD(opt, encryption.ModuleBloomFilterBitset), filter.Bitset()[:1])
	_, err = ReadEncryptedBloomFilter(bytes.NewReader(append(append([]byte{}, headerModule...), shortBitsetModule...)), 0, opt)
	require.ErrorContains(t, err, "does not match header numBytes")

	_, err = ReadEncryptedBloomFilter(bytes.NewReader(append(append([]byte{}, headerModule...), bitsetModule...)), 0, opt)
	require.NoError(t, err)
}

// headerWithExtensionField serializes a header carrying an unknown field, spliced in ahead of
// the struct's STOP marker so a decoder has to skip it rather than stop before it.
func headerWithExtensionField(t *testing.T, header *parquet.BloomFilterHeader, payload []byte) []byte {
	t.Helper()
	buf, err := serializeBloomFilterHeader(header)
	require.NoError(t, err)
	require.Equal(t, byte(thrift.STOP), buf[len(buf)-1])

	mem := thrift.NewTMemoryBuffer()
	proto := thrift.NewTCompactProtocolConf(mem, &thrift.TConfiguration{})
	require.NoError(t, proto.WriteFieldBegin(context.TODO(), "extension", thrift.STRING, 32767))
	require.NoError(t, proto.WriteBinary(context.TODO(), payload))
	require.NoError(t, proto.WriteFieldEnd(context.TODO()))
	require.NoError(t, proto.Flush(context.TODO()))

	return append(append(buf[:len(buf)-1], mem.Bytes()...), byte(thrift.STOP))
}

func serializeBloomFilterHeader(header *parquet.BloomFilterHeader) ([]byte, error) {
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	return ts.Write(context.TODO(), header)
}

func encryptBloomModule(t *testing.T, key, aad, plaintext []byte) []byte {
	t.Helper()
	nonce := []byte("123456789012")
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	gcm, err := cipher.NewGCMWithNonceSize(block, len(nonce))
	require.NoError(t, err)
	body := append(append([]byte{}, nonce...), gcm.Seal(nil, nonce, plaintext, aad)...)
	var length [4]byte
	binary.LittleEndian.PutUint32(length[:], uint32(len(body)))
	return append(length[:], body...)
}

// TestReadBloomFilterInterop tests that our ReadBloomFilter and HashValue functions
// are compatible with bloom filters generated by parquet-mr (Java reference implementation).
// The bloom_filter.xxhash.bin file from apache/parquet-testing was generated by inserting
// the strings "hello", "parquet", "bloom", "filter".
func TestReadBloomFilterInterop(t *testing.T) {
	bloomBinURL := "https://github.com/apache/parquet-testing/raw/refs/heads/master/data/bloom_filter.xxhash.bin"

	resp, err := http.Get(bloomBinURL) //nolint:gosec // test URL is a constant
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	r := bytes.NewReader(data)
	filter, err := ReadBloomFilter(r, 0)
	require.NoError(t, err)
	require.Greater(t, filter.NumBytes(), int32(0))

	// Verify our hash function produces hashes compatible with parquet-mr.
	// These four strings were inserted into the bloom filter by parquet-mr.
	for _, val := range []string{"hello", "parquet", "bloom", "filter"} {
		hash, err := HashValue(val, parquet.Type_BYTE_ARRAY)
		require.NoError(t, err)
		require.True(t, filter.Check(hash))
	}
}

func TestLimitToAvailable(t *testing.T) {
	t.Parallel()

	r := bytes.NewReader(make([]byte, 100))
	_, err := r.Seek(40, io.SeekStart)
	require.NoError(t, err)

	// Bounded by the bytes left after the current position, which must be restored.
	require.Equal(t, int64(60), limitToAvailable(r, 1000))
	pos, err := r.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(40), pos)

	// A caller limit below what remains still wins.
	require.Equal(t, int64(10), limitToAvailable(r, 10))

	// Readers that cannot report a length keep the caller's limit, whichever seek fails.
	require.Equal(t, int64(10), limitToAvailable(&failSeeker{}, 10))
	require.Equal(t, int64(10), limitToAvailable(&failNthSeek{Reader: bytes.NewReader(make([]byte, 100)), n: 2}, 10))
	require.Equal(t, int64(10), limitToAvailable(&failNthSeek{Reader: bytes.NewReader(make([]byte, 100)), n: 3}, 10))
}

// hugeFile reports a length far beyond the bytes it holds, so file-derived bounds do not apply.
type hugeFile struct {
	*bytes.Reader
	size int64
}

func (h *hugeFile) Seek(offset int64, whence int) (int64, error) {
	if whence == io.SeekEnd {
		return h.size, nil
	}
	return h.Reader.Seek(offset, whence)
}

// failNthSeek fails on the nth Seek call and passes the others through.
type failNthSeek struct {
	*bytes.Reader
	n     int
	calls int
}

func (f *failNthSeek) Seek(offset int64, whence int) (int64, error) {
	f.calls++
	if f.calls == f.n {
		return 0, io.ErrClosedPipe
	}
	return f.Reader.Seek(offset, whence)
}

// failSeeker is a ReadSeeker that always fails on Seek.
type failSeeker struct{}

func (f *failSeeker) Read(p []byte) (int, error) {
	return 0, io.EOF
}

func (f *failSeeker) Seek(offset int64, whence int) (int64, error) {
	return 0, io.ErrClosedPipe
}

// failSecondSeek allows the first Seek but fails on the second.
type failSecondSeek struct {
	*bytes.Reader
	seekCount int
}

func (f *failSecondSeek) Seek(offset int64, whence int) (int64, error) {
	f.seekCount++
	if f.seekCount >= 2 {
		return 0, io.ErrClosedPipe
	}
	return f.Reader.Seek(offset, whence)
}

func TestReadBloomFilterSize(t *testing.T) {
	t.Parallel()

	newHeader := func(numBytes int32) *parquet.BloomFilterHeader {
		return &parquet.BloomFilterHeader{
			NumBytes:    numBytes,
			Algorithm:   &parquet.BloomFilterAlgorithm{BLOCK: parquet.NewSplitBlockAlgorithm()},
			Hash:        &parquet.BloomFilterHash{XXHASH: parquet.NewXxHash()},
			Compression: &parquet.BloomFilterCompression{UNCOMPRESSED: parquet.NewUncompressed()},
		}
	}

	t.Run("header-only", func(t *testing.T) {
		// No bitset follows the header: the size must come from the header alone.
		headerBuf, err := serializeBloomFilterHeader(New(1024).Header())
		require.NoError(t, err)

		size, err := ReadBloomFilterSize(context.Background(), bytes.NewReader(headerBuf), 0)
		require.NoError(t, err)
		require.Equal(t, int32(1024), size)
	})

	t.Run("matches-full-read", func(t *testing.T) {
		data, err := serializeBloomFilter(New(64))
		require.NoError(t, err)

		filter, err := ReadBloomFilter(bytes.NewReader(data), 0)
		require.NoError(t, err)
		size, err := ReadBloomFilterSize(context.Background(), bytes.NewReader(data), 0)
		require.NoError(t, err)
		require.Equal(t, filter.NumBytes(), size)
	})

	t.Run("matches-full-read-non-power-of-2", func(t *testing.T) {
		threeBlocks, err := FromBitset(make([]byte, 96))
		require.NoError(t, err)
		data, err := serializeBloomFilter(threeBlocks)
		require.NoError(t, err)

		filter, err := ReadBloomFilter(bytes.NewReader(data), 0)
		require.NoError(t, err)
		size, err := ReadBloomFilterSize(context.Background(), bytes.NewReader(data), 0)
		require.NoError(t, err)
		require.Equal(t, int32(96), filter.NumBytes())
		require.Equal(t, filter.NumBytes(), size)
	})

	t.Run("non-zero-offset", func(t *testing.T) {
		headerBuf, err := serializeBloomFilterHeader(New(256).Header())
		require.NoError(t, err)
		padded := append(bytes.Repeat([]byte{0xFF}, 100), headerBuf...)

		size, err := ReadBloomFilterSize(context.Background(), bytes.NewReader(padded), 100)
		require.NoError(t, err)
		require.Equal(t, int32(256), size)
	})

	t.Run("seek-error", func(t *testing.T) {
		_, err := ReadBloomFilterSize(context.Background(), &failSeeker{}, 0)
		require.ErrorContains(t, err, "seek to bloom filter offset")
	})

	t.Run("invalid-header", func(t *testing.T) {
		_, err := ReadBloomFilterSize(context.Background(), bytes.NewReader([]byte{0xFF, 0xFF, 0xFF, 0xFF}), 0)
		require.ErrorContains(t, err, "read bloom filter header")
	})

	t.Run("zero-num-bytes", func(t *testing.T) {
		headerBuf, err := serializeBloomFilterHeader(newHeader(0))
		require.NoError(t, err)

		_, err = ReadBloomFilterSize(context.Background(), bytes.NewReader(headerBuf), 0)
		require.ErrorContains(t, err, "invalid bloom filter header: numBytes=0")
	})

	t.Run("block-count-not-power-of-2", func(t *testing.T) {
		// 96 = 3*32 is a valid filter size; only writers round up to a power of 2.
		headerBuf, err := serializeBloomFilterHeader(newHeader(96))
		require.NoError(t, err)

		size, err := ReadBloomFilterSize(context.Background(), bytes.NewReader(headerBuf), 0)
		require.NoError(t, err)
		require.Equal(t, int32(96), size)
	})

	t.Run("size-not-block-aligned", func(t *testing.T) {
		headerBuf, err := serializeBloomFilterHeader(newHeader(100))
		require.NoError(t, err)

		_, err = ReadBloomFilterSize(context.Background(), bytes.NewReader(headerBuf), 0)
		require.ErrorContains(t, err, "is not a multiple of block size")
	})

	t.Run("size-below-minimum", func(t *testing.T) {
		headerBuf, err := serializeBloomFilterHeader(newHeader(16))
		require.NoError(t, err)

		_, err = ReadBloomFilterSize(context.Background(), bytes.NewReader(headerBuf), 0)
		require.ErrorContains(t, err, "too small")
	})
}

func TestReadEncryptedBloomFilterSize(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	opt := ReadOptions{
		Key:             key,
		AADPrefix:       []byte("prefix"),
		AADFileUnique:   []byte("file-unique"),
		RowGroupOrdinal: 2,
		ColumnOrdinal:   3,
	}
	headerBuf, err := serializeBloomFilterHeader(New(64).Header())
	require.NoError(t, err)
	headerModule := encryptBloomModule(t, key, bloomAAD(opt, encryption.ModuleBloomFilterHeader), headerBuf)

	// The encrypted bitset module is absent: only the header is decrypted and read.
	size, err := ReadEncryptedBloomFilterSize(bytes.NewReader(headerModule), 0, opt)
	require.NoError(t, err)
	require.Equal(t, int32(64), size)

	badOpt := opt
	badOpt.AADPrefix = []byte("wrong")
	_, err = ReadEncryptedBloomFilterSize(bytes.NewReader(headerModule), 0, badOpt)
	require.ErrorContains(t, err, "decrypt bloom filter header")

	_, err = ReadEncryptedBloomFilterSize(&failSeeker{}, 0, opt)
	require.ErrorContains(t, err, "seek to bloom filter offset")

	_, err = ReadEncryptedBloomFilterSize(bytes.NewReader([]byte{1, 2}), 0, opt)
	require.ErrorContains(t, err, "read encrypted bloom filter header")

	// A hostile length prefix must be rejected before anything is allocated.
	var oversized [4]byte
	binary.LittleEndian.PutUint32(oversized[:], 1<<31)
	_, err = ReadEncryptedBloomFilterSize(bytes.NewReader(oversized[:]), 0, opt)
	require.ErrorContains(t, err, "exceeds limit")
	_, err = ReadEncryptedBloomFilter(bytes.NewReader(oversized[:]), 0, opt)
	require.ErrorContains(t, err, "exceeds limit")

	// The bitset module is bounded by the size the decrypted header declares.
	binary.LittleEndian.PutUint32(oversized[:], 1<<30)
	oversizedBitset := append(append([]byte{}, headerModule...), oversized[:]...)
	_, err = ReadEncryptedBloomFilter(bytes.NewReader(oversizedBitset), 0, opt)
	require.ErrorContains(t, err, "exceeds limit")

	// An unknown field the decoder must skip, rather than trailing bytes, must not trip the bound.
	extendedHeader := headerWithExtensionField(t, New(64).Header(), bytes.Repeat([]byte{0x7f}, 2000))
	decoded, err := readBloomFilterHeader(context.TODO(), bytes.NewReader(extendedHeader))
	require.NoError(t, err, "decoder must skip the unknown field and reach STOP")
	require.Equal(t, int32(64), decoded.NumBytes)

	extendedModule := encryptBloomModule(t, key, bloomAAD(opt, encryption.ModuleBloomFilterHeader), extendedHeader)
	require.Greater(t, len(extendedModule), 1024)
	size, err = ReadEncryptedBloomFilterSize(bytes.NewReader(extendedModule), 0, opt)
	require.NoError(t, err)
	require.Equal(t, int32(64), size)

	// A caller that knows the filter's stored length can bound the module tighter.
	tightOpt := opt
	tightOpt.MaxHeaderModuleSize = 64
	_, err = ReadEncryptedBloomFilterSize(bytes.NewReader(extendedModule), 0, tightOpt)
	require.ErrorContains(t, err, "exceeds limit")

	// A hostile stored length must not raise the ceiling, only lower it.
	var hugeClaim [4]byte
	binary.LittleEndian.PutUint32(hugeClaim[:], uint32(maxHeaderModuleBytes)+1)
	hostileOpt := opt
	hostileOpt.MaxHeaderModuleSize = math.MaxInt32
	_, err = ReadEncryptedBloomFilterSize(&hugeFile{Reader: bytes.NewReader(hugeClaim[:]), size: 1 << 40}, 0, hostileOpt)
	require.ErrorContains(t, err, fmt.Sprintf("exceeds limit %d", maxHeaderModuleBytes))

	badSizeHeader := &parquet.BloomFilterHeader{
		NumBytes:    100,
		Algorithm:   &parquet.BloomFilterAlgorithm{BLOCK: parquet.NewSplitBlockAlgorithm()},
		Hash:        &parquet.BloomFilterHash{XXHASH: parquet.NewXxHash()},
		Compression: &parquet.BloomFilterCompression{UNCOMPRESSED: parquet.NewUncompressed()},
	}
	badSizeBuf, err := serializeBloomFilterHeader(badSizeHeader)
	require.NoError(t, err)
	badSizeModule := encryptBloomModule(t, key, bloomAAD(opt, encryption.ModuleBloomFilterHeader), badSizeBuf)
	_, err = ReadEncryptedBloomFilterSize(bytes.NewReader(badSizeModule), 0, opt)
	require.ErrorContains(t, err, "is not a multiple of block size")
}
