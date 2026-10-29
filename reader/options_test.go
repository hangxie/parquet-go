package reader

import (
	"bytes"
	"context"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/writer"
)

func TestApplyReaderDefaults_Defaults(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{}
	require.NoError(t, applyReaderDefaults(pr, nil))
	require.Equal(t, int64(4), pr.np)
	require.False(t, pr.caseInsensitive)
}

func TestApplyReaderDefaults_AppliesOptions(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{}
	opts := []ReaderOption{
		WithNP(8),
		WithCaseInsensitive(true),
		WithCRCMode(common.CRCStrict),
		WithFooterKey([]byte("footer-key")),
		WithAADPrefix([]byte("aad")),
		WithColumnKey(common.PathToStr([]string{"a", "b"}), []byte("col-key")),
	}
	require.NoError(t, applyReaderDefaults(pr, opts))

	require.Equal(t, int64(8), pr.np)
	require.True(t, pr.caseInsensitive)
	require.Equal(t, common.CRCStrict, pr.crcMode)
	require.Equal(t, []byte("footer-key"), pr.footerKey)
	require.Equal(t, []byte("aad"), pr.aadPrefix)
	require.Equal(t, []byte("col-key"), pr.columnKeys[common.PathToStr([]string{"a", "b"})])
}

func TestApplyReaderDefaults_InvalidNP(t *testing.T) {
	t.Parallel()

	for _, np := range []int64{0, -1} {
		pr := &ParquetReader{}
		err := applyReaderDefaults(pr, []ReaderOption{WithNP(np)})
		require.Error(t, err)
		require.Contains(t, err.Error(), "WithNP: value must be positive")
	}
}

func TestApplyReaderDefaults_InvalidCRCMode(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{}
	err := applyReaderDefaults(pr, []ReaderOption{WithCRCMode(common.CRCMode(99))})
	require.Error(t, err)
	require.Contains(t, err.Error(), "WithCRCMode: unsupported mode")
}

func TestApplyReaderDefaults_ValidCRCModes(t *testing.T) {
	t.Parallel()

	for _, mode := range []common.CRCMode{common.CRCIgnore, common.CRCAuto, common.CRCStrict} {
		pr := &ParquetReader{}
		require.NoError(t, applyReaderDefaults(pr, []ReaderOption{WithCRCMode(mode)}))
		require.Equal(t, mode, pr.crcMode)
	}
}

func TestWithKeyRetriever(t *testing.T) {
	t.Parallel()

	called := false
	retriever := func(keyMetadata []byte) ([]byte, error) {
		called = true
		return []byte("resolved"), nil
	}

	pr := &ParquetReader{}
	require.NoError(t, applyReaderDefaults(pr, []ReaderOption{WithKeyRetriever(retriever)}))
	require.NotNil(t, pr.keyRetriever)

	key, err := pr.keyRetriever([]byte("meta"))
	require.NoError(t, err)
	require.Equal(t, []byte("resolved"), key)
	require.True(t, called)
}

func TestWithColumnKey_MultipleColumns(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{}
	opts := []ReaderOption{
		WithColumnKey(common.PathToStr([]string{"a", "b"}), []byte("key1")),
		WithColumnKey(common.PathToStr([]string{"c", "d"}), []byte("key2")),
	}
	require.NoError(t, applyReaderDefaults(pr, opts))
	require.Len(t, pr.columnKeys, 2)
	require.Equal(t, []byte("key1"), pr.columnKeys[common.PathToStr([]string{"a", "b"})])
	require.Equal(t, []byte("key2"), pr.columnKeys[common.PathToStr([]string{"c", "d"})])
}

func TestWithFooterKey_CopiesInput(t *testing.T) {
	t.Parallel()

	key := []byte("secret")
	pr := &ParquetReader{}
	require.NoError(t, applyReaderDefaults(pr, []ReaderOption{WithFooterKey(key)}))

	// Mutating the caller's slice must not affect the stored key.
	key[0] = 'X'
	require.Equal(t, []byte("secret"), pr.footerKey)
}

func TestApplyReaderDefaults_SizeLimits(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		opt  func(int64) ReaderOption
		get  func(*ParquetReader) int64
		def  int64
	}{
		{"WithMaxPageSize", WithMaxPageSize, func(pr *ParquetReader) int64 { return pr.maxPageSize }, layout.DefaultMaxPageSize},
		{"WithMaxDecompressedSize", WithMaxDecompressedSize, func(pr *ParquetReader) int64 { return pr.maxDecompressedSize }, compress.DefaultMaxDecompressedSize},
		{"WithMaxDecodedBatchBytes", WithMaxDecodedBatchBytes, func(pr *ParquetReader) int64 { return pr.maxDecodedBatchBytes }, layout.DefaultMaxDecodedBatchBytes},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pr := &ParquetReader{}
			require.NoError(t, applyReaderDefaults(pr, nil))
			require.Equal(t, tt.def, tt.get(pr))
			for _, v := range []int64{-1, 1, 1 << 40} {
				pr := &ParquetReader{}
				require.NoError(t, applyReaderDefaults(pr, []ReaderOption{tt.opt(v)}))
				require.Equal(t, v, tt.get(pr))
			}
			for _, v := range []int64{0, -2, math.MinInt64} {
				err := applyReaderDefaults(&ParquetReader{}, []ReaderOption{tt.opt(v)})
				require.ErrorContains(t, err, tt.name+": value must be positive or -1")
			}
		})
	}
}

func TestApplyReaderDefaults_DecompressedSizeCompressor(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{}
	require.NoError(t, applyReaderDefaults(pr, nil))
	require.Nil(t, pr.compressor)

	for v, want := range map[int64]int64{-1: -1, 1024: 1024} {
		pr := &ParquetReader{}
		require.NoError(t, applyReaderDefaults(pr, []ReaderOption{WithMaxDecompressedSize(v)}))
		require.Equal(t, want, pr.compressor.MaxDecompressedSize())
	}
}

type sizeLimitRecord struct {
	Delta string `parquet:"name=delta, type=BYTE_ARRAY, convertedtype=UTF8, encoding=DELTA_BYTE_ARRAY, omitstats=true"`
	Plain string `parquet:"name=plain, type=BYTE_ARRAY, convertedtype=UTF8, omitstats=true"`
	Dict  string `parquet:"name=dict, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY, omitstats=true"`
}

// sizeLimitDelta varies each delta value's last byte, so no value repeats the previous one and shares its bytes.
func sizeLimitDelta(i int) string {
	return strings.Repeat("x", 1023) + string(rune('a'+i%26))
}

// writeSizeLimitFile writes one page per column; the delta and plain pages each decode to about 1 MiB.
func writeSizeLimitFile(t *testing.T, footerKey []byte, codec parquet.CompressionCodec) []byte {
	t.Helper()

	var buf bytes.Buffer
	opts := []writer.WriterOption{
		writer.WithNP(1),
		writer.WithPageSize(64 << 20),
		writer.WithDataPageVersion(2),
		writer.WithCompressionCodec(codec),
	}
	if footerKey != nil {
		opts = append(opts, writer.WithFooterKey(footerKey), writer.WithAADFileUnique([]byte("size-limit-test")),
			writer.WithColumnEncrypted("delta"), writer.WithColumnEncrypted("plain"), writer.WithColumnEncrypted("dict"))
	}
	ctx := context.Background()
	pw, err := writer.NewParquetWriterWithContext(ctx, writerfile.NewWriterFile(&buf), new(sizeLimitRecord), opts...)
	require.NoError(t, err)
	value := strings.Repeat("x", 1024)
	for i := range 1024 {
		require.NoError(t, pw.WriteWithContext(ctx, sizeLimitRecord{Delta: sizeLimitDelta(i), Plain: value, Dict: value}))
	}
	require.NoError(t, pw.WriteStopWithContext(ctx))
	return buf.Bytes()
}

func TestReaderSizeLimits(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	for _, codec := range []parquet.CompressionCodec{parquet.CompressionCodec_SNAPPY, parquet.CompressionCodec_UNCOMPRESSED} {
		for name, key := range map[string][]byte{"plaintext": nil, "encrypted": footerKey} {
			t.Run(codec.String()+"/"+name, func(t *testing.T) {
				t.Parallel()

				data := writeSizeLimitFile(t, key, codec)
				read := func(opts ...ReaderOption) error {
					opts = append(opts, WithNP(1))
					if key != nil {
						opts = append(opts, WithFooterKey(key))
					}
					pr, err := NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(data), new(sizeLimitRecord), opts...)
					require.NoError(t, err)
					defer func() { require.NoError(t, pr.ReadStop()) }()
					rows := make([]sizeLimitRecord, pr.GetNumRows())
					if err := pr.ReadWithContext(context.Background(), &rows); err != nil {
						return err
					}
					require.Len(t, rows, 1024)
					require.Equal(t, sizeLimitDelta(1023), rows[1023].Delta)
					return nil
				}

				require.NoError(t, read())
				for _, tt := range []struct {
					opt  func(int64) ReaderOption
					low  int64
					want string
				}{
					{WithMaxPageSize, 16 << 10, "reader.WithMaxPageSize"},
					{WithMaxDecompressedSize, 256 << 10, "reader.WithMaxDecompressedSize"},
					{WithMaxDecodedBatchBytes, 256 << 10, "reader.WithMaxDecodedBatchBytes"},
				} {
					require.ErrorContains(t, read(tt.opt(tt.low)), tt.want)
					require.NoError(t, read(tt.opt(4<<20)))
					require.NoError(t, read(tt.opt(-1)))
				}
			})
		}
	}
}

func TestPageInspectionSizeLimits(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	for name, key := range map[string][]byte{"plaintext": nil, "encrypted": footerKey} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			data := writeSizeLimitFile(t, key, parquet.CompressionCodec_UNCOMPRESSED)
			open := func(opts ...ReaderOption) *ParquetReader {
				opts = append(opts, WithNP(1))
				if key != nil {
					opts = append(opts, WithFooterKey(key))
				}
				pr, err := NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(data), nil, opts...)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, pr.ReadStop()) })
				return pr
			}
			dictionaries := func(pr *ParquetReader) []error {
				_, err := pr.ReadDictionaryPageValuesInColumn(0, 2)
				errs := []error{err}
				if key == nil {
					meta := pr.Footer.RowGroups[0].Columns[2].MetaData
					_, err = pr.ReadDictionaryPageValues(meta.GetDictionaryPageOffset(), meta.GetCodec(), meta.GetType())
					errs = append(errs, err)
				}
				return errs
			}

			// Header listing seeks past plaintext bodies without reading them, so only encrypted lengths are checked.
			pr := open(WithMaxPageSize(16 << 10))
			_, allErr := pr.GetAllPageHeaders(0, 1)
			_, firstErr := pr.GetFirstDataPageHeader(0, 1)
			for _, err := range []error{allErr, firstErr} {
				if key == nil {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "reader.WithMaxPageSize")
				}
			}
			_, err := pr.GetFirstDataPageHeader(0, 2)
			require.NoError(t, err)

			for _, err := range dictionaries(open(WithMaxPageSize(16))) {
				require.ErrorContains(t, err, "reader.WithMaxPageSize")
			}
			for _, err := range dictionaries(open(WithMaxDecompressedSize(16))) {
				require.ErrorIs(t, err, compress.ErrDecompressedSizeExceeded)
				require.ErrorContains(t, err, "reader.WithMaxDecompressedSize")
			}
			for _, err := range dictionaries(open(WithMaxPageSize(-1), WithMaxDecompressedSize(-1))) {
				require.NoError(t, err)
			}
		})
	}
}
