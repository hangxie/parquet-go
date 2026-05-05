package reader

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/buffer"
)

func TestReadColumnAndOffsetIndex(t *testing.T) {
	t.Parallel()

	columnIndex := &parquet.ColumnIndex{
		NullPages:     []bool{false},
		MinValues:     [][]byte{{1}},
		MaxValues:     [][]byte{{9}},
		BoundaryOrder: parquet.BoundaryOrder_ASCENDING,
		NullCounts:    []int64{0},
	}
	offsetIndex := &parquet.OffsetIndex{
		PageLocations: []*parquet.PageLocation{{
			Offset:             12,
			CompressedPageSize: 34,
			FirstRowIndex:      56,
		}},
	}
	columnIndexBuf := serializeThrift(t, columnIndex)
	offsetIndexBuf := serializeThrift(t, offsetIndex)
	file := append(append([]byte{}, columnIndexBuf...), offsetIndexBuf...)

	pr := indexTestReader(file, false, nil, nil, int32(len(columnIndexBuf)), int32(len(offsetIndexBuf)))
	gotColumnIndex, err := pr.ReadColumnIndex(0, 0)
	require.NoError(t, err)
	require.Equal(t, columnIndex, gotColumnIndex)

	gotOffsetIndex, err := pr.ReadOffsetIndex(0, 0)
	require.NoError(t, err)
	require.Equal(t, offsetIndex, gotOffsetIndex)
}

func TestReadEncryptedColumnAndOffsetIndex(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	aadPrefix := []byte("prefix")
	fileUnique := []byte("file-unique")
	columnIndex := &parquet.ColumnIndex{
		NullPages:     []bool{false},
		MinValues:     [][]byte{{2}},
		MaxValues:     [][]byte{{8}},
		BoundaryOrder: parquet.BoundaryOrder_ASCENDING,
		NullCounts:    []int64{1},
	}
	offsetIndex := &parquet.OffsetIndex{
		PageLocations: []*parquet.PageLocation{{
			Offset:             22,
			CompressedPageSize: 44,
			FirstRowIndex:      66,
		}},
	}
	encryptedColumnIndex := encryptGCMModule(t, key, encryption.AAD(aadPrefix, fileUnique, encryption.ModuleColumnIndex, 0, 0, 0), serializeThrift(t, columnIndex))
	encryptedOffsetIndex := encryptGCMModule(t, key, encryption.AAD(aadPrefix, fileUnique, encryption.ModuleOffsetIndex, 0, 0, 0), serializeThrift(t, offsetIndex))
	file := append(append([]byte{}, encryptedColumnIndex...), encryptedOffsetIndex...)

	pr := indexTestReader(file, true, key, encryptionAlgorithm(aadPrefix, fileUnique), int32(len(encryptedColumnIndex)), int32(len(encryptedOffsetIndex)))
	gotColumnIndex, err := pr.ReadColumnIndex(0, 0)
	require.NoError(t, err)
	require.Equal(t, columnIndex, gotColumnIndex)

	gotOffsetIndex, err := pr.ReadOffsetIndex(0, 0)
	require.NoError(t, err)
	require.Equal(t, offsetIndex, gotOffsetIndex)
}

func TestReadIndexMissingAndErrors(t *testing.T) {
	t.Parallel()

	pr := indexTestReader(nil, false, nil, nil, 0, 0)
	pr.Footer.RowGroups[0].Columns[0].ColumnIndexOffset = nil
	pr.Footer.RowGroups[0].Columns[0].ColumnIndexLength = nil

	columnIndex, err := pr.ReadColumnIndex(0, 0)
	require.NoError(t, err)
	require.Nil(t, columnIndex)

	_, err = pr.ReadOffsetIndex(1, 0)
	require.ErrorContains(t, err, "invalid row group index")

	key := []byte("0123456789abcdef")
	aadPrefix := []byte("prefix")
	fileUnique := []byte("file-unique")
	encryptedColumnIndex := encryptGCMModule(t, key, encryption.AAD(aadPrefix, fileUnique, encryption.ModuleColumnIndex, 0, 0, 0), serializeThrift(t, parquet.NewColumnIndex()))
	badKeyReader := indexTestReader(encryptedColumnIndex, true, []byte("abcdef0123456789"), encryptionAlgorithm(aadPrefix, fileUnique), int32(len(encryptedColumnIndex)), 0)
	_, err = badKeyReader.ReadColumnIndex(0, 0)
	require.ErrorContains(t, err, "decrypt index module")

	_, err = (&ParquetReader{}).ReadColumnIndex(0, 0)
	require.ErrorContains(t, err, "reader footer is nil")

	nilRowGroupReader := indexTestReader(nil, false, nil, nil, 0, 0)
	nilRowGroupReader.Footer.RowGroups[0] = nil
	_, err = nilRowGroupReader.ReadColumnIndex(0, 0)
	require.ErrorContains(t, err, "row group 0 is nil")

	_, err = pr.ReadColumnIndex(0, 1)
	require.ErrorContains(t, err, "invalid column index")

	plainMissingOffsetReader := indexTestReader(nil, false, nil, nil, 0, 0)
	plainMissingOffsetReader.Footer.RowGroups[0].Columns[0].OffsetIndexOffset = nil
	plainMissingOffsetReader.Footer.RowGroups[0].Columns[0].OffsetIndexLength = nil
	offsetIndex, err := plainMissingOffsetReader.ReadOffsetIndex(0, 0)
	require.NoError(t, err)
	require.Nil(t, offsetIndex)

	nilFileReader := indexTestReader(nil, false, nil, nil, 1, 0)
	nilFileReader.PFile = nil
	_, err = nilFileReader.ReadColumnIndex(0, 0)
	require.ErrorContains(t, err, "file reader is nil")

	negativeLengthReader := indexTestReader(nil, false, nil, nil, -1, 0)
	_, err = negativeLengthReader.ReadColumnIndex(0, 0)
	require.ErrorContains(t, err, "negative index length")

	invalidPlainReader := indexTestReader([]byte{0xff}, false, nil, nil, 1, 0)
	_, err = invalidPlainReader.ReadColumnIndex(0, 0)
	require.ErrorContains(t, err, "read column index")

	missingAlgorithmReader := indexTestReader(encryptedColumnIndex, true, key, nil, int32(len(encryptedColumnIndex)), 0)
	_, err = missingAlgorithmReader.ReadColumnIndex(0, 0)
	require.ErrorContains(t, err, "encrypted index missing file encryption algorithm")

	missingKeyReader := indexTestReader(encryptedColumnIndex, true, nil, encryptionAlgorithm(aadPrefix, fileUnique), int32(len(encryptedColumnIndex)), 0)
	_, err = missingKeyReader.ReadColumnIndex(0, 0)
	require.ErrorContains(t, err, "footer decryption key is required")

	truncatedEncryptedReader := indexTestReader([]byte{1, 2}, true, key, encryptionAlgorithm(aadPrefix, fileUnique), 2, 0)
	_, err = truncatedEncryptedReader.ReadColumnIndex(0, 0)
	require.ErrorContains(t, err, "read encrypted index module")
}

func TestReadPlainIndexModuleSeekError(t *testing.T) {
	t.Parallel()

	_, err := readPlainIndexModule(&indexFailSeeker{}, 0, 1)
	require.ErrorContains(t, err, "seek to index offset")
}

type indexFailSeeker struct{}

func (f *indexFailSeeker) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (f *indexFailSeeker) Seek(int64, int) (int64, error) {
	return 0, io.ErrClosedPipe
}

func (f *indexFailSeeker) Close() error {
	return nil
}

func (f *indexFailSeeker) Open(string) (source.ParquetFileReader, error) {
	return nil, io.ErrClosedPipe
}

func (f *indexFailSeeker) Clone() (source.ParquetFileReader, error) {
	return nil, io.ErrClosedPipe
}

func indexTestReader(file []byte, encrypted bool, key []byte, algorithm *parquet.EncryptionAlgorithm, columnIndexLength, offsetIndexLength int32) *ParquetReader {
	columnIndexOffset := int64(0)
	offsetIndexOffset := int64(columnIndexLength)
	column := &parquet.ColumnChunk{
		ColumnIndexOffset: &columnIndexOffset,
		ColumnIndexLength: &columnIndexLength,
		OffsetIndexOffset: &offsetIndexOffset,
		OffsetIndexLength: &offsetIndexLength,
	}
	if encrypted {
		column.CryptoMetadata = &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey(),
		}
	}
	return &ParquetReader{
		PFile:     buffer.NewBufferReaderFromBytesNoAlloc(file),
		Footer:    &parquet.FileMetaData{EncryptionAlgorithm: algorithm, RowGroups: []*parquet.RowGroup{{Columns: []*parquet.ColumnChunk{column}}}},
		footerKey: key,
	}
}

func encryptionAlgorithm(aadPrefix, fileUnique []byte) *parquet.EncryptionAlgorithm {
	return &parquet.EncryptionAlgorithm{
		AES_GCM_V1: &parquet.AesGcmV1{
			AadPrefix:     aadPrefix,
			AadFileUnique: fileUnique,
		},
	}
}
