package reader

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/writer"
)

func TestResolveColumnKeyMatchesPathForms(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	tests := []struct {
		name           string
		configuredPath string
		cryptoPath     []string
	}{
		{
			name:           "leaf path",
			configuredPath: "name",
			cryptoPath:     []string{"name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pr := &ParquetReader{}
			applyReaderOptionsForTest(t, pr, WithColumnKey(tt.configuredPath, key))

			got, err := pr.resolveColumnKey(columnKeyChunk(tt.cryptoPath))
			require.NoError(t, err)
			require.Equal(t, key, got)
		})
	}
}

func TestResolveColumnKeyDoesNotStripRootNames(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")

	t.Run("configured root prefix does not match leaf path", func(t *testing.T) {
		t.Parallel()

		pr := &ParquetReader{}
		applyReaderOptionsForTest(t, pr, WithColumnKey(common.ParGoRootExName+".name", key))

		_, err := pr.resolveColumnKey(columnKeyChunk([]string{"name"}))
		require.ErrorIs(t, err, ErrColumnKeyRequired)
	})

	t.Run("metadata root prefix does not match leaf path", func(t *testing.T) {
		t.Parallel()

		pr := &ParquetReader{}
		applyReaderOptionsForTest(t, pr, WithColumnKey("name", key))

		_, err := pr.resolveColumnKey(columnKeyChunk([]string{common.ParGoRootExName, "name"}))
		require.ErrorIs(t, err, ErrColumnKeyRequired)
		require.ErrorContains(t, err, "for column "+common.ParGoRootExName+".name")
	})
}

func TestResolveColumnKeyRejectsGoFieldName(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{}
	applyReaderOptionsForTest(t, pr, WithColumnKey("Name", []byte("0123456789abcdef")))

	_, err := pr.resolveColumnKey(columnKeyChunk([]string{"name"}))
	require.ErrorIs(t, err, ErrColumnKeyRequired)
	require.ErrorContains(t, err, "for column name")
}

// TestNewParquetReaderRejectsUnknownColumnKeyPath checks that typos fail at
// construction instead of becoming read-time missing-key errors.
func TestNewParquetReaderRejectsUnknownColumnKeyPath(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")
	data := writeColumnKeyTestFile(t, footerKey, nameKey)

	tests := []struct {
		name string
		path string
	}{
		{name: "typo", path: "nmae"},
		{name: "bare root", path: common.ParGoRootExName},
		{name: "root-prefixed leaf", path: common.ParGoRootExName + ".name"},
		{name: "go field name", path: "Name"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewParquetReader(
				buffer.NewBufferReaderFromBytesNoAlloc(data),
				new(columnKeyTestRecord),
				WithFooterKey(footerKey),
				WithColumnKey(tt.path, nameKey),
			)
			require.Error(t, err)
			require.ErrorContains(t, err, "WithColumnKey")
			require.ErrorContains(t, err, tt.path)
		})
	}
}

func TestNewParquetColumnReaderRejectsUnknownColumnKeyPath(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")
	data := writeColumnKeyTestFile(t, footerKey, nameKey)

	_, err := NewParquetColumnReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		WithFooterKey(footerKey),
		WithColumnKey("nmae", nameKey),
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "WithColumnKey")
	require.ErrorContains(t, err, "nmae")
}

func TestNewParquetReaderAcceptsColumnNamedLikeRoot(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")
	data := writeRootNamedColumnKeyTestFile(t, footerKey, nameKey)

	pr, err := NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(columnKeyRootNamedTestRecord),
		WithFooterKey(footerKey),
		WithColumnKey(common.ParGoRootExName+".name", nameKey),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	rows := make([]columnKeyRootNamedTestRecord, 1)
	require.NoError(t, pr.Read(&rows))
	require.Equal(t, []columnKeyRootNamedTestRecord{{ID: 1, Root: columnKeyRootNamedGroup{Name: "alpha"}}}, rows)
}

type columnKeyTestRecord struct {
	ID   int32  `parquet:"name=id, type=INT32"`
	Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
}

type columnKeyRootNamedTestRecord struct {
	ID   int32                   `parquet:"name=id, type=INT32"`
	Root columnKeyRootNamedGroup `parquet:"name=parquet_go_root"`
}

type columnKeyRootNamedGroup struct {
	Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func writeColumnKeyTestFile(t *testing.T, footerKey, nameKey []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	pw, err := writer.NewParquetWriter(
		writerfile.NewWriterFile(&buf),
		new(columnKeyTestRecord),
		writer.WithFooterKey(footerKey),
		writer.WithColumnEncrypted("name", writer.ColumnKey(nameKey)),
		writer.WithAADFileUnique([]byte("column-key-test")),
	)
	require.NoError(t, err)
	require.NoError(t, pw.Write(columnKeyTestRecord{ID: 1, Name: "alpha"}))
	require.NoError(t, pw.WriteStop())
	return buf.Bytes()
}

func writeRootNamedColumnKeyTestFile(t *testing.T, footerKey, nameKey []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	pw, err := writer.NewParquetWriter(
		writerfile.NewWriterFile(&buf),
		new(columnKeyRootNamedTestRecord),
		writer.WithFooterKey(footerKey),
		writer.WithColumnEncrypted(common.ParGoRootExName+".name", writer.ColumnKey(nameKey)),
		writer.WithAADFileUnique([]byte("root-named-column-key-test")),
	)
	require.NoError(t, err)
	require.NoError(t, pw.Write(columnKeyRootNamedTestRecord{ID: 1, Root: columnKeyRootNamedGroup{Name: "alpha"}}))
	require.NoError(t, pw.WriteStop())
	return buf.Bytes()
}

func columnKeyChunk(path []string) *parquet.ColumnChunk {
	return &parquet.ColumnChunk{
		CryptoMetadata: &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{
				PathInSchema: path,
			},
		},
	}
}
