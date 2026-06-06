package writer

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

func TestEncryptedWriterKeyRetriever(t *testing.T) {
	t.Parallel()

	keys := map[string][]byte{
		testFooterKeyID: []byte("0123456789abcdef"),
		testNameKeyID:   []byte("abcdef0123456789"),
	}
	keyRetriever := func(keyMetadata []byte) ([]byte, error) {
		return keys[string(keyMetadata)], nil
	}

	var buf bytes.Buffer
	pw, err := NewParquetWriter(
		writerfile.NewWriterFile(&buf),
		new(encryptedWriterRecord),
		WithNP(1),
		WithRowGroupSize(128),
		WithPageSize(32),
		WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		WithFooterKeyMetadata([]byte(testFooterKeyID)),
		WithColumnEncrypted("name", ColumnKeyByMetadata([]byte(testNameKeyID))),
		WithKeyRetriever(keyRetriever),
		WithAADPrefix([]byte("writer-test")),
		WithAADFileUnique([]byte("file-unique3")),
	)
	require.NoError(t, err)
	require.NoError(t, pw.Write(encryptedWriterRecord{ID: 1, Name: "alpha"}))
	require.NoError(t, pw.WriteStop())

	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes()),
		new(encryptedWriterRecord),
		reader.WithKeyRetriever(keyRetriever),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pr.ReadStop())
	}()

	rows := make([]encryptedWriterRecord, 1)
	require.NoError(t, pr.Read(&rows))
	require.Equal(t, []encryptedWriterRecord{{ID: 1, Name: "alpha"}}, rows)
}

func TestEncryptedWriterFooterKeyPrecedence(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	wrongFooterKey := []byte("abcdef0123456789")
	keyRetriever := func(keyMetadata []byte) ([]byte, error) {
		return wrongFooterKey, nil
	}

	var buf bytes.Buffer
	pw, err := NewParquetWriter(
		writerfile.NewWriterFile(&buf),
		new(encryptedWriterRecord),
		WithNP(1),
		WithRowGroupSize(128),
		WithPageSize(32),
		WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		WithFooterKey(footerKey),
		WithFooterKeyMetadata([]byte(testFooterKeyID)),
		WithKeyRetriever(keyRetriever),
		WithAADPrefix([]byte("writer-test")),
		WithAADFileUnique([]byte("file-unique4")),
	)
	require.NoError(t, err)
	require.NoError(t, pw.Write(encryptedWriterRecord{ID: 1, Name: "alpha"}))
	require.NoError(t, pw.WriteStop())

	// WithFooterKey wins: file was encrypted with footerKey, not wrongFooterKey from retriever
	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes()),
		new(encryptedWriterRecord),
		reader.WithFooterKey(footerKey),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pr.ReadStop())
	}()

	rows := make([]encryptedWriterRecord, 1)
	require.NoError(t, pr.Read(&rows))
	require.Equal(t, []encryptedWriterRecord{{ID: 1, Name: "alpha"}}, rows)
}

func TestEncryptedWriterColumnKeyPrecedence(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")
	wrongNameKey := []byte("1234567890abcdef")
	keyRetriever := func(keyMetadata []byte) ([]byte, error) {
		if string(keyMetadata) == testNameKeyID {
			return wrongNameKey, nil
		}
		return nil, nil
	}

	var buf bytes.Buffer
	pw, err := NewParquetWriter(
		writerfile.NewWriterFile(&buf),
		new(encryptedWriterRecord),
		WithNP(1),
		WithRowGroupSize(128),
		WithPageSize(32),
		WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		WithFooterKey(footerKey),
		WithColumnEncrypted("name", ColumnKey(nameKey, []byte(testNameKeyID))),
		WithKeyRetriever(keyRetriever),
		WithAADPrefix([]byte("writer-test")),
		WithAADFileUnique([]byte("file-unique5")),
	)
	require.NoError(t, err)
	require.NoError(t, pw.Write(encryptedWriterRecord{ID: 1, Name: "alpha"}))
	require.NoError(t, pw.WriteStop())

	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes()),
		new(encryptedWriterRecord),
		reader.WithFooterKey(footerKey),
		reader.WithColumnKey("name", nameKey),
	)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, pr.ReadStop())
	}()

	rows := make([]encryptedWriterRecord, 1)
	require.NoError(t, pr.Read(&rows))
	require.Equal(t, []encryptedWriterRecord{{ID: 1, Name: "alpha"}}, rows)
}

func TestWithEncryptionValidatesKeys(t *testing.T) {
	t.Parallel()

	t.Run("invalid footer key size", func(t *testing.T) {
		t.Parallel()
		_, _, err := createTestParquetWriter(new(encryptedWriterRecord), WithFooterKey([]byte("short")))
		require.ErrorContains(t, err, "invalid AES footer key size")
	})

	t.Run("invalid column key size", func(t *testing.T) {
		t.Parallel()
		_, _, err := createTestParquetWriter(
			new(encryptedWriterRecord),
			WithFooterKey([]byte("0123456789abcdef")),
			WithColumnEncrypted("name", ColumnKey([]byte("short"))),
		)
		require.ErrorContains(t, err, `column "name" has invalid AES key size`)
	})

	t.Run("invalid column key size from key retriever", func(t *testing.T) {
		t.Parallel()
		_, _, err := createTestParquetWriter(
			new(encryptedWriterRecord),
			WithFooterKey([]byte("0123456789abcdef")),
			WithColumnEncrypted("name", ColumnKeyByMetadata([]byte(testNameKeyID))),
			WithKeyRetriever(func([]byte) ([]byte, error) { return []byte("short"), nil }),
		)
		require.ErrorContains(t, err, `column "name" resolved key has invalid AES size`)
	})

	t.Run("supply_aad_prefix without aad_prefix", func(t *testing.T) {
		t.Parallel()
		_, _, err := createTestParquetWriter(
			new(encryptedWriterRecord),
			WithFooterKey([]byte("0123456789abcdef")),
			WithSupplyAADPrefix(true),
		)
		require.ErrorContains(t, err, "WithSupplyAADPrefix: AADPrefix must be non-empty")
	})
}

func TestEncryptedWriterKeyRetrieverErrors(t *testing.T) {
	t.Parallel()

	retrieverErr := errors.New("KMS unavailable")

	t.Run("footer key retriever error", func(t *testing.T) {
		t.Parallel()
		_, _, err := createTestParquetWriter(
			new(encryptedWriterRecord),
			WithFooterKeyMetadata([]byte(testFooterKeyID)),
			WithKeyRetriever(func([]byte) ([]byte, error) { return nil, retrieverErr }),
		)
		require.ErrorContains(t, err, "retrieve footer key")
		require.ErrorIs(t, err, retrieverErr)
	})

	t.Run("column key retriever error", func(t *testing.T) {
		t.Parallel()
		_, _, err := createTestParquetWriter(
			new(encryptedWriterRecord),
			WithFooterKey([]byte("0123456789abcdef")),
			WithColumnEncrypted("name", ColumnKeyByMetadata([]byte(testNameKeyID))),
			WithKeyRetriever(func([]byte) ([]byte, error) { return nil, retrieverErr }),
		)
		require.ErrorContains(t, err, "retrieve column key")
		require.ErrorIs(t, err, retrieverErr)
	})
}

func TestWithEncryptionUnsupportedAlgorithm(t *testing.T) {
	t.Parallel()

	_, _, err := createTestParquetWriter(
		new(encryptedWriterRecord),
		WithEncryption(EncryptionConfig{
			FooterKey: []byte("0123456789abcdef"),
			Algorithm: EncryptionAlgorithm(99),
		}),
	)
	require.ErrorContains(t, err, "unsupported encryption algorithm")
}

func TestWithColumnEncryptedValidatesPathAgainstSchema(t *testing.T) {
	t.Parallel()

	t.Run("typo in column encryption path is rejected", func(t *testing.T) {
		t.Parallel()
		_, _, err := createTestParquetWriter(
			new(encryptedWriterRecord),
			WithFooterKey([]byte("0123456789abcdef")),
			WithColumnEncrypted("nmae", ColumnKey([]byte("abcdef0123456789"))),
		)
		require.ErrorContains(t, err, "WithColumnEncrypted")
		require.ErrorContains(t, err, "nmae")
	})

	t.Run("intermediate path is rejected", func(t *testing.T) {
		t.Parallel()
		// "Parquet_go_root" is the root, not a leaf column.
		_, _, err := createTestParquetWriter(
			new(encryptedWriterRecord),
			WithFooterKey([]byte("0123456789abcdef")),
			WithColumnEncrypted("parquet_go_root", ColumnKey([]byte("abcdef0123456789"))),
		)
		require.Error(t, err)
	})

	t.Run("valid column encryption path is accepted", func(t *testing.T) {
		t.Parallel()
		_, _, err := createTestParquetWriter(
			new(encryptedWriterRecord),
			WithFooterKey([]byte("0123456789abcdef")),
			WithColumnEncrypted("name", ColumnKey([]byte("abcdef0123456789"))),
		)
		require.NoError(t, err)
	})

	t.Run("SetSchemaHandlerFromJSON also validates", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		pw, err := NewParquetWriter(
			writerfile.NewWriterFile(&buf),
			nil,
			WithFooterKey([]byte("0123456789abcdef")),
			WithColumnEncrypted("nmae", ColumnKey([]byte("abcdef0123456789"))),
		)
		require.NoError(t, err) // no schema yet, validation deferred
		err = pw.SetSchemaHandlerFromJSON(`{"Tag": "name=parquet_go_root", "Fields": [{"Tag": "name=id, type=INT32"}, {"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"}]}`)
		require.ErrorContains(t, err, "nmae")
	})
}
