package reader

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/writer"
)

type encryptedReaderRecord struct {
	ID   int32  `parquet:"name=id, type=INT32"`
	Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func applyReaderOptionsForTest(t *testing.T, pr *ParquetReader, opts ...ReaderOption) {
	t.Helper()

	// These tests build minimal readers to exercise footer and encryption helper
	// paths directly. The public option API remains constructor-only.
	for _, opt := range opts {
		opt.apply(pr)
	}
}

func TestReadFooterEncryptedFooter(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	aadPrefix := []byte("table/part-0")
	fileUnique := []byte("file-unique")
	footer := minimalFileMetaData()
	file := buildEncryptedFooterFile(t, key, aadPrefix, fileUnique, footer)

	pr := &ParquetReader{
		PFile: buffer.NewBufferReaderFromBytesNoAlloc(file),
	}
	applyReaderOptionsForTest(t, pr, WithFooterKey(key))

	require.NoError(t, pr.ReadFooter())
	require.NotNil(t, pr.FileCrypto)
	require.True(t, footer.Equals(pr.Footer))
}

func TestReadFooterDoesNotReloadAfterSuccessfulRead(t *testing.T) {
	t.Parallel()

	firstFooter := minimalFileMetaData()
	firstFooter.NumRows = 1
	secondFooter := minimalFileMetaData()
	secondFooter.NumRows = 2

	pr := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(buildPlainFooterFile(t, firstFooter))}
	require.NoError(t, pr.ReadFooter())
	loadedFooter := pr.Footer
	require.Equal(t, int64(1), pr.Footer.GetNumRows())

	pr.PFile = buffer.NewBufferReaderFromBytesNoAlloc(buildPlainFooterFile(t, secondFooter))
	require.NoError(t, pr.ReadFooter())
	require.Same(t, loadedFooter, pr.Footer)
	require.Equal(t, int64(1), pr.Footer.GetNumRows())
}

func TestReadFooterEncryptedFooterErrors(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	aadPrefix := []byte("table/part-0")
	fileUnique := []byte("file-unique")
	file := buildEncryptedFooterFile(t, key, aadPrefix, fileUnique, minimalFileMetaData())

	tests := []struct {
		name    string
		opts    []ReaderOption
		wantErr string
	}{
		{name: "missing key", wantErr: "decryption key required"},
		{name: "wrong key", opts: []ReaderOption{WithFooterKey([]byte("abcdef0123456789"))}, wantErr: "decrypt footer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pr := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
			applyReaderOptionsForTest(t, pr, tt.opts...)
			err := pr.ReadFooter()
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestReadFooterEncryptedFooterWithSuppliedAADPrefix(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	aadPrefix := []byte("external-prefix")
	fileUnique := []byte("file-unique")
	footer := minimalFileMetaData()
	file := buildEncryptedFooterFileWithAlgorithm(t, key, aadPrefix, fileUnique, footer, &parquet.EncryptionAlgorithm{
		AES_GCM_V1: &parquet.AesGcmV1{
			AadFileUnique:   fileUnique,
			SupplyAadPrefix: boolPtr(true),
		},
	})

	t.Run("provided", func(t *testing.T) {
		t.Parallel()
		pr := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
		applyReaderOptionsForTest(t, pr, WithFooterKey(key), WithAADPrefix(aadPrefix))
		require.NoError(t, pr.ReadFooter())
		require.True(t, footer.Equals(pr.Footer))
	})

	t.Run("missing", func(t *testing.T) {
		t.Parallel()
		pr := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
		applyReaderOptionsForTest(t, pr, WithFooterKey(key))
		require.ErrorContains(t, pr.ReadFooter(), "AAD prefix is required")
	})
}

func TestReadFooterVerifiesPlaintextFooterSignature(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	aadPrefix := []byte("table/part-0")
	fileUnique := []byte("file-unique")
	footer := minimalFileMetaData()
	footer.EncryptionAlgorithm = &parquet.EncryptionAlgorithm{
		AES_GCM_V1: &parquet.AesGcmV1{
			AadPrefix:     aadPrefix,
			AadFileUnique: fileUnique,
		},
	}
	file := buildPlaintextEncryptedFooterFile(t, key, aadPrefix, fileUnique, footer)

	pr := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
	applyReaderOptionsForTest(t, pr, WithFooterKey(key))
	require.NoError(t, pr.ReadFooter())
	require.True(t, footer.Equals(pr.Footer))

	badKeyReader := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
	applyReaderOptionsForTest(t, badKeyReader, WithFooterKey([]byte("abcdef0123456789")))
	require.ErrorContains(t, badKeyReader.ReadFooter(), "verify plaintext footer signature")
}

func TestReadFooterPlaintextFooterDoesNotRequireKey(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	aadPrefix := []byte("table/part-0")
	fileUnique := []byte("file-unique")
	footer := minimalFileMetaData()
	footer.EncryptionAlgorithm = encryptionAlgorithm(aadPrefix, fileUnique)
	file := buildPlaintextEncryptedFooterFile(t, key, aadPrefix, fileUnique, footer)

	pr := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
	require.NoError(t, pr.ReadFooter())
	require.True(t, footer.Equals(pr.Footer))
}

func TestReadFooterPlaintextFooterMixedColumnsSkipsMissingColumnKeys(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	columnKey := []byte("abcdef0123456789")
	aadPrefix := []byte("table/part-0")
	fileUnique := []byte("file-unique")
	footer, plaintextMeta, encryptedPlaceholder, wantEncryptedMeta := fileMetaDataWithMixedPlainAndEncryptedColumns(
		t,
		columnKey,
		aadPrefix,
		fileUnique,
	)
	file := buildPlaintextEncryptedFooterFile(t, footerKey, aadPrefix, fileUnique, footer)

	pr := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
	require.NoError(t, pr.ReadFooter())
	require.True(t, plaintextMeta.Equals(pr.Footer.RowGroups[0].Columns[0].MetaData))
	require.True(t, encryptedPlaceholder.Equals(pr.Footer.RowGroups[0].Columns[1].MetaData))

	withColumnKey := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
	applyReaderOptionsForTest(t, withColumnKey, WithColumnKey("encrypted_leaf", columnKey))
	require.NoError(t, withColumnKey.ReadFooter())
	require.True(t, plaintextMeta.Equals(withColumnKey.Footer.RowGroups[0].Columns[0].MetaData))
	require.True(t, wantEncryptedMeta.Equals(withColumnKey.Footer.RowGroups[0].Columns[1].MetaData))
}

func TestNewParquetReaderPlaintextFooterMixedColumnsDoesNotRequireUnreadColumnKey(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	columnKey := []byte("abcdef0123456789")
	aadPrefix := []byte("table/part-0")
	fileUnique := []byte("file-unique")
	footer, _, _, _ := fileMetaDataWithMixedPlainAndEncryptedColumns(t, columnKey, aadPrefix, fileUnique)
	file := buildPlaintextEncryptedFooterFile(t, footerKey, aadPrefix, fileUnique, footer)

	pr, err := NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(file), nil)
	require.NoError(t, err)
	require.Len(t, pr.ColumnBuffers, 2)
	var encryptedBuffer *ColumnBufferType
	for _, columnBuffer := range pr.ColumnBuffers {
		if columnBuffer.ChunkHeader != nil && columnBuffer.ChunkHeader.GetCryptoMetadata() != nil {
			encryptedBuffer = columnBuffer
		}
	}
	require.NotNil(t, encryptedBuffer)
	require.Nil(t, encryptedBuffer.PageReadOptions.Decryptor)
	require.NoError(t, pr.ReadStop())
}

func TestPlaintextFooterMixedColumnsDefersAADUntilEncryptedColumnRead(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")
	aadPrefix := []byte("aad-must-be-supplied")
	data := buildMixedPlaintextFooterSuppliedAADData(t, footerKey, nameKey, aadPrefix)

	prPlainOnly, err := NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(encryptedReaderRecord),
	)
	require.NoError(t, err)
	idValues, _, _, err := prPlainOnly.ReadColumnByPath(valueColumnPathWithLeaf(t, prPlainOnly, "id"), 2)
	require.NoError(t, err)
	require.Equal(t, []any{int32(1), int32(2)}, idValues)
	require.NoError(t, prPlainOnly.ReadStop())

	_, err = NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(encryptedReaderRecord),
		WithColumnKey("name", nameKey),
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "AAD prefix is required")

	_, err = NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(encryptedReaderRecord),
		WithColumnKey("name", nameKey),
		WithAADPrefix([]byte("wrong-aad-prefix")),
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "decrypt column metadata")

	prWithPrefix, err := NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(encryptedReaderRecord),
		WithColumnKey("name", nameKey),
		WithAADPrefix(aadPrefix),
	)
	require.NoError(t, err)
	nameValues, _, _, err := prWithPrefix.ReadColumnByPath(valueColumnPathWithLeaf(t, prWithPrefix, "name"), 2)
	require.NoError(t, err)
	require.Equal(t, []any{"alpha", "beta"}, nameValues)
	require.NoError(t, prWithPrefix.ReadStop())
}

func TestPlaintextFooterDefersUnreadEncryptedColumnRetrieverError(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")
	unknownKeyErr := errors.New("unknown key id")
	data := buildPlaintextFooterEncryptedColumnData(t, footerKey, nameKey)

	pr, err := NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(encryptedReaderRecord),
		WithFooterKey(footerKey),
		WithKeyRetriever(func(keyMetadata []byte) ([]byte, error) {
			if string(keyMetadata) == "name-key" {
				return nil, unknownKeyErr
			}
			return nil, nil
		}),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	values, _, _, err := pr.ReadColumnByPath(valueColumnPathWithLeaf(t, pr, "id"), 2)
	require.NoError(t, err)
	require.Equal(t, []any{int32(1), int32(2)}, values)

	_, _, _, err = pr.ReadColumnByPath(valueColumnPathWithLeaf(t, pr, "name"), 1)
	require.ErrorIs(t, err, unknownKeyErr)
	require.ErrorContains(t, err, "retrieve column key for name")
}

func TestResetDoesNotRequireUnreadEncryptedColumnKey(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")
	data := buildPlaintextFooterEncryptedColumnData(t, footerKey, nameKey)

	pr, err := NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(encryptedReaderRecord),
		WithFooterKey(footerKey),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	require.NoError(t, pr.Reset())
	values, _, _, err := pr.ReadColumnByPath(valueColumnPathWithLeaf(t, pr, "id"), 2)
	require.NoError(t, err)
	require.Equal(t, []any{int32(1), int32(2)}, values)
}

func TestNextRowGroupDoesNotRequireUnreadEncryptedColumnKey(t *testing.T) {
	t.Parallel()

	footer := &parquet.FileMetaData{
		EncryptionAlgorithm: encryptionAlgorithm([]byte("prefix"), []byte("file-unique")),
		RowGroups: []*parquet.RowGroup{
			{Columns: []*parquet.ColumnChunk{encryptedTestColumnChunk("leaf")}},
			{Columns: []*parquet.ColumnChunk{encryptedTestColumnChunk("leaf")}},
		},
	}
	pr := &ParquetReader{Footer: footer}
	cbt := &ColumnBufferType{
		PFile:         buffer.NewBufferReaderFromBytesNoAlloc(make([]byte, 64)),
		Footer:        footer,
		SchemaHandler: newSchemaHandlerWithPath("leaf"),
		PathStr:       "root.leaf",
		RowGroupIndex: 1,
		Reader:        pr,
	}

	require.NoError(t, cbt.NextRowGroup())
	require.Nil(t, cbt.PageReadOptions.Decryptor)
}

func TestReadFooterPlaintextFooterFooterKeyColumnMetadata(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	aadPrefix := []byte("table/part-0")
	fileUnique := []byte("file-unique")
	footer, placeholderMeta, wantColumnMeta := fileMetaDataWithFooterKeyEncryptedColumnMetadataAndPlaintextPlaceholder(
		t,
		footerKey,
		aadPrefix,
		fileUnique,
	)
	file := buildPlaintextEncryptedFooterFile(t, footerKey, aadPrefix, fileUnique, footer)

	noKeyReader := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
	require.NoError(t, noKeyReader.ReadFooter())
	require.True(t, placeholderMeta.Equals(noKeyReader.Footer.RowGroups[0].Columns[0].MetaData))

	withFooterKey := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
	applyReaderOptionsForTest(t, withFooterKey, WithFooterKey(footerKey))
	require.NoError(t, withFooterKey.ReadFooter())
	require.True(t, wantColumnMeta.Equals(withFooterKey.Footer.RowGroups[0].Columns[0].MetaData))
}

func TestReadEncryptedColumnWithoutKeyFailsBeforePageRead(t *testing.T) {
	t.Parallel()

	cbt := &ColumnBufferType{
		ChunkHeader: &parquet.ColumnChunk{
			MetaData: &parquet.ColumnMetaData{
				NumValues:      1,
				PathInSchema:   []string{"leaf"},
				DataPageOffset: 4,
			},
			CryptoMetadata: &parquet.ColumnCryptoMetaData{
				ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{PathInSchema: []string{"leaf"}},
			},
		},
		Reader: &ParquetReader{},
	}

	require.ErrorContains(t, cbt.ReadPage(), "decryption key required for column leaf")
}

func TestReadPageForSkipEncryptedColumnWithoutKeyFailsBeforePageRead(t *testing.T) {
	t.Parallel()

	cbt := &ColumnBufferType{
		ChunkHeader: &parquet.ColumnChunk{
			MetaData: &parquet.ColumnMetaData{
				NumValues:      1,
				PathInSchema:   []string{"leaf"},
				DataPageOffset: 4,
			},
			CryptoMetadata: &parquet.ColumnCryptoMetaData{
				ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{PathInSchema: []string{"leaf"}},
			},
		},
		Reader: &ParquetReader{},
	}

	_, err := cbt.ReadPageForSkip()
	require.ErrorContains(t, err, "decryption key required for column leaf")
}

func TestReadFooterDecryptsEncryptedColumnMetadata(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	columnKey := []byte("abcdef0123456789")
	aadPrefix := []byte("table/part-0")
	fileUnique := []byte("file-unique")
	footer, wantColumnMeta := fileMetaDataWithEncryptedColumnMetadata(t, columnKey, aadPrefix, fileUnique)
	file := buildEncryptedFooterFile(t, footerKey, aadPrefix, fileUnique, footer)

	pr := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
	applyReaderOptionsForTest(t, pr, WithFooterKey(footerKey), WithColumnKey("leaf", columnKey))

	require.NoError(t, pr.ReadFooter())
	require.True(t, wantColumnMeta.Equals(pr.Footer.RowGroups[0].Columns[0].MetaData))
}

func TestReadFooterFooterKeyPrecedence(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	wrongFooterKey := []byte("abcdef0123456789")
	aadPrefix := []byte("table/part-0")
	fileUnique := []byte("file-unique")
	keyMetadata := []byte("footer-key")
	footer, wantColumnMeta := fileMetaDataWithFooterKeyEncryptedColumnMetadata(t, footerKey, aadPrefix, fileUnique)
	file := buildEncryptedFooterFileWithKeyMetadata(t, footerKey, keyMetadata, aadPrefix, fileUnique, footer)

	pr := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
	// WithFooterKey wins: retriever returning wrongFooterKey is ignored
	applyReaderOptionsForTest(t, pr, WithFooterKey(footerKey), WithKeyRetriever(func([]byte) ([]byte, error) {
		return wrongFooterKey, nil
	}))

	require.NoError(t, pr.ReadFooter())
	require.True(t, wantColumnMeta.Equals(pr.Footer.RowGroups[0].Columns[0].MetaData))
}

func TestReadFooterReusesRetrievedFooterKeyForColumnMetadata(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	aadPrefix := []byte("table/part-0")
	fileUnique := []byte("file-unique")
	keyMetadata := []byte("footer-key")
	footer, wantColumnMeta := fileMetaDataWithFooterKeyEncryptedColumnMetadata(t, footerKey, aadPrefix, fileUnique)
	file := buildEncryptedFooterFileWithKeyMetadata(t, footerKey, keyMetadata, aadPrefix, fileUnique, footer)

	pr := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
	applyReaderOptionsForTest(t, pr, WithKeyRetriever(func(got []byte) ([]byte, error) {
		if string(got) != string(keyMetadata) {
			return nil, fmt.Errorf("unexpected key metadata: %q", got)
		}
		return footerKey, nil
	}))

	require.NoError(t, pr.ReadFooter())
	require.True(t, wantColumnMeta.Equals(pr.Footer.RowGroups[0].Columns[0].MetaData))
}

func TestKeyRetrieverCachesColumnKeyByMetadata(t *testing.T) {
	t.Parallel()

	key := []byte("abcdef0123456789")
	keyMetadata := []byte{0x00, 0x01, 'k', 'e', 'y'}
	var calls int32
	pr := &ParquetReader{}
	applyReaderOptionsForTest(t, pr, WithKeyRetriever(func(got []byte) ([]byte, error) {
		require.Equal(t, keyMetadata, got)
		atomic.AddInt32(&calls, 1)
		return key, nil
	}))
	chunk := &parquet.ColumnChunk{
		CryptoMetadata: &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{
				PathInSchema: []string{"leaf"},
				KeyMetadata:  keyMetadata,
			},
		},
	}

	got, err := pr.resolveColumnKey(chunk)
	require.NoError(t, err)
	require.Equal(t, key, got)
	got[0] = 'x'

	got, err = pr.resolveColumnKey(chunk)
	require.NoError(t, err)
	require.Equal(t, key, got)
	require.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

func TestKeyRetrieverCacheIsConcurrentSafe(t *testing.T) {
	t.Parallel()

	key := []byte("abcdef0123456789")
	keyMetadata := []byte("shared-key")
	var calls int32
	pr := &ParquetReader{}
	applyReaderOptionsForTest(t, pr, WithKeyRetriever(func(got []byte) ([]byte, error) {
		require.Equal(t, keyMetadata, got)
		atomic.AddInt32(&calls, 1)
		return key, nil
	}))
	chunk := &parquet.ColumnChunk{
		CryptoMetadata: &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{
				PathInSchema: []string{"leaf"},
				KeyMetadata:  keyMetadata,
			},
		},
	}

	const goroutines = 32
	start := make(chan struct{})
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			<-start
			got, err := pr.resolveColumnKey(chunk)
			if err != nil {
				errs <- err
				return
			}
			if string(got) != string(key) {
				errs <- fmt.Errorf("unexpected key: %q", got)
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

func TestKeyRetrieverCacheDoesNotRetryError(t *testing.T) {
	t.Parallel()

	retrieverErr := errors.New("temporary kms failure")
	var calls int32
	pr := &ParquetReader{}
	applyReaderOptionsForTest(t, pr, WithKeyRetriever(func([]byte) ([]byte, error) {
		atomic.AddInt32(&calls, 1)
		return nil, retrieverErr
	}))
	chunk := &parquet.ColumnChunk{
		CryptoMetadata: &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{
				PathInSchema: []string{"leaf"},
				KeyMetadata:  []byte("shared-key"),
			},
		},
	}

	_, err := pr.resolveColumnKey(chunk)
	require.ErrorIs(t, err, retrieverErr)
	_, err = pr.resolveColumnKey(chunk)
	require.ErrorIs(t, err, retrieverErr)
	require.Equal(t, int32(1), atomic.LoadInt32(&calls))
}

func TestReadFooterEncryptedColumnMetadataMissingKey(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	columnKey := []byte("abcdef0123456789")
	aadPrefix := []byte("table/part-0")
	fileUnique := []byte("file-unique")
	footer, _ := fileMetaDataWithEncryptedColumnMetadata(t, columnKey, aadPrefix, fileUnique)
	file := buildEncryptedFooterFile(t, footerKey, aadPrefix, fileUnique, footer)

	pr := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
	applyReaderOptionsForTest(t, pr, WithFooterKey(footerKey))

	err := pr.ReadFooter()
	require.ErrorContains(t, err, "decryption key required for column leaf")
}

func TestNewColumnBufferConfiguresPageDecryptor(t *testing.T) {
	t.Parallel()

	key := []byte("abcdef0123456789")
	aadPrefix := []byte("prefix")
	fileUnique := []byte("file-unique")
	pr := &ParquetReader{
		PFile: buffer.NewBufferReaderFromBytesNoAlloc(make([]byte, 64)),
		Footer: &parquet.FileMetaData{
			Schema: []*parquet.SchemaElement{
				{Name: "root", NumChildren: int32Ptr(1)},
				{Name: "leaf", Type: parquet.TypePtr(parquet.Type_INT32), RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)},
			},
			RowGroups: []*parquet.RowGroup{
				{
					Columns: []*parquet.ColumnChunk{
						{
							MetaData: &parquet.ColumnMetaData{
								Type:                  parquet.Type_INT32,
								PathInSchema:          []string{"leaf"},
								Codec:                 parquet.CompressionCodec_UNCOMPRESSED,
								DataPageOffset:        0,
								TotalCompressedSize:   0,
								TotalUncompressedSize: 0,
							},
							CryptoMetadata: &parquet.ColumnCryptoMetaData{
								ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{PathInSchema: []string{"leaf"}},
							},
						},
					},
				},
			},
		},
		FileCrypto: &parquet.FileCryptoMetaData{
			EncryptionAlgorithm: &parquet.EncryptionAlgorithm{
				AES_GCM_V1: &parquet.AesGcmV1{
					AadPrefix:     aadPrefix,
					AadFileUnique: fileUnique,
				},
			},
		},
		SchemaHandler: newSchemaHandlerWithPath("leaf"),
	}
	applyReaderOptionsForTest(t, pr, WithColumnKey("leaf", key))

	cb, err := pr.newColumnBuffer("root.leaf")
	require.NoError(t, err)
	require.NotNil(t, cb.PageReadOptions.Decryptor)
	require.Equal(t, key, cb.PageReadOptions.Decryptor.Key)
}

func TestPageInspectionRejectsEncryptedColumnsWithoutKeys(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	data := buildPlaintextFooterEncryptedColumnData(t, footerKey, []byte("abcdef0123456789"))

	pr, err := NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(data), new(encryptedReaderRecord))
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	nameColumn := encryptedColumnIndex(t, pr, "name")

	_, err = pr.GetAllPageHeaders(0, nameColumn)
	require.ErrorContains(t, err, "decryption key required for column name")

	_, err = pr.GetFirstDataPageHeader(0, nameColumn)
	require.ErrorContains(t, err, "decryption key required for column name")

	_, err = pr.ReadDictionaryPageValuesInColumn(0, nameColumn)
	require.ErrorContains(t, err, "decryption key required for column name")
}

func TestReadDictionaryPageValuesByOffsetRejectsEncryptedOffset(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{
		Footer: &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{
					Columns: []*parquet.ColumnChunk{
						{
							MetaData: &parquet.ColumnMetaData{
								Type:                 parquet.Type_INT32,
								PathInSchema:         []string{"leaf"},
								Codec:                parquet.CompressionCodec_UNCOMPRESSED,
								DataPageOffset:       8,
								DictionaryPageOffset: int64Ptr(4),
							},
							CryptoMetadata: &parquet.ColumnCryptoMetaData{
								ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey(),
							},
						},
					},
				},
			},
		},
	}

	_, err := pr.ReadDictionaryPageValues(4, parquet.CompressionCodec_UNCOMPRESSED, parquet.Type_INT32)
	require.ErrorContains(t, err, "ReadDictionaryPageValuesInColumn")

	encrypted, err := (*ParquetReader)(nil).pageOffsetEncrypted(4)
	require.NoError(t, err)
	require.False(t, encrypted)

	encrypted, err = (&ParquetReader{}).pageOffsetEncrypted(4)
	require.NoError(t, err)
	require.False(t, encrypted)
}

func TestPageInspectionDecryptsEncryptedColumns(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")

	algorithms := []struct {
		name    string
		algoOpt writer.WriterOption
	}{
		{name: "AES-GCM", algoOpt: writer.WithEncryptionAlgorithm(writer.EncryptionAESGCMV1)},
		{name: "AES-GCM-CTR", algoOpt: writer.WithEncryptionAlgorithm(writer.EncryptionAESGCMCTRV1)},
	}

	for _, algo := range algorithms {
		algo := algo
		t.Run(algo.name, func(t *testing.T) {
			t.Parallel()

			data := buildEncryptedInspectionFile(t, footerKey, nameKey, algo.algoOpt)

			pr, err := NewParquetReader(
				buffer.NewBufferReaderFromBytesNoAlloc(data),
				new(encryptedReaderRecord),
				WithFooterKey(footerKey),
				WithColumnKey("name", nameKey),
			)
			require.NoError(t, err)
			defer func() { require.NoError(t, pr.ReadStop()) }()

			nameColumn := encryptedColumnIndex(t, pr, "name")

			headers, err := pr.GetAllPageHeaders(0, nameColumn)
			require.NoError(t, err)
			require.NotEmpty(t, headers)

			var seenDataPage, seenDictPage bool
			for _, h := range headers {
				require.Positive(t, h.CompressedSize)
				require.Positive(t, h.UncompressedSize)
				switch h.PageType {
				case parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2:
					seenDataPage = true
				case parquet.PageType_DICTIONARY_PAGE:
					seenDictPage = true
				}
			}
			require.True(t, seenDictPage)
			require.True(t, seenDataPage)

			firstData, err := pr.GetFirstDataPageHeader(0, nameColumn)
			require.NoError(t, err)
			require.Contains(t, []parquet.PageType{parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2}, firstData.PageType)

			values, err := pr.ReadDictionaryPageValuesInColumn(0, nameColumn)
			require.NoError(t, err)
			require.NotEmpty(t, values)
			seen := map[string]bool{}
			for _, v := range values {
				str, ok := v.(string)
				require.True(t, ok)
				seen[str] = true
			}
			require.True(t, seen["alpha"] || seen["beta"] || seen["gamma"])
		})
	}
}

func TestReadDictionaryPageValuesInColumnErrors(t *testing.T) {
	t.Parallel()

	t.Run("invalid row group index", func(t *testing.T) {
		t.Parallel()
		pr := &ParquetReader{Footer: &parquet.FileMetaData{RowGroups: []*parquet.RowGroup{{Columns: []*parquet.ColumnChunk{{}}}}}}
		_, err := pr.ReadDictionaryPageValuesInColumn(-1, 0)
		require.ErrorContains(t, err, "invalid row group index")
		_, err = pr.ReadDictionaryPageValuesInColumn(99, 0)
		require.ErrorContains(t, err, "invalid row group index")
	})

	t.Run("invalid column index", func(t *testing.T) {
		t.Parallel()
		pr := &ParquetReader{Footer: &parquet.FileMetaData{RowGroups: []*parquet.RowGroup{{Columns: []*parquet.ColumnChunk{{}}}}}}
		_, err := pr.ReadDictionaryPageValuesInColumn(0, -1)
		require.ErrorContains(t, err, "invalid column index")
		_, err = pr.ReadDictionaryPageValuesInColumn(0, 99)
		require.ErrorContains(t, err, "invalid column index")
	})

	t.Run("nil column metadata", func(t *testing.T) {
		t.Parallel()
		pr := &ParquetReader{Footer: &parquet.FileMetaData{RowGroups: []*parquet.RowGroup{{Columns: []*parquet.ColumnChunk{{}}}}}}
		_, err := pr.ReadDictionaryPageValuesInColumn(0, 0)
		require.ErrorContains(t, err, "metadata is nil")
	})

	t.Run("no dictionary page", func(t *testing.T) {
		t.Parallel()
		pr := &ParquetReader{
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{
					{Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{Type: parquet.Type_INT32, PathInSchema: []string{"leaf"}, DataPageOffset: 8}}}},
				},
			},
		}
		_, err := pr.ReadDictionaryPageValuesInColumn(0, 0)
		require.ErrorContains(t, err, "does not have a dictionary page")
	})

	t.Run("offset does not point to dictionary page", func(t *testing.T) {
		t.Parallel()
		testFile := getTestParquetFile(t)
		buf, err := local.NewLocalFileReader(testFile)
		require.NoError(t, err)
		pr, err := NewParquetReader(buf, new(TestPageRecord), WithNP(1))
		require.NoError(t, err)
		defer func() { _ = pr.ReadStop() }()

		// Forge a dictionary page offset that actually points to a data page,
		// so the read succeeds but the page type mismatches.
		dataPageOffset := pr.Footer.RowGroups[0].Columns[0].MetaData.GetDataPageOffset()
		pr.Footer.RowGroups[0].Columns[0].MetaData.DictionaryPageOffset = &dataPageOffset
		_, err = pr.ReadDictionaryPageValuesInColumn(0, 0)
		require.ErrorContains(t, err, "expected dictionary page")
	})
}

func TestPageInspectionDecryptorErrors(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{}
	column := &parquet.ColumnChunk{
		CryptoMetadata: &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey(),
		},
	}

	_, err := pr.pageInspectionDecryptor(nil, column, 0, 0)
	require.ErrorContains(t, err, "encrypted column missing file encryption algorithm")
}

func TestPageInspectionDecryptorAADPrefixError(t *testing.T) {
	t.Parallel()

	// Algorithm requires the supplied AAD prefix but the reader was not
	// configured with one, so footerAADParts returns an error.
	pr := &ParquetReader{
		FileCrypto: &parquet.FileCryptoMetaData{
			EncryptionAlgorithm: &parquet.EncryptionAlgorithm{
				AES_GCM_V1: &parquet.AesGcmV1{
					AadFileUnique:   []byte("file-unique"),
					SupplyAadPrefix: boolPtr(true),
				},
			},
		},
	}
	column := &parquet.ColumnChunk{
		CryptoMetadata: &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey(),
		},
	}

	_, err := pr.pageInspectionDecryptor(nil, column, 0, 0)
	require.ErrorContains(t, err, "AAD prefix is required")
}

func TestPageBodyDiskSize(t *testing.T) {
	t.Parallel()

	t.Run("plaintext returns CompressedPageSize", func(t *testing.T) {
		t.Parallel()
		header := &parquet.PageHeader{CompressedPageSize: 17}
		size, err := pageBodyDiskSize(bytes.NewReader(nil), header, nil)
		require.NoError(t, err)
		require.Equal(t, int64(17), size)
	})

	t.Run("encrypted reads length prefix", func(t *testing.T) {
		t.Parallel()
		var prefix [4]byte
		binary.LittleEndian.PutUint32(prefix[:], 123)
		size, err := pageBodyDiskSize(bytes.NewReader(prefix[:]), &parquet.PageHeader{}, &layout.PageDecryptor{})
		require.NoError(t, err)
		require.Equal(t, int64(4+123), size)
	})

	t.Run("length too short to read", func(t *testing.T) {
		t.Parallel()
		_, err := pageBodyDiskSize(bytes.NewReader([]byte{1, 2}), &parquet.PageHeader{}, &layout.PageDecryptor{})
		require.ErrorContains(t, err, "read encrypted page body length")
	})

	t.Run("length exceeds max page size", func(t *testing.T) {
		t.Parallel()
		var prefix [4]byte
		binary.LittleEndian.PutUint32(prefix[:], uint32(layout.DefaultMaxPageSize+1))
		_, err := pageBodyDiskSize(bytes.NewReader(prefix[:]), &parquet.PageHeader{}, &layout.PageDecryptor{})
		require.ErrorContains(t, err, "exceeds limit")
	})
}

func TestReadPageBodyErrors(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	decryptor := &layout.PageDecryptor{
		Algorithm:     layout.PageEncryptionAESGCM,
		Key:           key,
		AADPrefix:     []byte("prefix"),
		AADFileUnique: []byte("file-unique"),
	}

	t.Run("read encrypted body fails", func(t *testing.T) {
		t.Parallel()
		// Empty buffer — encryption.ReadModule will fail reading the length.
		_, err := readPageBody(bytes.NewReader(nil), 0, &parquet.PageHeader{Type: parquet.PageType_DICTIONARY_PAGE}, parquet.CompressionCodec_UNCOMPRESSED, common.CRCIgnore, decryptor)
		require.ErrorContains(t, err, "read encrypted page body")
	})

	t.Run("decrypt fails with wrong key", func(t *testing.T) {
		t.Parallel()
		header := &parquet.PageHeader{Type: parquet.PageType_DICTIONARY_PAGE}
		// Build a well-formed module encrypted with the right key, then try to
		// decrypt with a different decryptor key so DecryptGCM fails the tag.
		aad := encryption.AAD(decryptor.AADPrefix, decryptor.AADFileUnique, encryption.ModuleDictionaryPage, 0, 0, 0)
		mod, err := encryption.EncryptGCM(key, aad, []byte("hello"))
		require.NoError(t, err)
		encoded, err := encryption.EncodeModule(mod)
		require.NoError(t, err)

		wrongKeyDecryptor := *decryptor
		wrongKeyDecryptor.Key = []byte("abcdef0123456789")
		_, err = readPageBody(bytes.NewReader(encoded), 0, header, parquet.CompressionCodec_UNCOMPRESSED, common.CRCIgnore, &wrongKeyDecryptor)
		require.ErrorContains(t, err, "decrypt page body")
	})

	t.Run("decompress fails on bogus data", func(t *testing.T) {
		t.Parallel()
		// Plaintext path: feed bytes that aren't valid GZIP.
		header := &parquet.PageHeader{CompressedPageSize: 8, UncompressedPageSize: 64}
		buf := make([]byte, 8) // not a valid gzip stream
		_, err := readPageBody(bytes.NewReader(buf), 0, header, parquet.CompressionCodec_GZIP, common.CRCIgnore, nil)
		require.ErrorContains(t, err, "decompress page data")
	})
}

func TestReadPageHeaderEncryptedErrors(t *testing.T) {
	t.Parallel()

	decryptor := &layout.PageDecryptor{
		Algorithm:     layout.PageEncryptionAESGCM,
		Key:           []byte("0123456789abcdef"),
		AADPrefix:     []byte("prefix"),
		AADFileUnique: []byte("file-unique"),
	}

	t.Run("read module length fails", func(t *testing.T) {
		t.Parallel()
		_, _, err := readPageHeader(bytes.NewReader(nil), 0, decryptor)
		require.ErrorContains(t, err, "read encrypted page header module")
	})

	t.Run("decrypt fails", func(t *testing.T) {
		t.Parallel()
		// Build a module encrypted with a different key so DecryptPageHeader fails.
		aad := encryption.AAD(decryptor.AADPrefix, decryptor.AADFileUnique, encryption.ModuleDataPageHeader, 0, 0, 0)
		header := &parquet.PageHeader{
			Type:                 parquet.PageType_DATA_PAGE,
			CompressedPageSize:   4,
			UncompressedPageSize: 4,
			DataPageHeader:       &parquet.DataPageHeader{NumValues: 1, Encoding: parquet.Encoding_PLAIN, DefinitionLevelEncoding: parquet.Encoding_RLE, RepetitionLevelEncoding: parquet.Encoding_RLE},
		}
		plain := serializeThrift(t, header)
		mod, err := encryption.EncryptGCM([]byte("abcdef0123456789"), aad, plain)
		require.NoError(t, err)
		encoded, err := encryption.EncodeModule(mod)
		require.NoError(t, err)

		_, _, err = readPageHeader(bytes.NewReader(encoded), 0, decryptor)
		require.ErrorContains(t, err, "decrypt page header")
	})
}

func TestPageInspectionDecryptorUsesRowGroupOrdinal(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	pr := &ParquetReader{
		FileCrypto: &parquet.FileCryptoMetaData{
			EncryptionAlgorithm: &parquet.EncryptionAlgorithm{
				AES_GCM_V1: &parquet.AesGcmV1{
					AadPrefix:     []byte("prefix"),
					AadFileUnique: []byte("file-unique"),
				},
			},
		},
	}
	applyReaderOptionsForTest(t, pr, WithFooterKey(key))

	column := &parquet.ColumnChunk{
		CryptoMetadata: &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey(),
		},
	}
	rg := &parquet.RowGroup{Ordinal: int16Ptr(42)}

	dec, err := pr.pageInspectionDecryptor(rg, column, 1, 2)
	require.NoError(t, err)
	require.NotNil(t, dec)
	require.Equal(t, int16(42), dec.RowGroupOrdinal)
	require.Equal(t, int16(2), dec.ColumnOrdinal)
}

// buildEncryptedInspectionFile writes a small encrypted parquet file with both
// a dictionary page and at least one data page per encrypted column, so the
// page-inspection helpers have something to walk.
func buildEncryptedInspectionFile(t *testing.T, footerKey, nameKey []byte, algoOpt writer.WriterOption) []byte {
	t.Helper()

	var out bytes.Buffer
	pw, err := writer.NewParquetWriter(
		writerfile.NewWriterFile(&out),
		new(encryptedReaderRecord),
		writer.WithNP(1),
		writer.WithRowGroupSize(128),
		writer.WithPageSize(32),
		writer.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		algoOpt,
		writer.WithFooterKey(footerKey, []byte("footer-key")),
		writer.WithColumnEncrypted("name", writer.ColumnKey(nameKey, []byte("name-key"))),
		writer.WithAADPrefix([]byte("reader-test")),
		writer.WithAADFileUnique([]byte("reader-test-001")),
	)
	require.NoError(t, err)
	// Small page size + multiple unique names ensures the dictionary is written
	// and we get more than one data page after it.
	require.NoError(t, pw.Write(encryptedReaderRecord{ID: 1, Name: "alpha"}))
	require.NoError(t, pw.Write(encryptedReaderRecord{ID: 2, Name: "beta"}))
	require.NoError(t, pw.Write(encryptedReaderRecord{ID: 3, Name: "gamma"}))
	require.NoError(t, pw.Write(encryptedReaderRecord{ID: 4, Name: "alpha"}))
	require.NoError(t, pw.Write(encryptedReaderRecord{ID: 5, Name: "beta"}))
	require.NoError(t, pw.Write(encryptedReaderRecord{ID: 6, Name: "gamma"}))
	require.NoError(t, pw.WriteStop())
	return out.Bytes()
}

// encryptedColumnIndex finds the leaf-name column in the first row group.
func encryptedColumnIndex(t *testing.T, pr *ParquetReader, leaf string) int {
	t.Helper()
	for i, c := range pr.Footer.RowGroups[0].Columns {
		path := c.MetaData.GetPathInSchema()
		if len(path) > 0 && strings.EqualFold(path[len(path)-1], leaf) {
			return i
		}
	}
	t.Fatalf("column with leaf %q not found", leaf)
	return -1
}

func TestPageOffsetEncryptedCachesAllEncryptedOffsets(t *testing.T) {
	t.Parallel()

	const (
		dictionaryPageOffset int64 = 4
		dataPageOffset       int64 = 8
		indexPageOffset      int64 = 12
		bloomFilterOffset    int64 = 16
	)

	column := encryptedOffsetTestColumn(dictionaryPageOffset, dataPageOffset)
	column.MetaData.IndexPageOffset = int64Ptr(indexPageOffset)
	column.MetaData.BloomFilterOffset = int64Ptr(bloomFilterOffset)

	pr := &ParquetReader{
		Footer: &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{Columns: []*parquet.ColumnChunk{column}},
			},
		},
	}

	for _, offset := range []int64{dictionaryPageOffset, dataPageOffset, indexPageOffset, bloomFilterOffset} {
		encrypted, err := pr.pageOffsetEncrypted(offset)
		require.NoError(t, err)
		require.True(t, encrypted)
	}
	require.Len(t, pr.encryptedPageOffsets, 4)
}

func TestPageOffsetEncryptedBuildsAfterFooterSet(t *testing.T) {
	t.Parallel()

	const (
		dictionaryPageOffset int64 = 4
		dataPageOffset       int64 = 8
	)

	pr := &ParquetReader{}
	encrypted, err := pr.pageOffsetEncrypted(dictionaryPageOffset)
	require.NoError(t, err)
	require.False(t, encrypted)
	require.Nil(t, pr.encryptedPageOffsets)

	pr.Footer = &parquet.FileMetaData{
		RowGroups: []*parquet.RowGroup{
			{Columns: []*parquet.ColumnChunk{encryptedOffsetTestColumn(dictionaryPageOffset, dataPageOffset)}},
		},
	}
	encrypted, err = pr.pageOffsetEncrypted(dictionaryPageOffset)
	require.NoError(t, err)
	require.True(t, encrypted)
	encrypted, err = pr.pageOffsetEncrypted(dataPageOffset)
	require.NoError(t, err)
	require.True(t, encrypted)
	require.Len(t, pr.encryptedPageOffsets, 2)
}

func TestPageOffsetEncryptedSkipsDeferredColumnMetadata(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{
		Footer: &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{Columns: []*parquet.ColumnChunk{{
					CryptoMetadata: &parquet.ColumnCryptoMetaData{
						ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey(),
					},
					EncryptedColumnMetadata: []byte("opaque-ciphertext"),
				}}},
			},
		},
	}

	encrypted, err := pr.pageOffsetEncrypted(0)
	require.NoError(t, err)
	require.False(t, encrypted)
	require.Empty(t, pr.encryptedPageOffsets)
}

func TestPageOffsetEncryptedFailsOnCorruptedColumn(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{
		Footer: &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{Columns: []*parquet.ColumnChunk{{
					CryptoMetadata: &parquet.ColumnCryptoMetaData{
						ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey(),
					},
				}}},
			},
		},
	}

	_, err := pr.pageOffsetEncrypted(0)
	require.ErrorContains(t, err, "encrypted column is missing metadata")

	// Error is cached across calls.
	_, err2 := pr.pageOffsetEncrypted(123)
	require.Equal(t, err, err2)

	_, err = pr.ReadDictionaryPageValues(0, parquet.CompressionCodec_UNCOMPRESSED, parquet.Type_INT32)
	require.ErrorContains(t, err, "encrypted column is missing metadata")
}

// BenchmarkPageOffsetEncrypted exercises the post-build lookup path against a
// large encrypted footer. The cache makes each lookup O(1); a regression that
// reintroduces per-call footer scans would make each iteration O(R*C) and
// produce a dramatic slowdown here.
func BenchmarkPageOffsetEncrypted(b *testing.B) {
	const (
		numRowGroups = 1000
		numColumns   = 100
	)

	rowGroups := make([]*parquet.RowGroup, numRowGroups)
	offsets := make([]int64, 0, numRowGroups*numColumns*2)
	for rg := range numRowGroups {
		columns := make([]*parquet.ColumnChunk, numColumns)
		for c := range numColumns {
			base := int64(rg*numColumns+c) * 1024
			columns[c] = encryptedOffsetTestColumn(base, base+4)
			offsets = append(offsets, base, base+4)
		}
		rowGroups[rg] = &parquet.RowGroup{Columns: columns}
	}

	pr := &ParquetReader{
		Footer: &parquet.FileMetaData{RowGroups: rowGroups},
	}

	// Force the one-shot cache build before timing so the loop measures lookups only.
	if _, err := pr.pageOffsetEncrypted(offsets[0]); err != nil {
		b.Fatalf("cache build: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encrypted, err := pr.pageOffsetEncrypted(offsets[i%len(offsets)])
		if err != nil || !encrypted {
			b.Fatalf("offset %d: encrypted=%v err=%v", offsets[i%len(offsets)], encrypted, err)
		}
	}
}

func TestConfigurePageDecryptor(t *testing.T) {
	t.Parallel()

	key := []byte("abcdef0123456789")
	aadPrefix := []byte("prefix")
	fileUnique := []byte("file-unique")
	pr := &ParquetReader{
		FileCrypto: &parquet.FileCryptoMetaData{
			EncryptionAlgorithm: &parquet.EncryptionAlgorithm{
				AES_GCM_CTR_V1: &parquet.AesGcmCtrV1{
					AadPrefix:     aadPrefix,
					AadFileUnique: fileUnique,
				},
			},
		},
	}
	applyReaderOptionsForTest(t, pr, WithColumnKey("leaf", key))

	cbt := &ColumnBufferType{
		RowGroupIndex: 1,
		ChunkHeader: &parquet.ColumnChunk{
			CryptoMetadata: &parquet.ColumnCryptoMetaData{
				ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{PathInSchema: []string{"leaf"}},
			},
		},
	}
	rowGroup := &parquet.RowGroup{Ordinal: int16Ptr(7)}

	require.NoError(t, pr.configurePageDecryptor(cbt, rowGroup, 3))
	require.NotNil(t, cbt.PageReadOptions.Decryptor)
	require.Equal(t, layout.PageEncryptionAESGCMCTR, cbt.PageReadOptions.Decryptor.Algorithm)
	require.Equal(t, key, cbt.PageReadOptions.Decryptor.Key)
	require.Equal(t, aadPrefix, cbt.PageReadOptions.Decryptor.AADPrefix)
	require.Equal(t, fileUnique, cbt.PageReadOptions.Decryptor.AADFileUnique)
	require.Equal(t, int16(7), cbt.PageReadOptions.Decryptor.RowGroupOrdinal)
	require.Equal(t, int16(3), cbt.PageReadOptions.Decryptor.ColumnOrdinal)
}

func TestEncryptionHelperErrors(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{}
	_, err := pr.resolveColumnKey(&parquet.ColumnChunk{})
	require.ErrorContains(t, err, "column crypto metadata is required")

	_, err = pr.resolveColumnKey(&parquet.ColumnChunk{
		CryptoMetadata: &parquet.ColumnCryptoMetaData{},
	})
	require.ErrorContains(t, err, "unsupported column crypto metadata")

	_, err = pr.resolveColumnKey(&parquet.ColumnChunk{
		CryptoMetadata: &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{PathInSchema: []string{"leaf"}},
		},
	})
	require.ErrorContains(t, err, "decryption key required")

	retrieverErr := fmt.Errorf("kms failure")
	pr = &ParquetReader{}
	applyReaderOptionsForTest(t, pr, WithKeyRetriever(func([]byte) ([]byte, error) { return nil, retrieverErr }))
	_, err = pr.resolveColumnKey(&parquet.ColumnChunk{
		CryptoMetadata: &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{PathInSchema: []string{"leaf"}},
		},
	})
	require.ErrorContains(t, err, "retrieve column key")

	_, _, err = pr.footerAADParts(nil)
	require.ErrorContains(t, err, "missing encryption algorithm")

	_, _, err = pr.footerAADParts(&parquet.EncryptionAlgorithm{})
	require.ErrorContains(t, err, "unsupported encryption algorithm")

	require.NoError(t, (&ParquetReader{}).configurePageDecryptor(nil, nil, 0))

	cbt := &ColumnBufferType{}
	require.NoError(t, (&ParquetReader{}).configurePageDecryptor(cbt, nil, 0))
	require.Nil(t, cbt.PageReadOptions.Decryptor)

	cbt = &ColumnBufferType{
		ChunkHeader: &parquet.ColumnChunk{
			CryptoMetadata: &parquet.ColumnCryptoMetaData{ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey()},
		},
	}
	err = (&ParquetReader{}).configurePageDecryptor(cbt, nil, 0)
	require.ErrorContains(t, err, "encrypted column missing file encryption algorithm")

	require.NoError(t, (&ParquetReader{}).reconfigureDecryptorForBuffer(nil))
	require.NoError(t, (&ParquetReader{}).reconfigureDecryptorForBuffer(&ColumnBufferType{}))
	require.NoError(t, (&ParquetReader{Footer: &parquet.FileMetaData{}}).reconfigureDecryptorForBuffer(&ColumnBufferType{RowGroupIndex: 1}))

	require.NoError(t, (&ParquetReader{}).decryptEncryptedColumnMetadata())
	require.NoError(t, (&ParquetReader{Footer: &parquet.FileMetaData{RowGroups: []*parquet.RowGroup{nil}}}).decryptEncryptedColumnMetadata())
}

func TestOptionalKeyResolutionHelpers(t *testing.T) {
	t.Parallel()

	t.Run("footer key from retriever", func(t *testing.T) {
		t.Parallel()
		key := []byte("0123456789abcdef")
		pr := &ParquetReader{}
		applyReaderOptionsForTest(t, pr, WithKeyRetriever(func([]byte) ([]byte, error) { return key, nil }))

		got, err := pr.resolveOptionalFooterKeyFromMetadata([]byte("meta"))
		require.NoError(t, err)
		require.Equal(t, key, got)
		require.Equal(t, key, pr.resolvedFooterKey)
	})

	t.Run("footer key retriever returns empty", func(t *testing.T) {
		t.Parallel()
		pr := &ParquetReader{}
		applyReaderOptionsForTest(t, pr, WithKeyRetriever(func([]byte) ([]byte, error) { return nil, nil }))

		got, err := pr.resolveOptionalFooterKeyFromMetadata(nil)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("footer key retriever returns error", func(t *testing.T) {
		t.Parallel()
		retrieverErr := errors.New("kms unavailable")
		pr := &ParquetReader{}
		applyReaderOptionsForTest(t, pr, WithKeyRetriever(func([]byte) ([]byte, error) { return nil, retrieverErr }))

		_, err := pr.resolveOptionalFooterKeyFromMetadata(nil)
		require.ErrorIs(t, err, retrieverErr)
		require.ErrorContains(t, err, "retrieve footer key")
	})

	t.Run("optional column key nil crypto", func(t *testing.T) {
		t.Parallel()
		_, err := (&ParquetReader{}).resolveOptionalColumnKey(&parquet.ColumnChunk{})
		require.ErrorContains(t, err, "column crypto metadata is required")
	})

	t.Run("optional column key unsupported", func(t *testing.T) {
		t.Parallel()
		chunk := &parquet.ColumnChunk{CryptoMetadata: &parquet.ColumnCryptoMetaData{}}
		_, err := (&ParquetReader{}).resolveOptionalColumnKey(chunk)
		require.ErrorContains(t, err, "unsupported column crypto metadata")
	})

	t.Run("optional column key footer-key path with retriever error", func(t *testing.T) {
		t.Parallel()
		retrieverErr := errors.New("kms unavailable")
		pr := &ParquetReader{}
		applyReaderOptionsForTest(t, pr, WithKeyRetriever(func([]byte) ([]byte, error) { return nil, retrieverErr }))
		chunk := &parquet.ColumnChunk{
			CryptoMetadata: &parquet.ColumnCryptoMetaData{ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey()},
		}
		_, err := pr.resolveOptionalColumnKey(chunk)
		require.ErrorIs(t, err, retrieverErr)
	})

	t.Run("optional column key from retriever", func(t *testing.T) {
		t.Parallel()
		key := []byte("0123456789abcdef")
		pr := &ParquetReader{}
		applyReaderOptionsForTest(t, pr, WithKeyRetriever(func([]byte) ([]byte, error) { return key, nil }))
		chunk := &parquet.ColumnChunk{
			CryptoMetadata: &parquet.ColumnCryptoMetaData{
				ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{
					PathInSchema: []string{"leaf"},
					KeyMetadata:  []byte("meta"),
				},
			},
		}
		got, err := pr.resolveOptionalColumnKey(chunk)
		require.NoError(t, err)
		require.Equal(t, key, got)
	})

	t.Run("optional column key retriever returns empty", func(t *testing.T) {
		t.Parallel()
		pr := &ParquetReader{}
		applyReaderOptionsForTest(t, pr, WithKeyRetriever(func([]byte) ([]byte, error) { return nil, nil }))
		chunk := &parquet.ColumnChunk{
			CryptoMetadata: &parquet.ColumnCryptoMetaData{
				ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{PathInSchema: []string{"leaf"}},
			},
		}
		got, err := pr.resolveOptionalColumnKey(chunk)
		require.NoError(t, err)
		require.Empty(t, got)
	})

	t.Run("optional column key retriever returns error", func(t *testing.T) {
		t.Parallel()
		retrieverErr := errors.New("kms unavailable")
		pr := &ParquetReader{}
		applyReaderOptionsForTest(t, pr, WithKeyRetriever(func([]byte) ([]byte, error) { return nil, retrieverErr }))
		chunk := &parquet.ColumnChunk{
			CryptoMetadata: &parquet.ColumnCryptoMetaData{
				ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{PathInSchema: []string{"leaf"}},
			},
		}
		got, err := pr.resolveOptionalColumnKey(chunk)
		require.NoError(t, err)
		require.Empty(t, got)

		_, err = pr.resolveColumnKey(chunk)
		require.ErrorIs(t, err, retrieverErr)
		require.ErrorContains(t, err, "retrieve column key for leaf")
	})
}

func TestOptionalPageDecryptorHelpers(t *testing.T) {
	t.Parallel()

	t.Run("reconfigureOptionalDecryptorForBuffer guards", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, (&ParquetReader{}).reconfigureOptionalDecryptorForBuffer(nil))
		require.NoError(t, (&ParquetReader{}).reconfigureOptionalDecryptorForBuffer(&ColumnBufferType{}))
		require.NoError(t, (&ParquetReader{Footer: &parquet.FileMetaData{}}).reconfigureOptionalDecryptorForBuffer(&ColumnBufferType{RowGroupIndex: 1}))
	})

	t.Run("configurePageDecryptor missing column key", func(t *testing.T) {
		t.Parallel()
		pr := &ParquetReader{
			Footer: &parquet.FileMetaData{EncryptionAlgorithm: encryptionAlgorithm([]byte("prefix"), []byte("file-unique"))},
		}
		cbt := &ColumnBufferType{
			ChunkHeader: &parquet.ColumnChunk{
				CryptoMetadata: &parquet.ColumnCryptoMetaData{
					ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{PathInSchema: []string{"leaf"}},
				},
			},
		}
		err := pr.configurePageDecryptor(cbt, nil, 0)
		require.ErrorContains(t, err, "decryption key required")
		require.Nil(t, cbt.PageReadOptions.Decryptor)
	})
}

func TestReadBloomFilterForColumnErrors(t *testing.T) {
	t.Parallel()

	pr := &ParquetReader{}
	_, err := pr.readBloomFilterForColumn(nil, 0, 0, nil, nil)
	require.ErrorContains(t, err, "column metadata is nil")

	chunk := &parquet.ColumnChunk{
		MetaData:       &parquet.ColumnMetaData{},
		CryptoMetadata: &parquet.ColumnCryptoMetaData{ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey()},
	}
	_, err = pr.readBloomFilterForColumn(nil, 0, 0, nil, chunk)
	require.ErrorContains(t, err, "encrypted bloom filter missing file encryption algorithm")

	pr.Footer = &parquet.FileMetaData{EncryptionAlgorithm: encryptionAlgorithm([]byte("prefix"), []byte("file-unique"))}
	_, err = pr.readBloomFilterForColumn(nil, 0, 0, nil, chunk)
	require.ErrorContains(t, err, "decryption key required")
}

func minimalFileMetaData() *parquet.FileMetaData {
	return &parquet.FileMetaData{
		Version: 2,
		Schema: []*parquet.SchemaElement{
			{Name: "schema", NumChildren: int32Ptr(0)},
		},
		NumRows:   0,
		RowGroups: []*parquet.RowGroup{},
	}
}

func fileMetaDataWithEncryptedColumnMetadata(t *testing.T, columnKey, aadPrefix, fileUnique []byte) (*parquet.FileMetaData, *parquet.ColumnMetaData) {
	t.Helper()

	columnMeta := &parquet.ColumnMetaData{
		Type:                  parquet.Type_INT32,
		Encodings:             []parquet.Encoding{parquet.Encoding_PLAIN},
		PathInSchema:          []string{"leaf"},
		Codec:                 parquet.CompressionCodec_UNCOMPRESSED,
		NumValues:             0,
		TotalUncompressedSize: 0,
		TotalCompressedSize:   0,
		DataPageOffset:        4,
	}
	encryptedColumnMeta := encryptGCMModule(
		t,
		columnKey,
		encryption.AAD(aadPrefix, fileUnique, encryption.ModuleColumnMetaData, 0, 0, 0),
		serializeThrift(t, columnMeta),
	)
	return &parquet.FileMetaData{
		Version: 2,
		Schema: []*parquet.SchemaElement{
			{Name: "schema", NumChildren: int32Ptr(1)},
			{Name: "leaf", Type: parquet.TypePtr(parquet.Type_INT32), RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)},
		},
		NumRows: 0,
		RowGroups: []*parquet.RowGroup{
			{
				Columns: []*parquet.ColumnChunk{
					{
						FileOffset:              4,
						CryptoMetadata:          &parquet.ColumnCryptoMetaData{ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{PathInSchema: []string{"leaf"}}},
						EncryptedColumnMetadata: encryptedColumnMeta,
					},
				},
				TotalByteSize: 0,
				NumRows:       0,
				Ordinal:       int16Ptr(0),
			},
		},
	}, columnMeta
}

func fileMetaDataWithFooterKeyEncryptedColumnMetadata(t *testing.T, footerKey, aadPrefix, fileUnique []byte) (*parquet.FileMetaData, *parquet.ColumnMetaData) {
	t.Helper()

	columnMeta := &parquet.ColumnMetaData{
		Type:                  parquet.Type_INT32,
		Encodings:             []parquet.Encoding{parquet.Encoding_PLAIN},
		PathInSchema:          []string{"leaf"},
		Codec:                 parquet.CompressionCodec_UNCOMPRESSED,
		NumValues:             0,
		TotalUncompressedSize: 0,
		TotalCompressedSize:   0,
		DataPageOffset:        4,
	}
	encryptedColumnMeta := encryptGCMModule(
		t,
		footerKey,
		encryption.AAD(aadPrefix, fileUnique, encryption.ModuleColumnMetaData, 0, 0, 0),
		serializeThrift(t, columnMeta),
	)
	return &parquet.FileMetaData{
		Version: 2,
		Schema: []*parquet.SchemaElement{
			{Name: "schema", NumChildren: int32Ptr(1)},
			{Name: "leaf", Type: parquet.TypePtr(parquet.Type_INT32), RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)},
		},
		NumRows: 0,
		RowGroups: []*parquet.RowGroup{
			{
				Columns: []*parquet.ColumnChunk{
					{
						FileOffset:              4,
						CryptoMetadata:          &parquet.ColumnCryptoMetaData{ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey()},
						EncryptedColumnMetadata: encryptedColumnMeta,
					},
				},
				TotalByteSize: 0,
				NumRows:       0,
				Ordinal:       int16Ptr(0),
			},
		},
	}, columnMeta
}

func fileMetaDataWithMixedPlainAndEncryptedColumns(t *testing.T, columnKey, aadPrefix, fileUnique []byte) (*parquet.FileMetaData, *parquet.ColumnMetaData, *parquet.ColumnMetaData, *parquet.ColumnMetaData) {
	t.Helper()

	plaintextMeta := &parquet.ColumnMetaData{
		Type:                  parquet.Type_INT32,
		Encodings:             []parquet.Encoding{parquet.Encoding_PLAIN},
		PathInSchema:          []string{"plain_leaf"},
		Codec:                 parquet.CompressionCodec_UNCOMPRESSED,
		NumValues:             0,
		TotalUncompressedSize: 0,
		TotalCompressedSize:   0,
		DataPageOffset:        4,
	}
	encryptedPlaceholder := &parquet.ColumnMetaData{
		Type:                  parquet.Type_INT32,
		Encodings:             []parquet.Encoding{parquet.Encoding_PLAIN},
		PathInSchema:          []string{"encrypted_leaf"},
		Codec:                 parquet.CompressionCodec_UNCOMPRESSED,
		NumValues:             0,
		TotalUncompressedSize: 0,
		TotalCompressedSize:   0,
		DataPageOffset:        4,
	}
	encryptedMeta := &parquet.ColumnMetaData{
		Type:                  parquet.Type_INT32,
		Encodings:             []parquet.Encoding{parquet.Encoding_PLAIN},
		PathInSchema:          []string{"encrypted_leaf"},
		Codec:                 parquet.CompressionCodec_UNCOMPRESSED,
		NumValues:             7,
		TotalUncompressedSize: 11,
		TotalCompressedSize:   13,
		DataPageOffset:        4,
		Statistics:            &parquet.Statistics{NullCount: int64Ptr(1)},
	}
	encryptedColumnMeta := encryptGCMModule(
		t,
		columnKey,
		encryption.AAD(aadPrefix, fileUnique, encryption.ModuleColumnMetaData, 0, 1, 0),
		serializeThrift(t, encryptedMeta),
	)
	footer := &parquet.FileMetaData{
		Version:             2,
		EncryptionAlgorithm: encryptionAlgorithm(aadPrefix, fileUnique),
		Schema: []*parquet.SchemaElement{
			{Name: "schema", NumChildren: int32Ptr(2)},
			{Name: "plain_leaf", Type: parquet.TypePtr(parquet.Type_INT32), RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)},
			{Name: "encrypted_leaf", Type: parquet.TypePtr(parquet.Type_INT32), RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)},
		},
		NumRows: 0,
		RowGroups: []*parquet.RowGroup{
			{
				Columns: []*parquet.ColumnChunk{
					{FileOffset: 4, MetaData: plaintextMeta},
					{
						FileOffset:              4,
						MetaData:                encryptedPlaceholder,
						CryptoMetadata:          &parquet.ColumnCryptoMetaData{ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{PathInSchema: []string{"encrypted_leaf"}}},
						EncryptedColumnMetadata: encryptedColumnMeta,
					},
				},
				TotalByteSize: 0,
				NumRows:       0,
				Ordinal:       int16Ptr(0),
			},
		},
	}
	return footer, plaintextMeta, encryptedPlaceholder, encryptedMeta
}

func fileMetaDataWithFooterKeyEncryptedColumnMetadataAndPlaintextPlaceholder(t *testing.T, footerKey, aadPrefix, fileUnique []byte) (*parquet.FileMetaData, *parquet.ColumnMetaData, *parquet.ColumnMetaData) {
	t.Helper()

	placeholderMeta := &parquet.ColumnMetaData{
		Type:                  parquet.Type_INT32,
		Encodings:             []parquet.Encoding{parquet.Encoding_PLAIN},
		PathInSchema:          []string{"leaf"},
		Codec:                 parquet.CompressionCodec_UNCOMPRESSED,
		NumValues:             0,
		TotalUncompressedSize: 0,
		TotalCompressedSize:   0,
		DataPageOffset:        4,
	}
	columnMeta := &parquet.ColumnMetaData{
		Type:                  parquet.Type_INT32,
		Encodings:             []parquet.Encoding{parquet.Encoding_PLAIN},
		PathInSchema:          []string{"leaf"},
		Codec:                 parquet.CompressionCodec_UNCOMPRESSED,
		NumValues:             3,
		TotalUncompressedSize: 5,
		TotalCompressedSize:   7,
		DataPageOffset:        4,
		Statistics:            &parquet.Statistics{NullCount: int64Ptr(1)},
	}
	encryptedColumnMeta := encryptGCMModule(
		t,
		footerKey,
		encryption.AAD(aadPrefix, fileUnique, encryption.ModuleColumnMetaData, 0, 0, 0),
		serializeThrift(t, columnMeta),
	)
	return &parquet.FileMetaData{
		Version:             2,
		EncryptionAlgorithm: encryptionAlgorithm(aadPrefix, fileUnique),
		Schema: []*parquet.SchemaElement{
			{Name: "schema", NumChildren: int32Ptr(1)},
			{Name: "leaf", Type: parquet.TypePtr(parquet.Type_INT32), RepetitionType: parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)},
		},
		NumRows: 0,
		RowGroups: []*parquet.RowGroup{
			{
				Columns: []*parquet.ColumnChunk{
					{
						FileOffset:              4,
						MetaData:                placeholderMeta,
						CryptoMetadata:          &parquet.ColumnCryptoMetaData{ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey()},
						EncryptedColumnMetadata: encryptedColumnMeta,
					},
				},
				TotalByteSize: 0,
				NumRows:       0,
				Ordinal:       int16Ptr(0),
			},
		},
	}, placeholderMeta, columnMeta
}

func buildPlaintextFooterEncryptedColumnData(t *testing.T, footerKey, nameKey []byte) []byte {
	t.Helper()

	var out bytes.Buffer
	pw, err := writer.NewParquetWriter(
		writerfile.NewWriterFile(&out),
		new(encryptedReaderRecord),
		writer.WithNP(1),
		writer.WithRowGroupSize(128),
		writer.WithPageSize(32),
		writer.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		writer.WithPlaintextFooter(true),
		writer.WithFooterKey(footerKey, []byte("footer-key")),
		writer.WithColumnEncrypted("name", writer.ColumnKey(nameKey, []byte("name-key"))),
		writer.WithAADPrefix([]byte("reader-test")),
		writer.WithAADFileUnique([]byte("reader-test-001")),
	)
	require.NoError(t, err)
	require.NoError(t, pw.Write(encryptedReaderRecord{ID: 1, Name: "alpha"}))
	require.NoError(t, pw.Write(encryptedReaderRecord{ID: 2, Name: "beta"}))
	require.NoError(t, pw.WriteStop())
	return out.Bytes()
}

func buildMixedPlaintextFooterSuppliedAADData(t *testing.T, footerKey, nameKey, aadPrefix []byte) []byte {
	t.Helper()

	var out bytes.Buffer
	pw, err := writer.NewParquetWriter(
		writerfile.NewWriterFile(&out),
		new(encryptedReaderRecord),
		writer.WithNP(1),
		writer.WithRowGroupSize(128),
		writer.WithPageSize(32),
		writer.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		writer.WithPlaintextFooter(true),
		writer.WithFooterKey(footerKey, []byte("footer-key")),
		writer.WithPlaintextUnkeyedColumns(true),
		writer.WithColumnEncrypted("name", writer.ColumnKey(nameKey, []byte("name-key"))),
		writer.WithAADPrefix(aadPrefix),
		writer.WithSupplyAADPrefix(true),
		writer.WithAADFileUnique([]byte("reader-test-002")),
	)
	require.NoError(t, err)
	require.NoError(t, pw.Write(encryptedReaderRecord{ID: 1, Name: "alpha"}))
	require.NoError(t, pw.Write(encryptedReaderRecord{ID: 2, Name: "beta"}))
	require.NoError(t, pw.WriteStop())
	return out.Bytes()
}

func encryptedTestColumnChunk(path string) *parquet.ColumnChunk {
	return &parquet.ColumnChunk{
		MetaData: &parquet.ColumnMetaData{
			Type:                  parquet.Type_INT64,
			Encodings:             []parquet.Encoding{parquet.Encoding_PLAIN},
			PathInSchema:          []string{path},
			Codec:                 parquet.CompressionCodec_UNCOMPRESSED,
			NumValues:             0,
			TotalUncompressedSize: 0,
			TotalCompressedSize:   0,
			DataPageOffset:        0,
		},
		CryptoMetadata: &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{PathInSchema: []string{path}},
		},
	}
}

func encryptedOffsetTestColumn(dictionaryPageOffset, dataPageOffset int64) *parquet.ColumnChunk {
	return &parquet.ColumnChunk{
		MetaData: &parquet.ColumnMetaData{
			Type:                 parquet.Type_INT32,
			PathInSchema:         []string{"leaf"},
			Codec:                parquet.CompressionCodec_UNCOMPRESSED,
			DataPageOffset:       dataPageOffset,
			DictionaryPageOffset: int64Ptr(dictionaryPageOffset),
		},
		CryptoMetadata: &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey(),
		},
	}
}

func valueColumnPathWithLeaf(t *testing.T, pr *ParquetReader, leaf string) string {
	t.Helper()

	for _, path := range pr.SchemaHandler.ValueColumns {
		parts := common.StrToPath(path)
		if len(parts) > 0 && strings.EqualFold(parts[len(parts)-1], leaf) {
			return path
		}
	}
	t.Fatalf("value column with leaf %q not found in %v", leaf, pr.SchemaHandler.ValueColumns)
	return ""
}

func buildEncryptedFooterFile(t *testing.T, key, aadPrefix, fileUnique []byte, footer *parquet.FileMetaData) []byte {
	t.Helper()
	return buildEncryptedFooterFileWithAlgorithm(t, key, aadPrefix, fileUnique, footer, &parquet.EncryptionAlgorithm{
		AES_GCM_V1: &parquet.AesGcmV1{
			AadPrefix:     aadPrefix,
			AadFileUnique: fileUnique,
		},
	})
}

func buildPlainFooterFile(t *testing.T, footer *parquet.FileMetaData) []byte {
	t.Helper()

	footerBytes := serializeThrift(t, footer)
	file := append([]byte(common.MagicBytes), footerBytes...)
	var footerSize [4]byte
	binary.LittleEndian.PutUint32(footerSize[:], uint32(len(footerBytes)))
	file = append(file, footerSize[:]...)
	file = append(file, []byte(common.MagicBytes)...)
	return file
}

func buildEncryptedFooterFileWithKeyMetadata(t *testing.T, key, keyMetadata, aadPrefix, fileUnique []byte, footer *parquet.FileMetaData) []byte {
	t.Helper()

	footerBytes := serializeThrift(t, footer)
	algorithm := &parquet.EncryptionAlgorithm{
		AES_GCM_V1: &parquet.AesGcmV1{
			AadPrefix:     aadPrefix,
			AadFileUnique: fileUnique,
		},
	}
	cryptoMeta := &parquet.FileCryptoMetaData{EncryptionAlgorithm: algorithm, KeyMetadata: keyMetadata}
	cryptoMetaBytes := serializeThrift(t, cryptoMeta)
	encryptedFooter := encryptGCMModule(t, key, encryption.AAD(aadPrefix, fileUnique, encryption.ModuleFooter, 0, 0, 0), footerBytes)

	section := append(append([]byte{}, cryptoMetaBytes...), encryptedFooter...)
	file := append([]byte(common.MagicBytesEncrypted), section...)
	var footerSize [4]byte
	binary.LittleEndian.PutUint32(footerSize[:], uint32(len(section)))
	file = append(file, footerSize[:]...)
	file = append(file, []byte(common.MagicBytesEncrypted)...)
	return file
}

func buildEncryptedFooterFileWithAlgorithm(t *testing.T, key, aadPrefix, fileUnique []byte, footer *parquet.FileMetaData, algorithm *parquet.EncryptionAlgorithm) []byte {
	t.Helper()

	footerBytes := serializeThrift(t, footer)
	cryptoMeta := &parquet.FileCryptoMetaData{EncryptionAlgorithm: algorithm}
	cryptoMetaBytes := serializeThrift(t, cryptoMeta)
	encryptedFooter := encryptGCMModule(t, key, encryption.AAD(aadPrefix, fileUnique, encryption.ModuleFooter, 0, 0, 0), footerBytes)

	section := append(append([]byte{}, cryptoMetaBytes...), encryptedFooter...)
	file := append([]byte(common.MagicBytesEncrypted), section...)
	var footerSize [4]byte
	binary.LittleEndian.PutUint32(footerSize[:], uint32(len(section)))
	file = append(file, footerSize[:]...)
	file = append(file, []byte(common.MagicBytesEncrypted)...)
	return file
}

func buildPlaintextEncryptedFooterFile(t *testing.T, key, aadPrefix, fileUnique []byte, footer *parquet.FileMetaData) []byte {
	t.Helper()
	footerBytes := serializeThrift(t, footer)
	signature := signPlaintextFooter(t, key, encryption.AAD(aadPrefix, fileUnique, encryption.ModuleFooter, 0, 0, 0), footerBytes)
	section := append(append([]byte{}, footerBytes...), signature...)
	file := append([]byte(common.MagicBytes), section...)
	var footerSize [4]byte
	binary.LittleEndian.PutUint32(footerSize[:], uint32(len(section)))
	file = append(file, footerSize[:]...)
	file = append(file, []byte(common.MagicBytes)...)
	return file
}

func serializeThrift(t *testing.T, value thrift.TStruct) []byte {
	t.Helper()
	serializer := thrift.NewTSerializer()
	serializer.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(serializer.Transport)
	buf, err := serializer.Write(context.TODO(), value)
	require.NoError(t, err)
	return buf
}

func signPlaintextFooter(t *testing.T, key, aad, plaintext []byte) []byte {
	t.Helper()
	nonce := []byte("123456789012")
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	gcm, err := cipher.NewGCMWithNonceSize(block, len(nonce))
	require.NoError(t, err)
	sealed := gcm.Seal(nil, nonce, plaintext, aad)
	signature := append([]byte{}, nonce...)
	return append(signature, sealed[len(sealed)-16:]...)
}

func encryptGCMModule(t *testing.T, key, aad, plaintext []byte) []byte {
	t.Helper()
	nonce := []byte("123456789012")
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	gcm, err := cipher.NewGCMWithNonceSize(block, len(nonce))
	require.NoError(t, err)
	body := append(append([]byte{}, nonce...), gcm.Seal(nil, nonce, plaintext, aad)...)
	buf := make([]byte, 4, 4+len(body))
	binary.LittleEndian.PutUint32(buf, uint32(len(body)))
	return append(buf, body...)
}

func int32Ptr(v int32) *int32 {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}

func int16Ptr(v int16) *int16 {
	return &v
}

func int64Ptr(v int64) *int64 {
	return &v
}
