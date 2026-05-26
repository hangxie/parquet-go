package reader

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/buffer"
)

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
	WithFooterKey(key)(pr)

	require.NoError(t, pr.ReadFooter())
	require.NotNil(t, pr.FileCrypto)
	require.True(t, footer.Equals(pr.Footer))
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
			for _, opt := range tt.opts {
				opt(pr)
			}
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
		WithFooterKey(key)(pr)
		WithAADPrefix(aadPrefix)(pr)
		require.NoError(t, pr.ReadFooter())
		require.True(t, footer.Equals(pr.Footer))
	})

	t.Run("missing", func(t *testing.T) {
		t.Parallel()
		pr := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
		WithFooterKey(key)(pr)
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
	WithFooterKey(key)(pr)
	require.NoError(t, pr.ReadFooter())
	require.True(t, footer.Equals(pr.Footer))

	badKeyReader := &ParquetReader{PFile: buffer.NewBufferReaderFromBytesNoAlloc(file)}
	WithFooterKey([]byte("abcdef0123456789"))(badKeyReader)
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
	WithColumnKey("encrypted_leaf", columnKey)(withColumnKey)
	require.NoError(t, withColumnKey.ReadFooter())
	require.True(t, plaintextMeta.Equals(withColumnKey.Footer.RowGroups[0].Columns[0].MetaData))
	require.True(t, wantEncryptedMeta.Equals(withColumnKey.Footer.RowGroups[0].Columns[1].MetaData))
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
	WithFooterKey(footerKey)(withFooterKey)
	require.NoError(t, withFooterKey.ReadFooter())
	require.True(t, wantColumnMeta.Equals(withFooterKey.Footer.RowGroups[0].Columns[0].MetaData))
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
	WithFooterKey(footerKey)(pr)
	WithColumnKey("leaf", columnKey)(pr)

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
	WithFooterKey(footerKey)(pr)
	WithKeyRetriever(func([]byte) ([]byte, error) {
		return wrongFooterKey, nil
	})(pr)

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
	WithKeyRetriever(func(got []byte) ([]byte, error) {
		if string(got) != string(keyMetadata) {
			return nil, fmt.Errorf("unexpected key metadata: %q", got)
		}
		return footerKey, nil
	})(pr)

	require.NoError(t, pr.ReadFooter())
	require.True(t, wantColumnMeta.Equals(pr.Footer.RowGroups[0].Columns[0].MetaData))
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
	WithFooterKey(footerKey)(pr)

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
	WithColumnKey("leaf", key)(pr)

	cb, err := pr.newColumnBuffer("root.leaf")
	require.NoError(t, err)
	require.NotNil(t, cb.PageReadOptions.Decryptor)
	require.Equal(t, key, cb.PageReadOptions.Decryptor.Key)
}

func TestPageInspectionRejectsEncryptedColumns(t *testing.T) {
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

	_, err := pr.GetAllPageHeaders(0, 0)
	require.ErrorContains(t, err, "not supported for encrypted columns")

	_, err = pr.GetFirstDataPageHeader(0, 0)
	require.ErrorContains(t, err, "not supported for encrypted columns")

	_, err = pr.ReadDictionaryPageValues(4, parquet.CompressionCodec_UNCOMPRESSED, parquet.Type_INT32)
	require.ErrorContains(t, err, "not supported for encrypted columns")

	require.False(t, (*ParquetReader)(nil).pageOffsetEncrypted(4))
	require.False(t, (&ParquetReader{}).pageOffsetEncrypted(4))
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
	WithColumnKey("leaf", key)(pr)

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
	WithKeyRetriever(func([]byte) ([]byte, error) { return nil, retrieverErr })(pr)
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
		WithKeyRetriever(func([]byte) ([]byte, error) { return key, nil })(pr)

		got, err := pr.resolveOptionalFooterKeyFromMetadata([]byte("meta"))
		require.NoError(t, err)
		require.Equal(t, key, got)
		require.Equal(t, key, pr.resolvedFooterKey)
	})

	t.Run("footer key retriever returns empty", func(t *testing.T) {
		t.Parallel()
		pr := &ParquetReader{}
		WithKeyRetriever(func([]byte) ([]byte, error) { return nil, nil })(pr)

		got, err := pr.resolveOptionalFooterKeyFromMetadata(nil)
		require.NoError(t, err)
		require.Nil(t, got)
	})

	t.Run("footer key retriever returns error", func(t *testing.T) {
		t.Parallel()
		retrieverErr := errors.New("kms unavailable")
		pr := &ParquetReader{}
		WithKeyRetriever(func([]byte) ([]byte, error) { return nil, retrieverErr })(pr)

		_, err := pr.resolveOptionalFooterKeyFromMetadata(nil)
		require.ErrorIs(t, err, retrieverErr)
		require.ErrorContains(t, err, "retrieve footer key")
	})

	t.Run("optional column key nil crypto", func(t *testing.T) {
		t.Parallel()
		_, _, err := (&ParquetReader{}).resolveOptionalColumnKey(&parquet.ColumnChunk{})
		require.ErrorContains(t, err, "column crypto metadata is required")
	})

	t.Run("optional column key unsupported", func(t *testing.T) {
		t.Parallel()
		chunk := &parquet.ColumnChunk{CryptoMetadata: &parquet.ColumnCryptoMetaData{}}
		_, _, err := (&ParquetReader{}).resolveOptionalColumnKey(chunk)
		require.ErrorContains(t, err, "unsupported column crypto metadata")
	})

	t.Run("optional column key footer-key path with retriever error", func(t *testing.T) {
		t.Parallel()
		retrieverErr := errors.New("kms unavailable")
		pr := &ParquetReader{}
		WithKeyRetriever(func([]byte) ([]byte, error) { return nil, retrieverErr })(pr)
		chunk := &parquet.ColumnChunk{
			CryptoMetadata: &parquet.ColumnCryptoMetaData{ENCRYPTION_WITH_FOOTER_KEY: parquet.NewEncryptionWithFooterKey()},
		}
		_, _, err := pr.resolveOptionalColumnKey(chunk)
		require.ErrorIs(t, err, retrieverErr)
	})

	t.Run("optional column key from retriever", func(t *testing.T) {
		t.Parallel()
		key := []byte("0123456789abcdef")
		pr := &ParquetReader{}
		WithKeyRetriever(func([]byte) ([]byte, error) { return key, nil })(pr)
		chunk := &parquet.ColumnChunk{
			CryptoMetadata: &parquet.ColumnCryptoMetaData{
				ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{
					PathInSchema: []string{"leaf"},
					KeyMetadata:  []byte("meta"),
				},
			},
		}
		got, found, err := pr.resolveOptionalColumnKey(chunk)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, key, got)
	})

	t.Run("optional column key retriever returns empty", func(t *testing.T) {
		t.Parallel()
		pr := &ParquetReader{}
		WithKeyRetriever(func([]byte) ([]byte, error) { return nil, nil })(pr)
		chunk := &parquet.ColumnChunk{
			CryptoMetadata: &parquet.ColumnCryptoMetaData{
				ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{PathInSchema: []string{"leaf"}},
			},
		}
		got, found, err := pr.resolveOptionalColumnKey(chunk)
		require.NoError(t, err)
		require.False(t, found)
		require.Nil(t, got)
	})

	t.Run("optional column key retriever returns error", func(t *testing.T) {
		t.Parallel()
		retrieverErr := errors.New("kms unavailable")
		pr := &ParquetReader{}
		WithKeyRetriever(func([]byte) ([]byte, error) { return nil, retrieverErr })(pr)
		chunk := &parquet.ColumnChunk{
			CryptoMetadata: &parquet.ColumnCryptoMetaData{
				ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{PathInSchema: []string{"leaf"}},
			},
		}
		_, _, err := pr.resolveOptionalColumnKey(chunk)
		require.ErrorIs(t, err, retrieverErr)
		require.ErrorContains(t, err, "retrieve column key for leaf")
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

func buildEncryptedFooterFile(t *testing.T, key, aadPrefix, fileUnique []byte, footer *parquet.FileMetaData) []byte {
	t.Helper()
	return buildEncryptedFooterFileWithAlgorithm(t, key, aadPrefix, fileUnique, footer, &parquet.EncryptionAlgorithm{
		AES_GCM_V1: &parquet.AesGcmV1{
			AadPrefix:     aadPrefix,
			AadFileUnique: fileUnique,
		},
	})
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
