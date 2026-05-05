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
		{name: "missing key", wantErr: "footer decryption key is required"},
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
	require.ErrorContains(t, err, "column decryption key is required for leaf")
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
	require.ErrorContains(t, err, "column decryption key is required")

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
	require.ErrorContains(t, err, "footer decryption key is required")
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
	file := append([]byte("PARE"), section...)
	var footerSize [4]byte
	binary.LittleEndian.PutUint32(footerSize[:], uint32(len(section)))
	file = append(file, footerSize[:]...)
	file = append(file, []byte("PARE")...)
	return file
}

func buildEncryptedFooterFileWithAlgorithm(t *testing.T, key, aadPrefix, fileUnique []byte, footer *parquet.FileMetaData, algorithm *parquet.EncryptionAlgorithm) []byte {
	t.Helper()

	footerBytes := serializeThrift(t, footer)
	cryptoMeta := &parquet.FileCryptoMetaData{EncryptionAlgorithm: algorithm}
	cryptoMetaBytes := serializeThrift(t, cryptoMeta)
	encryptedFooter := encryptGCMModule(t, key, encryption.AAD(aadPrefix, fileUnique, encryption.ModuleFooter, 0, 0, 0), footerBytes)

	section := append(append([]byte{}, cryptoMetaBytes...), encryptedFooter...)
	file := append([]byte("PARE"), section...)
	var footerSize [4]byte
	binary.LittleEndian.PutUint32(footerSize[:], uint32(len(section)))
	file = append(file, footerSize[:]...)
	file = append(file, []byte("PARE")...)
	return file
}

func buildPlaintextEncryptedFooterFile(t *testing.T, key, aadPrefix, fileUnique []byte, footer *parquet.FileMetaData) []byte {
	t.Helper()
	footerBytes := serializeThrift(t, footer)
	signature := signPlaintextFooter(t, key, encryption.AAD(aadPrefix, fileUnique, encryption.ModuleFooter, 0, 0, 0), footerBytes)
	section := append(append([]byte{}, footerBytes...), signature...)
	file := append([]byte("PAR1"), section...)
	var footerSize [4]byte
	binary.LittleEndian.PutUint32(footerSize[:], uint32(len(section)))
	file = append(file, footerSize[:]...)
	file = append(file, []byte("PAR1")...)
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
