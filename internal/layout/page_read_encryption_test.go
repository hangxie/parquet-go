package layout

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

func TestReadPageRawDataEncrypted(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	aadPrefix := []byte("prefix")
	fileUnique := []byte("file-unique")
	body := []byte{0x2A, 0x00, 0x00, 0x00}
	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE,
		CompressedPageSize:   int32(len(body)),
		UncompressedPageSize: int32(len(body)),
		DataPageHeader: &parquet.DataPageHeader{
			NumValues:               1,
			Encoding:                parquet.Encoding_PLAIN,
			DefinitionLevelEncoding: parquet.Encoding_RLE,
			RepetitionLevelEncoding: parquet.Encoding_RLE,
		},
	}

	tests := []struct {
		name      string
		algorithm PageEncryptionAlgorithm
		body      []byte
	}{
		{
			name:      "AES GCM",
			algorithm: PageEncryptionAESGCM,
			body:      encryptPageGCMModule(t, key, pageTestAAD(aadPrefix, fileUnique, encryption.ModuleDataPage), body),
		},
		{
			name:      "AES GCM CTR",
			algorithm: PageEncryptionAESGCMCTR,
			body:      encryptPageCTRModule(t, key, body),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			headerModule := encryptPageGCMModule(t, key, pageTestAAD(aadPrefix, fileUnique, encryption.ModuleDataPageHeader), serializePageHeader(t, header))
			thriftReader := encryptedPageReader(headerModule, tt.body)
			page, err := ReadPageRawData(thriftReader, testSchemaHandler(), testColumnMetaData(), &PageReadOptions{
				MaxPageSize: 100,
				Decryptor: &PageDecryptor{
					Algorithm:     tt.algorithm,
					Key:           key,
					AADPrefix:     aadPrefix,
					AADFileUnique: fileUnique,
				},
			})

			require.NoError(t, err)
			require.Equal(t, parquet.PageType_DATA_PAGE, page.Header.GetType())
			require.Equal(t, body, page.RawData)
		})
	}
}

func TestReadPageRawDataEncryptedDictionaryHeader(t *testing.T) {
	t.Parallel()

	key := []byte("0123456789abcdef")
	aadPrefix := []byte("prefix")
	fileUnique := []byte("file-unique")
	body := []byte{0x2A, 0x00, 0x00, 0x00}
	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DICTIONARY_PAGE,
		CompressedPageSize:   int32(len(body)),
		UncompressedPageSize: int32(len(body)),
		DictionaryPageHeader: &parquet.DictionaryPageHeader{
			NumValues: 1,
			Encoding:  parquet.Encoding_PLAIN,
		},
	}

	headerModule := encryptPageGCMModule(t, key, pageTestAAD(aadPrefix, fileUnique, encryption.ModuleDictionaryPageHeader), serializePageHeader(t, header))
	bodyModule := encryptPageGCMModule(t, key, pageTestAAD(aadPrefix, fileUnique, encryption.ModuleDictionaryPage), body)
	page, err := ReadPageRawData(encryptedPageReader(headerModule, bodyModule), testSchemaHandler(), testColumnMetaData(), &PageReadOptions{
		MaxPageSize: 100,
		Decryptor: &PageDecryptor{
			Algorithm:     PageEncryptionAESGCM,
			Key:           key,
			AADPrefix:     aadPrefix,
			AADFileUnique: fileUnique,
		},
	})

	require.NoError(t, err)
	require.Equal(t, parquet.PageType_DICTIONARY_PAGE, page.Header.GetType())
	require.Equal(t, body, page.RawData)
}

func testSchemaHandler() *schema.SchemaHandler {
	return schema.NewSchemaHandlerFromSchemaList([]*parquet.SchemaElement{
		{
			Name:           "parquet_go_root",
			NumChildren:    common.ToPtr(int32(1)),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "test_col",
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
	})
}

func testColumnMetaData() *parquet.ColumnMetaData {
	return &parquet.ColumnMetaData{
		Type:         parquet.Type_INT32,
		Codec:        parquet.CompressionCodec_UNCOMPRESSED,
		PathInSchema: []string{"test_col"},
	}
}

func pageTestAAD(prefix, fileUnique []byte, moduleType encryption.ModuleType) []byte {
	return encryption.AAD(prefix, fileUnique, moduleType, 0, 0, 0)
}

func encryptedPageReader(modules ...[]byte) *thrift.TBufferedTransport {
	mem := thrift.NewTMemoryBuffer()
	for _, module := range modules {
		_, _ = mem.Write(module)
	}
	return thrift.NewTBufferedTransport(mem, 1024)
}

func serializePageHeader(t *testing.T, header *parquet.PageHeader) []byte {
	t.Helper()
	transport := thrift.NewTMemoryBufferLen(1024)
	protocol := thrift.NewTCompactProtocolConf(transport, &thrift.TConfiguration{})
	require.NoError(t, header.Write(context.Background(), protocol))
	require.NoError(t, protocol.Flush(context.Background()))
	return transport.Bytes()
}

func encryptPageGCMModule(t *testing.T, key, aad, plaintext []byte) []byte {
	t.Helper()
	nonce := []byte("123456789012")
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	gcm, err := cipher.NewGCMWithNonceSize(block, len(nonce))
	require.NoError(t, err)
	body := append(append([]byte{}, nonce...), gcm.Seal(nil, nonce, plaintext, aad)...)
	return pageModule(body)
}

func encryptPageCTRModule(t *testing.T, key, plaintext []byte) []byte {
	t.Helper()
	nonce := []byte("123456789012")
	block, err := aes.NewCipher(key)
	require.NoError(t, err)
	iv := make([]byte, aes.BlockSize)
	copy(iv, nonce)
	iv[aes.BlockSize-1] = 1
	ciphertext := make([]byte, len(plaintext))
	cipher.NewCTR(block, iv).XORKeyStream(ciphertext, plaintext)
	return pageModule(append(append([]byte{}, nonce...), ciphertext...))
}

func pageModule(body []byte) []byte {
	var buf bytes.Buffer
	var length [4]byte
	binary.LittleEndian.PutUint32(length[:], uint32(len(body)))
	buf.Write(length[:])
	buf.Write(body)
	return buf.Bytes()
}
