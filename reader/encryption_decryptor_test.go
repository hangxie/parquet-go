package reader

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
)

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

func TestAADParts(t *testing.T) {
	t.Parallel()

	stored := []byte("orders")
	fileUnique := []byte("file-unique")

	tests := []struct {
		name         string
		storedPrefix []byte
		supplied     []byte
		supplyPrefix bool
		wantPrefix   []byte
		wantErr      string
	}{
		{name: "stored prefix used when none supplied", storedPrefix: stored, wantPrefix: stored},
		{name: "stored prefix matches supplied", storedPrefix: stored, supplied: []byte("orders"), wantPrefix: stored},
		{name: "stored prefix differs from supplied", storedPrefix: stored, supplied: []byte("customers"), wantErr: "AAD prefix mismatch"},
		{name: "supplied prefix but file uses none", supplied: []byte("orders"), wantErr: "not used for file encryption"},
		{name: "no prefix anywhere"},
		{name: "required prefix supplied", supplied: stored, supplyPrefix: true, wantPrefix: stored},
		{name: "required prefix missing", supplyPrefix: true, wantErr: "AAD prefix is required"},
		{name: "empty supplied prefix uses stored prefix", storedPrefix: stored, supplied: []byte{}, wantPrefix: stored},
		{name: "empty supplied prefix for file without one", supplied: []byte{}},
		{name: "empty supplied prefix when required", supplied: []byte{}, supplyPrefix: true, wantErr: "AAD prefix is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pr := &ParquetReader{}
			if tt.supplied != nil {
				applyReaderOptionsForTest(t, pr, WithAADPrefix(tt.supplied))
			}
			prefix, unique, err := pr.aadParts(tt.storedPrefix, fileUnique, tt.supplyPrefix)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantPrefix, prefix)
			require.Equal(t, fileUnique, unique)
		})
	}
}
