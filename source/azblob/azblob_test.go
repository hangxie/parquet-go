package azblob

import (
	"context"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/source"
)

// Compile-time checks for interface compliance.
var (
	_ source.ParquetFileReader = (*azBlobReader)(nil)
	_ source.ParquetFileWriter = (*azBlobWriter)(nil)
)

func TestAzBlockBlob_Fields(t *testing.T) {
	// Verify azBlockBlob is correctly embedded in both reader and writer.
	ctx := context.Background()
	r := &azBlobReader{
		azBlockBlob: azBlockBlob{ctx: ctx},
		fileSize:    100,
	}
	require.Equal(t, ctx, r.ctx)
	require.Equal(t, int64(100), r.fileSize)

	w := &azBlobWriter{
		azBlockBlob: azBlockBlob{ctx: ctx},
	}
	require.Equal(t, ctx, w.ctx)
}

func TestNewAzBlobFileReader_InvalidCredentialType(t *testing.T) {
	_, err := NewAzBlobFileReader(
		context.Background(),
		"https://account.blob.core.windows.net/container/file.parquet",
		"invalid-credential-type",
		blockblob.ClientOptions{},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid credential type")
}

func TestNewAzBlobFileWriter_InvalidCredentialType(t *testing.T) {
	_, err := NewAzBlobFileWriter(
		context.Background(),
		"https://account.blob.core.windows.net/container/file.parquet",
		"invalid-credential-type",
		blockblob.ClientOptions{},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid credential type")
}

func TestAzBlobReader_Read_NilClient(t *testing.T) {
	r := &azBlobReader{
		azBlockBlob: azBlockBlob{ctx: context.Background()},
	}
	buf := make([]byte, 10)
	_, err := r.Read(buf)
	require.ErrorIs(t, err, errReadNotOpened)
}
