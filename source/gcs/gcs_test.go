package gcs

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/source"
)

// Compile-time checks for interface compliance.
var (
	_ source.ParquetFileReader = (*gcsReader)(nil)
	_ source.ParquetFileWriter = (*gcsFileWriter)(nil)
)

func TestMain(m *testing.M) {
	// A missing credential file fails default client creation, so tests never pick up the developer's own Google credentials.
	if err := os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/nonexistent/parquet-go-test-credentials.json"); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestGcsFile_Fields(t *testing.T) {
	ctx := context.Background()
	f := &gcsFile{
		projectID:      "my-project",
		bucketName:     "my-bucket",
		filePath:       "data/file.parquet",
		externalClient: true,
		ctx:            ctx,
	}
	require.Equal(t, "my-project", f.projectID)
	require.Equal(t, "my-bucket", f.bucketName)
	require.Equal(t, "data/file.parquet", f.filePath)
	require.True(t, f.externalClient)
}

func TestGcsReader_Close_NilClientExternalClient(t *testing.T) {
	// When externalClient=true and gcsClient=nil, Close should be a no-op on the client.
	r := &gcsReader{
		gcsFile: gcsFile{
			externalClient: true,
			gcsClient:      nil,
		},
	}
	// gcsReader is nil, so Close should return nil.
	err := r.Close()
	require.NoError(t, err)
}

func TestGcsFileWriter_Close_NilClientExternalClient(t *testing.T) {
	// When externalClient=true, the client is not closed on Close().
	w := &gcsFileWriter{
		gcsFile: gcsFile{
			externalClient: true,
			gcsClient:      nil,
		},
		gcsWriter: nil,
	}
	err := w.Close()
	require.NoError(t, err)
}

func TestNewGcsFileReader_CanceledContextClientError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // immediately cancel
	_, err := NewGcsFileReader(ctx, "proj", "bucket", "file.parquet", 0)
	require.Error(t, err)
}

func TestNewGcsFileWriter_CanceledContextClientError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewGcsFileWriter(ctx, "proj", "bucket", "file.parquet")
	require.Error(t, err)
}
