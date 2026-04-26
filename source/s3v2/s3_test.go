package s3v2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/source"
)

// Compile-time checks for interface compliance.
var (
	_ source.ParquetFileReader = (*s3Reader)(nil)
	_ source.ParquetFileWriter = (*s3Writer)(nil)
)

func TestS3File_Fields(t *testing.T) {
	ctx := context.Background()
	f := s3File{
		ctx:        ctx,
		bucketName: "my-bucket",
		key:        "data/file.parquet",
	}
	require.Equal(t, "my-bucket", f.bucketName)
	require.Equal(t, "data/file.parquet", f.key)
	require.NoError(t, f.err)
}

func TestS3Writer_EmbeddsS3File(t *testing.T) {
	ctx := context.Background()
	w := &s3Writer{
		s3File: s3File{
			ctx:        ctx,
			bucketName: "test-bucket",
			key:        "test-key",
		},
	}
	require.Equal(t, "test-bucket", w.bucketName)
	require.Equal(t, "test-key", w.key)
}
