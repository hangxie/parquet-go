package gcs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"cloud.google.com/go/storage"

	"github.com/hangxie/parquet-go/v2/source"
)

func Test_GcsFileInterfaceComplianceWriter(t *testing.T) {
	var _ source.ParquetFileWriter = (*gcsFileWriter)(nil)
}

func Test_GcsWriterCreate(t *testing.T) {
	writer := &gcsFileWriter{
		gcsFile: gcsFile{
			gcsClient:  nil,
			projectID:  "test-project",
			bucketName: "test-bucket",
			filePath:   "test.parquet",
		},
	}

	_, err := writer.Create("new-file.parquet")
	require.Error(t, err)
}

func Test_GcsWriterNilOperations(t *testing.T) {
	writer := &gcsFileWriter{
		gcsFile: gcsFile{
			gcsClient: nil,
		},
		gcsWriter: nil,
	}

	err := writer.Close()
	require.NoError(t, err)
}

func Test_GcsWriterStructure(t *testing.T) {
	ctx := context.Background()

	writer := &gcsFileWriter{
		gcsFile: gcsFile{
			ctx:        ctx,
			gcsClient:  &storage.Client{},
			projectID:  "test-project",
			bucketName: "test-bucket",
			filePath:   "test.parquet",
		},
	}

	require.Equal(t, "test-project", writer.projectID)
	require.Equal(t, "test-bucket", writer.bucketName)
	require.Equal(t, "test.parquet", writer.filePath)
}

func Test_NewGcsFileWriterWithClient(t *testing.T) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	require.NoError(t, err)
	_, err = NewGcsFileWriterWithClient(ctx, client, "test-project", "test-bucket", "test.parquet")
	require.Error(t, err)
	require.Contains(t, err.Error(), "close writer: googleapi: Error 401")
}
