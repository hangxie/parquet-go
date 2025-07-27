package gcs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"cloud.google.com/go/storage"

	"github.com/hangxie/parquet-go/v2/source"
)

func Test_GcsFileExternalClientFlag(t *testing.T) {
	ctx := context.Background()

	file := gcsFile{
		ctx:            ctx,
		gcsClient:      &storage.Client{},
		projectID:      "test-project",
		bucketName:     "test-bucket",
		filePath:       "test.parquet",
		externalClient: true,
	}

	require.True(t, file.externalClient)

	file.externalClient = false
	require.False(t, file.externalClient)
}

func Test_GcsFileInterfaceComplianceWriter(t *testing.T) {
	var _ source.ParquetFileWriter = (*gcsFileWriter)(nil)
}

func Test_GcsFileStructure(t *testing.T) {
	ctx := context.Background()

	file := gcsFile{
		projectID:      "test-project",
		bucketName:     "test-bucket",
		filePath:       "test.parquet",
		gcsClient:      &storage.Client{},
		externalClient: true,
		ctx:            ctx,
	}

	require.Equal(t, "test-project", file.projectID)
	require.Equal(t, "test-bucket", file.bucketName)
	require.Equal(t, "test.parquet", file.filePath)
	require.True(t, file.externalClient)
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

	require.Panics(t, func() {
		_ = writer.Close()
	})
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
