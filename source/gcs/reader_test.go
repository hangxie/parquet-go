package gcs

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"cloud.google.com/go/storage"

	"github.com/hangxie/parquet-go/v2/source"
)

func Test_GcsFileInterfaceComplianceReader(t *testing.T) {
	var _ source.ParquetFileReader = (*gcsReader)(nil)
}

func Test_GcsReaderNilOperations(t *testing.T) {
	reader := &gcsReader{
		gcsFile: gcsFile{
			gcsClient: nil,
		},
		gcsReader: nil,
	}

	err := reader.Close()
	require.NoError(t, err)
}

func Test_GcsReaderOpen(t *testing.T) {
	reader := &gcsReader{
		gcsFile: gcsFile{
			gcsClient:  nil,
			projectID:  "test-project",
			bucketName: "test-bucket",
			filePath:   "test.parquet",
		},
	}

	_, err := reader.Open("new-file.parquet")
	require.Error(t, err)
}

func Test_GcsReaderStructure(t *testing.T) {
	ctx := context.Background()

	reader := &gcsReader{
		gcsFile: gcsFile{
			ctx:        ctx,
			gcsClient:  &storage.Client{},
			projectID:  "test-project",
			bucketName: "test-bucket",
			filePath:   "test.parquet",
		},
		generation: 123,
	}

	require.Equal(t, "test-project", reader.projectID)
	require.Equal(t, "test-bucket", reader.bucketName)
	require.Equal(t, "test.parquet", reader.filePath)
	require.Equal(t, int64(123), reader.generation)
}
