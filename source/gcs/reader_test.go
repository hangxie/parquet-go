package gcs

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"cloud.google.com/go/storage"
)

var (
	// this is public accessible (ie allow anonymouse access) data set
	gcsProjectID     = ""
	gcsBucket        = "cloud-samples-data"
	gcsObjectName    = "bigquery/us-states/us-states.parquet"
	gcsObjectVersion = int64(-1)
)

func TestGcsReader_Close(t *testing.T) {
	t.Run("nil-reader", func(t *testing.T) {
		reader := &gcsReader{
			gcsFile: gcsFile{
				gcsClient: nil,
			},
			gcsReader: nil,
		}

		err := reader.Close()
		require.NoError(t, err)
	})

	ctx := context.Background()

	t.Run("non-nil-reader", func(t *testing.T) {
		client, err := storage.NewClient(ctx, option.WithoutAuthentication())
		require.NoError(t, err)

		reader, err := NewGcsFileReaderWithClient(ctx, client, gcsProjectID, gcsBucket, gcsObjectName, gcsObjectVersion)
		require.NoError(t, err)

		_, err = reader.Open(gcsObjectName)
		require.NoError(t, err)

		err = reader.Close()
		require.NoError(t, err)
	})

	t.Run("mimic-internal-client", func(t *testing.T) {
		client, err := storage.NewClient(ctx, option.WithoutAuthentication())
		require.NoError(t, err)

		reader, err := NewGcsFileReaderWithClient(ctx, client, gcsProjectID, gcsBucket, gcsObjectName, gcsObjectVersion)
		reader.externalClient = false
		require.NoError(t, err)

		_, err = reader.Open(gcsObjectName)
		require.NoError(t, err)

		err = reader.Close()
		require.NoError(t, err)
	})
}

func TestGcsReader_Open(t *testing.T) {
	t.Run("internal-client", func(t *testing.T) {
		reader := &gcsReader{}
		_, err := reader.Open(gcsObjectName)
		require.NotNil(t, err)
	})

	t.Run("anonymous-access", func(t *testing.T) {
		ctx := context.Background()

		client, err := storage.NewClient(ctx, option.WithoutAuthentication())
		require.NoError(t, err)

		reader, err := NewGcsFileReaderWithClient(ctx, client, gcsProjectID, gcsBucket, gcsObjectName, gcsObjectVersion)
		require.NoError(t, err)

		pr, err := reader.Open(gcsObjectName)
		require.NoError(t, err)
		require.NotNil(t, pr)
	})
}

func TestGcsReader_Clone(t *testing.T) {
	t.Run("mock", func(t *testing.T) {
		ctx := context.Background()

		client, err := storage.NewClient(ctx, option.WithoutAuthentication())
		require.NoError(t, err)

		reader, err := NewGcsFileReaderWithClient(ctx, client, gcsProjectID, gcsBucket, gcsObjectName, gcsObjectVersion)
		require.NoError(t, err)

		pr, err := reader.Clone()
		require.NoError(t, err)
		require.Equal(t, reader, pr)
	})

	t.Run("real", func(t *testing.T) {
		if testing.Short() {
			t.Skip("Skipping integration test with real GCS file")
		}

		ctx := context.Background()

		client, err := storage.NewClient(ctx, option.WithoutAuthentication())
		require.NoError(t, err)

		bucket := "cloud-samples-data"
		objectName := "bigquery/us-states/us-states.parquet"

		reader1, err := NewGcsFileReaderWithClient(ctx, client, "", bucket, objectName, 0)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, reader1.Close())
		}()

		buf1 := make([]byte, 1024)
		n1, err := reader1.Read(buf1)
		require.NoError(t, err)
		require.Equal(t, 1024, n1)

		reader2, err := reader1.Clone()
		require.NoError(t, err)
		defer func() {
			require.NoError(t, reader2.Close())
		}()

		buf2 := make([]byte, 1024)
		n2, err := reader2.Read(buf2)
		require.NoError(t, err)
		require.Equal(t, 1024, n2)

		require.Equal(t, buf1, buf2)

		buf3 := make([]byte, 512)
		n3, err := reader1.Read(buf3)
		require.NoError(t, err)
		require.Equal(t, 512, n3)

		buf4 := make([]byte, 512)
		n4, err := reader2.Read(buf4)
		require.NoError(t, err)
		require.Equal(t, 512, n4)

		require.Equal(t, buf3, buf4)
	})
}

func TestGcsReader_Seek(t *testing.T) {
	ctx := context.Background()

	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	require.NoError(t, err)

	reader, err := NewGcsFileReaderWithClient(ctx, client, gcsProjectID, gcsBucket, gcsObjectName, gcsObjectVersion)
	require.NoError(t, err)

	offset, err := reader.Seek(10, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(10), offset)
}

func TestGcsReader_Read(t *testing.T) {
	ctx := context.Background()

	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	require.NoError(t, err)

	reader, err := NewGcsFileReaderWithClient(ctx, client, gcsProjectID, gcsBucket, gcsObjectName, gcsObjectVersion)
	require.NoError(t, err)

	buf := make([]byte, 4)
	bytesRead, err := reader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 4, bytesRead)
	require.Equal(t, "PAR1", string(buf))
}

func TestNewGcsFileReaderWithClient(t *testing.T) {
	ctx := context.Background()

	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	require.NoError(t, err)

	reader, err := NewGcsFileReaderWithClient(ctx, client, gcsProjectID, gcsBucket, gcsObjectName, gcsObjectVersion)
	require.NoError(t, err)
	require.NotNil(t, reader)
}
