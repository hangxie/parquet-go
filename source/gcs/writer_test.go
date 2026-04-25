package gcs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"cloud.google.com/go/storage"

	"github.com/hangxie/parquet-go/v3/source"
)

func TestGcsFileInterfaceComplianceWriter(t *testing.T) {
	var _ source.ParquetFileWriter = (*gcsFileWriter)(nil)
}

func newFakeGCSClient(t *testing.T) (*storage.Client, func()) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/upload/storage/v1/b/test-bucket/o") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"kind":"storage#object","bucket":"test-bucket","name":"test.parquet","size":"0"}`))
			return
		}

		http.NotFound(w, r)
	}))

	client, err := storage.NewClient(
		context.Background(),
		option.WithEndpoint(server.URL),
		option.WithoutAuthentication(),
	)
	require.NoError(t, err)

	return client, func() {
		require.NoError(t, client.Close())
		server.Close()
	}
}

func TestGcsWriterCreate(t *testing.T) {
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
	require.Contains(t, err.Error(), "open gcs writer")

	t.Run("with_client_success", func(t *testing.T) {
		client, cleanup := newFakeGCSClient(t)
		defer cleanup()

		writer := &gcsFileWriter{
			gcsFile: gcsFile{
				ctx:        context.Background(),
				gcsClient:  client,
				projectID:  "test-project",
				bucketName: "test-bucket",
			},
		}

		created, err := writer.Create("test.parquet")
		require.NoError(t, err)
		require.NotNil(t, created)
	})
}

func TestGcsWriterNilOperations(t *testing.T) {
	writer := &gcsFileWriter{
		gcsFile: gcsFile{
			gcsClient: nil,
		},
		gcsWriter: nil,
	}

	err := writer.Close()
	require.NoError(t, err)
}

func TestGcsWriterWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client, err := storage.NewClient(context.Background(), option.WithoutAuthentication())
	require.NoError(t, err)

	writer := &gcsFileWriter{
		gcsWriter: client.Bucket("test-bucket").Object("test.parquet").NewWriter(ctx),
	}

	n, err := writer.Write([]byte("test data"))
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 0, n)
}

func TestGcsWriterClose(t *testing.T) {
	t.Run("internal_client", func(t *testing.T) {
		client, err := storage.NewClient(context.Background(), option.WithoutAuthentication())
		require.NoError(t, err)

		writer := &gcsFileWriter{
			gcsFile: gcsFile{
				gcsClient:      client,
				externalClient: false,
			},
		}

		err = writer.Close()
		require.NoError(t, err)
		require.Nil(t, writer.gcsClient)
	})

	t.Run("writer_error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		client, err := storage.NewClient(context.Background(), option.WithoutAuthentication())
		require.NoError(t, err)

		writer := &gcsFileWriter{
			gcsFile: gcsFile{
				gcsClient:      client,
				externalClient: true,
			},
			gcsWriter: client.Bucket("test-bucket").Object("test.parquet").NewWriter(ctx),
		}

		err = writer.Close()
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("writer_success", func(t *testing.T) {
		client, cleanup := newFakeGCSClient(t)
		defer cleanup()

		writer := &gcsFileWriter{
			gcsFile: gcsFile{
				gcsClient:      client,
				externalClient: true,
			},
			gcsWriter: client.Bucket("test-bucket").Object("test.parquet").NewWriter(context.Background()),
		}

		err := writer.Close()
		require.NoError(t, err)
	})
}

func TestGcsWriterStructure(t *testing.T) {
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

func TestNewGcsFileWriterWithClient(t *testing.T) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	require.NoError(t, err)
	_, err = NewGcsFileWriterWithClient(ctx, client, "test-project", "test-bucket", "test.parquet")
	require.Error(t, err)
	require.Contains(t, err.Error(), "close writer: googleapi: Error 401")

	t.Run("success", func(t *testing.T) {
		client, cleanup := newFakeGCSClient(t)
		defer cleanup()

		writer, err := NewGcsFileWriterWithClient(ctx, client, "test-project", "test-bucket", "test.parquet")
		require.NoError(t, err)
		require.NotNil(t, writer)
		require.True(t, writer.externalClient)
		require.Equal(t, "test-project", writer.projectID)
		require.Equal(t, "test-bucket", writer.bucketName)
		require.Equal(t, "test.parquet", writer.filePath)
	})
}

func TestGcsWriterCreateWithClientError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client, err := storage.NewClient(context.Background(), option.WithoutAuthentication())
	require.NoError(t, err)

	writer := &gcsFileWriter{
		gcsFile: gcsFile{
			ctx:        ctx,
			gcsClient:  client,
			projectID:  "test-project",
			bucketName: "test-bucket",
		},
	}

	_, err = writer.Create("new-file.parquet")
	require.Error(t, err)
	require.Contains(t, err.Error(), "open gcs writer with client")
	require.ErrorIs(t, err, context.Canceled)
}

func TestNewGcsFileWriterCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	writer, err := NewGcsFileWriter(ctx, "test-project", "test-bucket", "test.parquet")
	require.Error(t, err)
	require.Nil(t, writer)
}
