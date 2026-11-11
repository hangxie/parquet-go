package gcs

import (
	"context"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
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

// uploadedData returns the object data of a multipart upload, which follows the JSON metadata part.
func uploadedData(r *http.Request) (string, error) {
	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return "", err
	}
	parts := multipart.NewReader(r.Body, params["boundary"])
	if _, err := parts.NextPart(); err != nil {
		return "", err
	}
	media, err := parts.NextPart()
	if err != nil {
		return "", err
	}
	data, err := io.ReadAll(media)
	return string(data), err
}

// newRecordingGCSClient returns a client for a fake GCS server and a getter for the object data it received.
func newRecordingGCSClient(t *testing.T) (*storage.Client, func() []string) {
	t.Helper()

	var mu sync.Mutex
	var uploads []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/upload/storage/v1/b/test-bucket/o") {
			http.NotFound(w, r)
			return
		}
		data, err := uploadedData(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		uploads = append(uploads, data)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"storage#object","bucket":"test-bucket","name":"test.parquet"}`))
	}))
	t.Cleanup(server.Close)

	client, err := storage.NewClient(context.Background(), option.WithEndpoint(server.URL), option.WithoutAuthentication())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), uploads...)
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

	t.Run("canceled_context", func(t *testing.T) {
		client, uploads := newRecordingGCSClient(t)
		canceled, cancel := context.WithCancel(ctx)
		cancel()

		writer, err := NewGcsFileWriterWithClient(canceled, client, "test-project", "test-bucket", "test.parquet")
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, writer)
		require.Empty(t, uploads())
	})

	t.Run("upload_error_on_close", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "forbidden", http.StatusForbidden)
		}))
		defer server.Close()
		client, err := storage.NewClient(ctx, option.WithEndpoint(server.URL), option.WithoutAuthentication())
		require.NoError(t, err)
		defer func() { _ = client.Close() }()

		writer, err := NewGcsFileWriterWithClient(ctx, client, "test-project", "test-bucket", "test.parquet")
		require.NoError(t, err)

		err = writer.Close()
		require.ErrorContains(t, err, "close GCS writer: ")
		var apiErr *googleapi.Error
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, http.StatusForbidden, apiErr.Code)
	})

	t.Run("write_then_close", func(t *testing.T) {
		client, uploads := newRecordingGCSClient(t)

		writer, err := NewGcsFileWriterWithClient(ctx, client, "test-project", "test-bucket", "test.parquet")
		require.NoError(t, err)
		require.True(t, writer.externalClient)
		require.Equal(t, "test-project", writer.projectID)
		require.Equal(t, "test-bucket", writer.bucketName)
		require.Equal(t, "test.parquet", writer.filePath)

		n, err := writer.Write([]byte("PAR1-payload"))
		require.NoError(t, err)
		require.Equal(t, 12, n)
		require.Empty(t, uploads(), "object must not be created before Close")

		require.NoError(t, writer.Close())
		require.Len(t, uploads(), 1)
		require.Equal(t, "PAR1-payload", uploads()[0])
	})

	t.Run("create_then_write", func(t *testing.T) {
		client, uploads := newRecordingGCSClient(t)

		writer, err := NewGcsFileWriterWithClient(ctx, client, "test-project", "test-bucket", "test.parquet")
		require.NoError(t, err)
		created, err := writer.Create("other.parquet")
		require.NoError(t, err)

		_, err = created.Write([]byte("PAR1-created"))
		require.NoError(t, err)
		require.NoError(t, created.Close())
		require.Len(t, uploads(), 1)
		require.Equal(t, "PAR1-created", uploads()[0])

		// Closing the unwritten receiver commits an empty object at its own path.
		require.NoError(t, writer.Close())
		require.Len(t, uploads(), 2)
		require.Empty(t, uploads()[1])
	})

	t.Run("owned_client_commits_before_client_close", func(t *testing.T) {
		client, uploads := newRecordingGCSClient(t)

		writer, err := NewGcsFileWriterWithClient(ctx, client, "test-project", "test-bucket", "test.parquet")
		require.NoError(t, err)
		writer.externalClient = false

		_, err = writer.Write([]byte("PAR1-owned"))
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		require.Nil(t, writer.gcsClient)
		require.Len(t, uploads(), 1)
		require.Equal(t, "PAR1-owned", uploads()[0])
	})

	t.Run("cancel_before_close_leaves_target_untouched", func(t *testing.T) {
		client, uploads := newRecordingGCSClient(t)
		cancelable, cancel := context.WithCancel(ctx)

		writer, err := NewGcsFileWriterWithClient(cancelable, client, "test-project", "test-bucket", "test.parquet")
		require.NoError(t, err)
		_, err = writer.Write([]byte("PAR1-aborted"))
		require.NoError(t, err)

		cancel()
		require.ErrorIs(t, writer.Close(), context.Canceled)
		require.Empty(t, uploads())
	})
}

func TestNewGcsFileWriter(t *testing.T) {
	var uploads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		uploads.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"storage#object","bucket":"test-bucket","name":"test.parquet"}`))
	}))
	defer server.Close()
	// storage.NewClient targets this host without credentials when it is set.
	t.Setenv("STORAGE_EMULATOR_HOST", strings.TrimPrefix(server.URL, "http://"))

	t.Run("owned_client_write_then_close", func(t *testing.T) {
		writer, err := NewGcsFileWriter(context.Background(), "test-project", "test-bucket", "test.parquet")
		require.NoError(t, err)
		require.False(t, writer.externalClient)

		_, err = writer.Write([]byte("PAR1"))
		require.NoError(t, err)
		require.NoError(t, writer.Close())
		require.Nil(t, writer.gcsClient)
		require.Equal(t, int32(1), uploads.Load())
	})

	t.Run("canceled_context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		writer, err := NewGcsFileWriter(ctx, "test-project", "test-bucket", "test.parquet")
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorContains(t, err, "new gcs writer with client")
		require.Nil(t, writer)
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
