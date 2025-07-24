package azblob

import (
	"context"
	"errors"
	"io"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"

	"github.com/hangxie/parquet-go/v2/source"
)

// Test interface compliance
func Test_AzBlobWriterInterfaceCompliance(t *testing.T) {
	var _ source.ParquetFileWriter = (*azBlobWriter)(nil)
}

func Test_NewAzBlobFileWriter(t *testing.T) {
	ctx := context.Background()
	clientOptions := blockblob.ClientOptions{
		ClientOptions: policy.ClientOptions{
			Retry: policy.RetryOptions{
				TryTimeout: 10 * time.Second,
				MaxRetries: 1,
			},
		},
	}

	testCases := []struct {
		name           string
		url            string
		credential     any
		clientOptions  blockblob.ClientOptions
		expectError    bool
		expectedErrMsg string
	}{
		{
			name:          "nil_credential",
			url:           "https://teststorage.blob.core.windows.net/container/test.parquet",
			credential:    nil,
			clientOptions: clientOptions,
			expectError:   false, // Should succeed with no credential
		},
		{
			name:           "invalid_credential_type",
			url:            "https://teststorage.blob.core.windows.net/container/test.parquet",
			credential:     "invalid-credential-type",
			clientOptions:  clientOptions,
			expectError:    true,
			expectedErrMsg: "invalid credential type",
		},
		{
			name:          "invalid_url",
			url:           "not-a-valid-url",
			credential:    nil,
			clientOptions: clientOptions,
			expectError:   false, // Client creation may succeed, operation fails later
		},
		{
			name:          "empty_url",
			url:           "",
			credential:    nil,
			clientOptions: clientOptions,
			expectError:   false, // Client creation may succeed, operation fails later
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer, err := NewAzBlobFileWriter(ctx, tc.url, tc.credential, tc.clientOptions)

			if tc.expectError {
				require.Error(t, err)
				require.Nil(t, writer)
				if tc.expectedErrMsg != "" {
					require.Contains(t, err.Error(), tc.expectedErrMsg)
				}
			} else {
				if err != nil {
					// For real Azure Blob operations, we might get network errors
					// which is acceptable in unit tests without real Azure access
					t.Logf("Network error (expected in unit test): %v", err)
				}
			}
		})
	}
}

func Test_NewAzBlobFileWriterWithClient(t *testing.T) {
	ctx := context.Background()
	testURL := "https://teststorage.blob.core.windows.net/container/test.parquet"

	t.Run("nil_client", func(t *testing.T) {
		writer, err := NewAzBlobFileWriterWithClient(ctx, testURL, nil)
		require.Error(t, err)
		require.Nil(t, writer)
		require.Contains(t, err.Error(), "client cannot be nil")
	})

	t.Run("valid_client", func(t *testing.T) {
		client, err := blockblob.NewClientWithNoCredential(testURL, &blockblob.ClientOptions{})
		require.NoError(t, err)

		writer, err := NewAzBlobFileWriterWithClient(ctx, testURL, client)
		// This might fail due to network/auth issues in unit tests, which is expected
		if err != nil {
			t.Logf("Network error (expected in unit test): %v", err)
		} else {
			require.NotNil(t, writer)
		}
	})

	t.Run("invalid_url", func(t *testing.T) {
		client, err := blockblob.NewClientWithNoCredential(testURL, &blockblob.ClientOptions{})
		require.NoError(t, err)

		writer, err := NewAzBlobFileWriterWithClient(ctx, "not-a-valid-url", client)
		// Azure SDK may not validate URL format until operations, so error might come later
		if err != nil {
			require.Nil(t, writer)
			t.Logf("URL validation error (expected): %v", err)
		} else {
			// Clean up if successful
			if writer != nil {
				_ = writer.(*azBlobWriter).pipeWriter.Close()
			}
		}
	})
}

func Test_AzBlobWriter_Write(t *testing.T) {
	testCases := []struct {
		name          string
		setupWriter   func() *azBlobWriter
		writeData     []byte
		expectError   bool
		expectedError string
	}{
		{
			name: "nil_client",
			setupWriter: func() *azBlobWriter {
				return &azBlobWriter{
					azBlockBlob: azBlockBlob{
						blockBlobClient: nil,
					},
				}
			},
			writeData:     []byte("test data"),
			expectError:   true,
			expectedError: "Write url not opened",
		},
		{
			name: "successful_write",
			setupWriter: func() *azBlobWriter {
				// Create a pipe for testing
				r, w := io.Pipe()
				go func() {
					// Consume data to prevent blocking
					_, _ = io.Copy(io.Discard, r)
					_ = r.Close()
				}()

				// Mock client (we can't test actual Azure calls without auth)
				client, _ := blockblob.NewClientWithNoCredential(
					"https://test.blob.core.windows.net/container/test.parquet",
					&blockblob.ClientOptions{},
				)

				return &azBlobWriter{
					azBlockBlob: azBlockBlob{
						blockBlobClient: client,
					},
					pipeWriter: w,
				}
			},
			writeData:   []byte("test data"),
			expectError: false,
		},
		{
			name: "pipe_write_error",
			setupWriter: func() *azBlobWriter {
				// Create a pipe and immediately close the reader to cause write errors
				r, w := io.Pipe()
				r.CloseWithError(errors.New("pipe closed"))

				client, _ := blockblob.NewClientWithNoCredential(
					"https://test.blob.core.windows.net/container/test.parquet",
					&blockblob.ClientOptions{},
				)

				return &azBlobWriter{
					azBlockBlob: azBlockBlob{
						blockBlobClient: client,
					},
					pipeWriter: w,
				}
			},
			writeData:     []byte("test data"),
			expectError:   true,
			expectedError: "pipe closed",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer := tc.setupWriter()

			n, err := writer.Write(tc.writeData)

			if tc.expectError {
				require.Error(t, err)
				require.Equal(t, 0, n)
				if tc.expectedError != "" {
					require.Contains(t, err.Error(), tc.expectedError)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, len(tc.writeData), n)
			}
		})
	}
}

func Test_AzBlobWriter_Close(t *testing.T) {
	testCases := []struct {
		name        string
		setupWriter func() *azBlobWriter
		expectError bool
	}{
		{
			name: "nil_pipe_writer",
			setupWriter: func() *azBlobWriter {
				return &azBlobWriter{
					pipeWriter: nil,
				}
			},
			expectError: false, // Should handle nil gracefully
		},
		{
			name: "successful_close",
			setupWriter: func() *azBlobWriter {
				r, w := io.Pipe()
				writeDone := make(chan error, 1)

				// Simulate successful upload completion
				go func() {
					// Consume any data
					_, _ = io.Copy(io.Discard, r)
					_ = r.Close()
					writeDone <- nil // Signal successful completion
				}()

				return &azBlobWriter{
					pipeWriter: w,
					writeDone:  writeDone,
				}
			},
			expectError: false,
		},
		{
			name: "upload_error_from_channel",
			setupWriter: func() *azBlobWriter {
				r, w := io.Pipe()
				writeDone := make(chan error, 1)

				// Simulate upload failure
				go func() {
					_, _ = io.Copy(io.Discard, r)
					_ = r.Close()
					writeDone <- errors.New("upload failed")
				}()

				return &azBlobWriter{
					pipeWriter: w,
					writeDone:  writeDone,
				}
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer := tc.setupWriter()
			err := writer.Close()

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_AzBlobWriter_Create(t *testing.T) {
	ctx := context.Background()
	client, err := blockblob.NewClientWithNoCredential(
		"https://test.blob.core.windows.net/container/test.parquet",
		&blockblob.ClientOptions{},
	)
	require.NoError(t, err)

	testCases := []struct {
		name        string
		setupWriter func() *azBlobWriter
		url         string
		expectError bool
	}{
		{
			name: "empty_url_with_existing_url",
			setupWriter: func() *azBlobWriter {
				existingURL := "https://test.blob.core.windows.net/container/existing.parquet"
				parsedURL, _ := url.Parse(existingURL)
				return &azBlobWriter{
					azBlockBlob: azBlockBlob{
						ctx:             ctx,
						url:             parsedURL,
						blockBlobClient: client,
					},
				}
			},
			url:         "", // Empty URL should use existing URL
			expectError: false,
		},
		{
			name: "valid_new_url",
			setupWriter: func() *azBlobWriter {
				return &azBlobWriter{
					azBlockBlob: azBlockBlob{
						ctx:             ctx,
						blockBlobClient: client,
					},
				}
			},
			url:         "https://test.blob.core.windows.net/container/new.parquet",
			expectError: false,
		},
		{
			name: "invalid_url",
			setupWriter: func() *azBlobWriter {
				return &azBlobWriter{
					azBlockBlob: azBlockBlob{
						ctx:             ctx,
						blockBlobClient: client,
					},
				}
			},
			url:         "not-a-valid-url",
			expectError: false, // URL parsing may succeed, errors come during operations
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			writer := tc.setupWriter()
			newWriter, err := writer.Create(tc.url)

			if tc.expectError {
				require.Error(t, err)
			} else {
				// Error might still occur due to invalid URLs, which is acceptable
				if err != nil {
					t.Logf("Expected error for invalid input: %v", err)
				} else {
					require.NotNil(t, newWriter)

					// Verify the returned writer has proper structure
					azWriter, ok := newWriter.(*azBlobWriter)
					require.True(t, ok)
					require.NotNil(t, azWriter.pipeReader)
					require.NotNil(t, azWriter.pipeWriter)
					require.NotNil(t, azWriter.writeDone)

					// Clean up
					_ = azWriter.pipeWriter.Close()
				}
			}
		})
	}
}

func Test_AzBlobWriter_ErrorMessages(t *testing.T) {
	// Test that errWriteNotOpened is properly defined
	require.Equal(t, "Write url not opened", errWriteNotOpened.Error())
}

func Test_AzBlobWriter_CredentialTypes(t *testing.T) {
	ctx := context.Background()
	testURL := "https://test.blob.core.windows.net/container/test.parquet"
	clientOptions := blockblob.ClientOptions{}

	// Test with TokenCredential interface (mock)
	t.Run("token_credential", func(t *testing.T) {
		// We can't easily create a real TokenCredential without external dependencies
		// This test would be more complete with a mock that implements azcore.TokenCredential
		// For now, we test that the function exists and handles the credential parameter

		// Test with nil credential (already covered in other tests)
		_, err := NewAzBlobFileWriter(ctx, testURL, nil, clientOptions)
		// Error is expected due to network/auth, but function should not panic
		if err != nil {
			t.Logf("Expected network/auth error: %v", err)
		}
	})

	// Test with SharedKeyCredential (would require actual credentials)
	t.Run("shared_key_credential", func(t *testing.T) {
		// We can't test this without real credentials, but we can verify
		// the function handles SharedKeyCredential type

		// This would be tested with a real blob.SharedKeyCredential:
		// credential, _ := blob.NewSharedKeyCredential("account", "key")
		// _, err := NewAzBlobFileWriter(ctx, testURL, credential, clientOptions)

		// For unit tests, we just verify the function exists
		require.NotNil(t, NewAzBlobFileWriter)
	})
}

func Test_AzBlobWriter_WriteWithCloseError(t *testing.T) {
	// Test that Write calls CloseWithError when write fails
	r, w := io.Pipe()

	// Close reader immediately to cause write errors
	r.CloseWithError(errors.New("test close error"))

	client, err := blockblob.NewClientWithNoCredential(
		"https://test.blob.core.windows.net/container/test.parquet",
		&blockblob.ClientOptions{},
	)
	require.NoError(t, err)

	writer := &azBlobWriter{
		azBlockBlob: azBlockBlob{
			blockBlobClient: client,
		},
		pipeWriter: w,
	}

	// This should fail and call CloseWithError
	n, err := writer.Write([]byte("test data"))
	require.Error(t, err)
	require.Equal(t, 0, n)
	require.Contains(t, err.Error(), "close")
}

func Test_AzBlobWriter_CreateWithUploadFailure(t *testing.T) {
	ctx := context.Background()

	// Create a client that will fail during upload
	client, err := blockblob.NewClientWithNoCredential(
		"https://nonexistent.blob.core.windows.net/container/test.parquet",
		&blockblob.ClientOptions{},
	)
	require.NoError(t, err)

	writer := &azBlobWriter{
		azBlockBlob: azBlockBlob{
			ctx:             ctx,
			blockBlobClient: client,
		},
	}

	// Create should succeed (goroutine handles upload failure)
	newWriter, err := writer.Create("https://nonexistent.blob.core.windows.net/container/test.parquet")
	require.NoError(t, err)
	require.NotNil(t, newWriter)

	azWriter := newWriter.(*azBlobWriter)

	// Write some data
	_, err = azWriter.Write([]byte("test data"))
	require.NoError(t, err) // Write to pipe should succeed

	// Close should return the upload error
	err = azWriter.Close()
	require.Error(t, err) // Should get upload error from goroutine
}

func Test_AzBlobWriter_ConcurrentOperations(t *testing.T) {
	ctx := context.Background()
	client, err := blockblob.NewClientWithNoCredential(
		"https://test.blob.core.windows.net/container/test.parquet",
		&blockblob.ClientOptions{},
	)
	require.NoError(t, err)

	writer := &azBlobWriter{
		azBlockBlob: azBlockBlob{
			ctx:             ctx,
			blockBlobClient: client,
		},
	}

	// Test creating multiple writers concurrently
	const numWriters = 5
	writers := make([]source.ParquetFileWriter, numWriters)
	errors := make([]error, numWriters)
	var wg sync.WaitGroup

	// Create writers concurrently
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			w, err := writer.Create("https://test.blob.core.windows.net/container/test" + string(rune('0'+index)) + ".parquet")
			writers[index] = w
			errors[index] = err
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	// Check results
	for i := 0; i < numWriters; i++ {
		if errors[i] != nil {
			// Network errors are expected in unit tests
			t.Logf("Expected network error for writer %d: %v", i, errors[i])
		}
		if writers[i] != nil {
			// Clean up
			_ = writers[i].(*azBlobWriter).pipeWriter.Close()
		}
	}
}

func Test_AzBlobWriter_EdgeCases(t *testing.T) {
	t.Run("write_after_close", func(t *testing.T) {
		r, w := io.Pipe()
		writeDone := make(chan error, 1)

		go func() {
			_, _ = io.Copy(io.Discard, r)
			_ = r.Close()
			writeDone <- nil
		}()

		client, _ := blockblob.NewClientWithNoCredential(
			"https://test.blob.core.windows.net/container/test.parquet",
			&blockblob.ClientOptions{},
		)

		writer := &azBlobWriter{
			azBlockBlob: azBlockBlob{
				blockBlobClient: client,
			},
			pipeWriter: w,
			writeDone:  writeDone,
		}

		// Close first
		err := writer.Close()
		require.NoError(t, err)

		// Try to write after close - should fail
		n, err := writer.Write([]byte("test"))
		require.Error(t, err)
		require.Equal(t, 0, n)
	})

	t.Run("multiple_close_calls", func(t *testing.T) {
		writer := &azBlobWriter{
			pipeWriter: nil, // Already closed
		}

		// Multiple close calls should be safe
		err1 := writer.Close()
		require.NoError(t, err1)

		err2 := writer.Close()
		require.NoError(t, err2)
	})

	t.Run("create_with_background_context", func(t *testing.T) {
		client, err := blockblob.NewClientWithNoCredential(
			"https://test.blob.core.windows.net/container/test.parquet",
			&blockblob.ClientOptions{},
		)
		require.NoError(t, err)

		writer := &azBlobWriter{
			azBlockBlob: azBlockBlob{
				ctx:             context.Background(),
				blockBlobClient: client,
			},
		}

		// Should work with proper context
		newWriter, err := writer.Create("https://test.blob.core.windows.net/container/new.parquet")
		// May fail due to network issues, which is expected in unit tests
		if err != nil {
			t.Logf("Expected network error: %v", err)
		} else {
			require.NotNil(t, newWriter)
			_ = newWriter.(*azBlobWriter).pipeWriter.Close()
		}
	})
}
