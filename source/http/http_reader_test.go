package http

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/source"
)

// Mock data for testing
var testData = []byte("Hello, this is test data for HTTP reader testing with parquet file content simulation")

// Helper function to create a test HTTP server that supports range requests
func createTestServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")

		if rangeHeader == "" {
			// No range header - return full content
			w.Header().Set("Content-Length", strconv.Itoa(len(testData)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(testData)
			return
		}

		// Parse range header: "bytes=start-end"
		rangeStr := strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.Split(rangeStr, "-")
		if len(parts) != 2 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		start, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		end, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Validate range
		if start < 0 || end >= int64(len(testData)) || start > end {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}

		// Set range response headers
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(testData)))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(http.StatusPartialContent)

		// Write the requested range
		_, _ = w.Write(testData[start : end+1])
	}))
}

// Helper function to create a test HTTP server that does NOT support range requests
func createNonRangeServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Ignore range header and return full content
		w.Header().Set("Content-Length", strconv.Itoa(len(testData)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(testData)
	}))
}

// Helper function to create a server that returns malformed Content-Range headers
func createMalformedRangeServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "malformed-range-header")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("test"))
	}))
}

// Note: Multipart file reader tests are complex due to the need to mock
// multipart.FileHeader and multipart.File interfaces. The existing basic
// test coverage should be sufficient for now, as the multipart wrapper
// is a simple passthrough implementation.

func Test_HttpReader_Clone(t *testing.T) {
	server := createTestServer()
	defer server.Close()

	reader, err := NewHttpReader(server.URL, false, false, nil)
	require.NoError(t, err)

	// Test Clone method
	clonedReader, err := reader.Clone()
	require.NoError(t, err)

	require.NotNil(t, clonedReader)

	// Verify it's a different instance
	require.NotSame(t, reader, clonedReader)
}

func Test_HttpReader_Close(t *testing.T) {
	server := createTestServer()
	defer server.Close()

	reader, err := NewHttpReader(server.URL, false, false, nil)
	require.NoError(t, err)

	// Test Close method (should not return error)
	err = reader.Close()
	require.NoError(t, err)
}

func Test_HttpReader_Open(t *testing.T) {
	server := createTestServer()
	defer server.Close()

	reader, err := NewHttpReader(server.URL, false, false, nil)
	require.NoError(t, err)

	// Test Open method
	newReader, err := reader.Open("ignored")
	require.NoError(t, err)

	require.NotNil(t, newReader)

	// Verify it's a different instance
	require.NotSame(t, reader, newReader)
}

func Test_HttpReader_Read(t *testing.T) {
	server := createTestServer()
	defer server.Close()

	reader, err := NewHttpReader(server.URL, false, false, nil)
	require.NoError(t, err)

	// Test reading first 10 bytes
	buffer := make([]byte, 10)
	n, err := reader.Read(buffer)
	require.NoError(t, err)

	require.Equal(t, 10, n)

	expected := testData[:10]
	for i := 0; i < 10; i++ {
		require.Equal(t, expected[i], buffer[i], "Byte %d mismatch", i)
	}
}

func Test_HttpReader_ReadAfterSeek(t *testing.T) {
	server := createTestServer()
	defer server.Close()

	reader, err := NewHttpReader(server.URL, false, false, nil)
	require.NoError(t, err)

	// Seek to position 20
	_, err = reader.Seek(20, io.SeekStart)
	require.NoError(t, err)

	// Read 10 bytes from position 20
	buffer := make([]byte, 10)
	n, err := reader.Read(buffer)
	require.NoError(t, err)

	require.Equal(t, 10, n)

	expected := testData[20:30]
	for i := 0; i < 10; i++ {
		require.Equal(t, expected[i], buffer[i], "Byte %d mismatch", i)
	}
}

func Test_HttpReader_ReadMultiple(t *testing.T) {
	server := createTestServer()
	defer server.Close()

	reader, err := NewHttpReader(server.URL, false, false, nil)
	require.NoError(t, err)

	// Read in two chunks to test multiple reads
	firstChunk := make([]byte, 30)
	n1, err := reader.Read(firstChunk)
	require.NoError(t, err)

	require.Equal(t, 30, n1)

	// Read the remaining data
	remainingSize := len(testData) - 30
	secondChunk := make([]byte, remainingSize)
	n2, err := reader.Read(secondChunk)
	require.NoError(t, err)

	require.Equal(t, remainingSize, n2)

	// Combine and verify
	totalRead := make([]byte, 0, len(testData))
	totalRead = append(totalRead, firstChunk[:n1]...)
	totalRead = append(totalRead, secondChunk[:n2]...)

	require.Equal(t, len(testData), len(totalRead))

	for i := 0; i < len(testData) && i < len(totalRead); i++ {
		require.Equal(t, testData[i], totalRead[i], "Byte %d mismatch", i)
	}
}

func Test_HttpReader_Seek(t *testing.T) {
	server := createTestServer()
	defer server.Close()

	reader, err := NewHttpReader(server.URL, false, false, nil)
	require.NoError(t, err)

	// Test seek from start
	offset, err := reader.Seek(10, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(10), offset)

	// Test seek current
	offset, err = reader.Seek(5, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(15), offset)

	// Test seek from end
	offset, err = reader.Seek(-10, io.SeekEnd)
	require.NoError(t, err)
	expectedOffset := int64(len(testData)) - 10
	require.Equal(t, expectedOffset, offset)
}

func Test_HttpReader_SeekBoundaries(t *testing.T) {
	server := createTestServer()
	defer server.Close()

	reader, err := NewHttpReader(server.URL, false, false, nil)
	require.NoError(t, err)

	// Test seek to negative offset (should clamp to 0)
	offset, err := reader.Seek(-100, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)

	// Test seek beyond end (should clamp to size)
	offset, err = reader.Seek(int64(len(testData))+100, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(len(testData)), offset)
}

func Test_HttpReader_SeekInvalidWhence(t *testing.T) {
	server := createTestServer()
	defer server.Close()

	reader, err := NewHttpReader(server.URL, false, false, nil)
	require.NoError(t, err)

	// Test invalid whence
	_, err = reader.Seek(0, 999)
	require.Error(t, err)

	expectedError := "unknown whence"
	require.Contains(t, err.Error(), expectedError)
}

func Test_NewHttpReader_DedicatedTransport(t *testing.T) {
	server := createTestServer()
	defer server.Close()

	reader, err := NewHttpReader(server.URL, true, false, nil)
	require.NoError(t, err)

	require.NotNil(t, reader)
}

func Test_NewHttpReader_IgnoreTLSError(t *testing.T) {
	server := createTestServer()
	defer server.Close()

	reader, err := NewHttpReader(server.URL, false, true, nil)
	require.NoError(t, err)

	require.NotNil(t, reader)
}

func Test_NewHttpReader_InvalidURL(t *testing.T) {
	invalidURL := "not-a-valid-url"

	_, err := NewHttpReader(invalidURL, false, false, nil)
	require.Error(t, err)
}

func Test_NewHttpReader_MalformedContentRange(t *testing.T) {
	server := createMalformedRangeServer()
	defer server.Close()

	_, err := NewHttpReader(server.URL, false, false, nil)
	require.Error(t, err)

	expectedError := "format is unknown"
	require.Contains(t, err.Error(), expectedError)
}

func Test_NewHttpReader_NoRangeSupport(t *testing.T) {
	server := createNonRangeServer()
	defer server.Close()

	_, err := NewHttpReader(server.URL, false, false, nil)
	require.Error(t, err)

	expectedError := "does not support range"
	require.Contains(t, err.Error(), expectedError)
}

func Test_NewHttpReader_Success(t *testing.T) {
	server := createTestServer()
	defer server.Close()

	reader, err := NewHttpReader(server.URL, false, false, nil)
	require.NoError(t, err)

	require.NotNil(t, reader)

	// Test that it implements the interface
	var _ source.ParquetFileReader = reader
}

func Test_NewHttpReader_WithExtraHeaders(t *testing.T) {
	server := createTestServer()
	defer server.Close()

	extraHeaders := map[string]string{
		"Authorization": "Bearer token",
		"User-Agent":    "test-agent",
	}

	reader, err := NewHttpReader(server.URL, false, false, extraHeaders)
	require.NoError(t, err)

	require.NotNil(t, reader)
}

func Test_SetDefaultClient(t *testing.T) {
	// Save original client
	originalClient := defaultClient
	defer func() {
		defaultClient = originalClient
	}()

	// Create a custom client
	customClient := &http.Client{}

	// Set as default
	SetDefaultClient(customClient)

	require.Equal(t, customClient, defaultClient)

	// Test that NewHttpReader uses the default client
	server := createTestServer()
	defer server.Close()

	reader, err := NewHttpReader(server.URL, false, false, nil)
	require.NoError(t, err)

	httpReader := reader.(*httpReader)
	require.Equal(t, customClient, httpReader.httpClient)
}

// Integration test with real-world HTTP endpoints
func Test_http_reader_no_range_support(t *testing.T) {
	testCases := []struct {
		URL           string
		expectedError error
	}{
		{"https://no-such-host.tld/", errors.New("no such host")},
		{"https://www.google.com/", errors.New("does not support range")},
		{"https://pandemicdatalake.blob.core.windows.net/public/curated/covid-19/bing_covid-19_data/latest/bing_covid-19_data.parquet", nil},
	}

	for _, tc := range testCases {
		_, err := NewHttpReader(tc.URL, false, false, map[string]string{})
		if tc.expectedError == nil {
			require.NoError(t, err, "expected nil error but got [%v]", err)
		} else {
			require.Error(t, err, "expected error like [%v] but got nil", tc.expectedError)
			require.Contains(t, err.Error(), tc.expectedError.Error(), "expected error like [%v] but got [%v]", tc.expectedError, err)
		}
	}
}

func Test_NewHttpReader_ConcurrentSafety(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Length", "1000")
		w.Header().Set("Content-Range", "bytes 0-0/1000")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("x"))
	}))
	defer server.Close()

	const numGoroutines = 100
	results := make(chan error, numGoroutines)

	// Test concurrent calls with different configurations to trigger race conditions
	for i := 0; i < numGoroutines; i++ {
		go func(idx int) {
			// Alternate between dedicated transport and shared transport
			dedicatedTransport := idx%2 == 0
			ignoreTLS := idx%3 == 0

			reader, err := NewHttpReader(server.URL, dedicatedTransport, ignoreTLS, nil)
			if err != nil {
				results <- err
				return
			}
			defer func() {
				_ = reader.Close()
			}()

			// Verify we got a valid reader
			if reader == nil {
				results <- fmt.Errorf("reader is nil")
				return
			}

			results <- nil
		}(i)
	}

	// Collect all results
	for i := 0; i < numGoroutines; i++ {
		err := <-results
		require.NoError(t, err)
	}
}
