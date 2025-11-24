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

func TestHttpReader_Clone(t *testing.T) {
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

func TestHttpReader_Close(t *testing.T) {
	server := createTestServer()
	defer server.Close()

	reader, err := NewHttpReader(server.URL, false, false, nil)
	require.NoError(t, err)

	// Test Close method (should not return error)
	err = reader.Close()
	require.NoError(t, err)
}

func TestHttpReader_Open(t *testing.T) {
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

func TestHttpReader_Read(t *testing.T) {
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

func TestHttpReader_ReadAfterSeek(t *testing.T) {
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

func TestHttpReader_ReadMultiple(t *testing.T) {
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

func TestHttpReader_Seek(t *testing.T) {
	tests := []struct {
		name           string
		offset         int64
		whence         int
		expectedOffset int64 // -1 means calculate dynamically
		errMsg         string
	}{
		{
			name:           "seek_from_start",
			offset:         10,
			whence:         io.SeekStart,
			expectedOffset: 10,
			errMsg:         "",
		},
		{
			name:           "seek_current",
			offset:         5,
			whence:         io.SeekCurrent,
			expectedOffset: 5, // Will be adjusted in test
			errMsg:         "",
		},
		{
			name:           "seek_from_end",
			offset:         -10,
			whence:         io.SeekEnd,
			expectedOffset: -1, // Will be calculated as len(testData) - 10
			errMsg:         "",
		},
		{
			name:           "seek_negative_offset_clamped",
			offset:         -100,
			whence:         io.SeekStart,
			expectedOffset: 0,
			errMsg:         "",
		},
		{
			name:           "seek_beyond_end_clamped",
			offset:         int64(len(testData)) + 100,
			whence:         io.SeekStart,
			expectedOffset: -1, // Will be calculated as len(testData)
			errMsg:         "",
		},
		{
			name:           "invalid_whence",
			offset:         0,
			whence:         999,
			expectedOffset: 0,
			errMsg:         "unknown whence",
		},
	}

	server := createTestServer()
	defer server.Close()

	reader, err := NewHttpReader(server.URL, false, false, nil)
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset reader position for each test
			if tt.name != "seek_current" {
				_, _ = reader.Seek(0, io.SeekStart)
			} else {
				// For seek_current test, start at position 10
				_, _ = reader.Seek(10, io.SeekStart)
				tt.expectedOffset = 15 // 10 + 5
			}

			offset, err := reader.Seek(tt.offset, tt.whence)

			if tt.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)

				expectedOff := tt.expectedOffset
				if expectedOff == -1 {
					if tt.name == "seek_from_end" {
						expectedOff = int64(len(testData)) - 10
					} else if tt.name == "seek_beyond_end_clamped" {
						expectedOff = int64(len(testData))
					}
				}

				require.Equal(t, expectedOff, offset)
			}
		})
	}
}

func TestNewHttpReader(t *testing.T) {
	tests := []struct {
		name               string
		setupServer        func() (string, func()) // returns URL and cleanup function
		dedicatedTransport bool
		ignoreTLS          bool
		extraHeaders       map[string]string
		errMsg             string // empty string means no error expected
	}{
		{
			name: "success",
			setupServer: func() (string, func()) {
				server := createTestServer()
				return server.URL, server.Close
			},
			dedicatedTransport: false,
			ignoreTLS:          false,
			extraHeaders:       nil,
			errMsg:             "",
		},
		{
			name: "dedicated_transport",
			setupServer: func() (string, func()) {
				server := createTestServer()
				return server.URL, server.Close
			},
			dedicatedTransport: true,
			ignoreTLS:          false,
			extraHeaders:       nil,
			errMsg:             "",
		},
		{
			name: "ignore_tls_error",
			setupServer: func() (string, func()) {
				server := createTestServer()
				return server.URL, server.Close
			},
			dedicatedTransport: false,
			ignoreTLS:          true,
			extraHeaders:       nil,
			errMsg:             "",
		},
		{
			name: "with_extra_headers",
			setupServer: func() (string, func()) {
				server := createTestServer()
				return server.URL, server.Close
			},
			dedicatedTransport: false,
			ignoreTLS:          false,
			extraHeaders: map[string]string{
				"Authorization": "Bearer token",
				"User-Agent":    "test-agent",
			},
			errMsg: "",
		},
		{
			name: "invalid_url",
			setupServer: func() (string, func()) {
				return "not-a-valid-url", func() {}
			},
			dedicatedTransport: false,
			ignoreTLS:          false,
			extraHeaders:       nil,
			errMsg:             "", // Any error is acceptable for invalid URL
		},
		{
			name: "malformed_content_range",
			setupServer: func() (string, func()) {
				server := createMalformedRangeServer()
				return server.URL, server.Close
			},
			dedicatedTransport: false,
			ignoreTLS:          false,
			extraHeaders:       nil,
			errMsg:             "format is unknown",
		},
		{
			name: "no_range_support",
			setupServer: func() (string, func()) {
				server := createNonRangeServer()
				return server.URL, server.Close
			},
			dedicatedTransport: false,
			ignoreTLS:          false,
			extraHeaders:       nil,
			errMsg:             "does not support range",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url, cleanup := tt.setupServer()
			defer cleanup()

			reader, err := NewHttpReader(url, tt.dedicatedTransport, tt.ignoreTLS, tt.extraHeaders)

			if tt.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else if tt.name == "invalid_url" {
				// For invalid URL, just expect an error
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, reader)

				// For the success case, verify it implements the interface
				if tt.name == "success" {
					var _ source.ParquetFileReader = reader
				}
			}
		})
	}
}

func TestSetDefaultClient(t *testing.T) {
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
func TestHttp_reader_no_range_support(t *testing.T) {
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

func TestNewHttpReader_ConcurrentSafety(t *testing.T) {
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
