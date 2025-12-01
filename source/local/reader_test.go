package local

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalReader_Clone(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.parquet")

	testData := []byte("0123456789ABCDEF")
	err := os.WriteFile(tmpFile, testData, 0o644)
	require.NoError(t, err)

	reader1, err := NewLocalFileReader(tmpFile)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader1.Close())
	}()

	buf1 := make([]byte, 3)
	n1, err := reader1.Read(buf1)
	require.NoError(t, err)
	require.Equal(t, 3, n1)
	require.Equal(t, testData[:3], buf1)

	reader2, err := reader1.Clone()
	require.NoError(t, err)
	require.NotNil(t, reader2)
	defer func() {
		require.NoError(t, reader2.Close())
	}()

	buf2 := make([]byte, 3)
	n2, err := reader2.Read(buf2)
	require.NoError(t, err)
	require.Equal(t, 3, n2)
	require.Equal(t, buf1, buf2)

	buf3 := make([]byte, 1)
	n3, err := reader1.Read(buf3)
	require.NoError(t, err)
	require.Equal(t, 1, n3)

	buf4 := make([]byte, 1)
	n4, err := reader2.Read(buf4)
	require.NoError(t, err)
	require.Equal(t, 1, n4)

	require.Equal(t, buf3, buf4)
	require.Equal(t, testData[3:4], buf3)
}

func TestLocalReader_Open(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.parquet")

	err := os.WriteFile(tmpFile, []byte("test data"), 0o644)
	require.NoError(t, err)

	reader := &localReader{}
	opened, err := reader.Open(tmpFile)
	require.NoError(t, err)
	require.NotNil(t, opened)

	err = opened.Close()
	require.NoError(t, err)
}

func TestLocalReader_PartialRead(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.parquet")
	testData := []byte("Hello, World!")

	err := os.WriteFile(tmpFile, testData, 0o644)
	require.NoError(t, err)

	reader, err := NewLocalFileReader(tmpFile)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	// Test reading with a larger buffer than the file
	buffer := make([]byte, 20)
	n, err := reader.Read(buffer)
	if err != nil && !errors.Is(err, io.EOF) {
		require.NoError(t, err)
	}
	require.Equal(t, len(testData), n)
	require.Equal(t, string(testData), string(buffer[:n]))
}

func TestLocalReader_Read(t *testing.T) {
	testCases := []struct {
		name         string
		testData     []byte
		bufferSize   int
		expectError  bool
		expectedRead int
	}{
		{
			name:         "exact-buffer-size",
			testData:     []byte("Hello, World!"),
			bufferSize:   13, // Exact size
			expectError:  false,
			expectedRead: 13,
		},
		{
			name:         "larger-buffer-than-file",
			testData:     []byte("Hello, World!"),
			bufferSize:   20,    // Larger than file
			expectError:  false, // EOF is not considered an error in this context
			expectedRead: 13,
		},
		{
			name:         "small-buffer",
			testData:     []byte("Hello, World!"),
			bufferSize:   5, // Smaller than file
			expectError:  false,
			expectedRead: 5,
		},
		{
			name:         "empty-file",
			testData:     []byte(""),
			bufferSize:   10,
			expectError:  false,
			expectedRead: 0,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Setup test file
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "test.parquet")

			err := os.WriteFile(tmpFile, testCase.testData, 0o644)
			require.NoError(t, err)

			// Create reader
			reader, err := NewLocalFileReader(tmpFile)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, reader.Close())
			}()

			// Test reading
			buffer := make([]byte, testCase.bufferSize)
			n, err := reader.Read(buffer)

			// Check error expectation
			if testCase.expectError {
				require.Error(t, err)
			}
			if !testCase.expectError && err != nil && !errors.Is(err, io.EOF) {
				require.NoError(t, err)
			}

			// Check bytes read
			require.Equal(t, testCase.expectedRead, n)

			// Verify data correctness (only for non-empty reads)
			if n > 0 {
				expectedData := testCase.testData[:n]
				actualData := buffer[:n]
				require.Equal(t, string(expectedData), string(actualData))
			}
		})
	}
}

func TestLocalReader_Seek(t *testing.T) {
	testData := []byte("Hello, World!")

	testCases := []struct {
		name           string
		seekOffset     int64
		seekWhence     int
		expectedOffset int64
		readSize       int
		expectedRead   string
		expectError    bool
	}{
		{
			name:           "seek-to-middle-from-start",
			seekOffset:     7,
			seekWhence:     0, // os.SEEK_SET
			expectedOffset: 7,
			readSize:       6,
			expectedRead:   "World!",
			expectError:    false,
		},
		{
			name:           "seek-to-beginning",
			seekOffset:     0,
			seekWhence:     0, // os.SEEK_SET
			expectedOffset: 0,
			readSize:       5,
			expectedRead:   "Hello",
			expectError:    false,
		},
		{
			name:           "seek-to-end",
			seekOffset:     0,
			seekWhence:     2, // os.SEEK_END
			expectedOffset: int64(len(testData)),
			readSize:       5,
			expectedRead:   "", // Nothing to read at EOF
			expectError:    false,
		},
		{
			name:           "seek-past-end",
			seekOffset:     100,
			seekWhence:     0, // os.SEEK_SET
			expectedOffset: 100,
			readSize:       5,
			expectedRead:   "", // Nothing to read past EOF
			expectError:    false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Setup test file
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "test.parquet")

			err := os.WriteFile(tmpFile, testData, 0o644)
			require.NoError(t, err)

			// Create reader
			reader, err := NewLocalFileReader(tmpFile)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, reader.Close())
			}()

			// Test seeking
			actualOffset, err := reader.Seek(testCase.seekOffset, testCase.seekWhence)

			// Check error expectation
			if testCase.expectError {
				require.Error(t, err)
			}
			if !testCase.expectError {
				require.NoError(t, err)
			}

			// Check offset
			require.Equal(t, testCase.expectedOffset, actualOffset)

			// Test reading from the seeked position
			if testCase.readSize > 0 {
				buffer := make([]byte, testCase.readSize)
				n, readErr := reader.Read(buffer)

				// Check read data
				actualRead := string(buffer[:n])
				require.Equal(t, testCase.expectedRead, actualRead)

				// EOF is expected when reading past end, so don't treat it as error
				if readErr != nil && readErr.Error() != "EOF" && testCase.expectedRead != "" {
					require.NoError(t, readErr)
				}
			}
		})
	}
}

func TestNewLocalFileReader(t *testing.T) {
	testCases := []struct {
		name            string
		setupFile       func(t *testing.T) string
		expectError     bool
		expectNilReader bool
	}{
		{
			name: "valid-existing-file",
			setupFile: func(t *testing.T) string {
				tmpDir := t.TempDir()
				tmpFile := filepath.Join(tmpDir, "test.parquet")
				err := os.WriteFile(tmpFile, []byte("test data"), 0o644)
				require.NoError(t, err)
				return tmpFile
			},
			expectError:     false,
			expectNilReader: false,
		},
		{
			name: "non-existent-file",
			setupFile: func(t *testing.T) string {
				return "/non/existent/file.parquet"
			},
			expectError:     true,
			expectNilReader: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			filePath := testCase.setupFile(t)

			reader, err := NewLocalFileReader(filePath)

			// Check error expectation
			if testCase.expectError {
				require.Error(t, err)
			}
			if !testCase.expectError {
				require.NoError(t, err)
			}

			// Check reader nullness expectation
			if testCase.expectNilReader {
				require.Nil(t, reader)
			}
			if !testCase.expectNilReader {
				require.NotNil(t, reader)
			}

			// Clean up if reader was created successfully
			if reader != nil {
				require.NoError(t, reader.Close())
			}
		})
	}
}
