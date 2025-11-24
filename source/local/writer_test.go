package local

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalWriter_Create(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test_create.parquet")

	writer := &localWriter{}
	created, err := writer.Create(tmpFile)
	require.NoError(t, err)
	require.NotNil(t, created)

	err = created.Close()
	require.NoError(t, err)

	// Check that file was created
	_, err = os.Stat(tmpFile)
	require.False(t, os.IsNotExist(err))
}

func TestLocalWriter_Write(t *testing.T) {
	testCases := []struct {
		name         string
		writeData    [][]byte // Support multiple writes
		expectedData string
		expectError  bool
	}{
		{
			name:         "single-write",
			writeData:    [][]byte{[]byte("Hello, Writer!")},
			expectedData: "Hello, Writer!",
			expectError:  false,
		},
		{
			name: "multiple-writes",
			writeData: [][]byte{
				[]byte("Hello, "),
				[]byte("Writer! "),
				[]byte("Multiple writes."),
			},
			expectedData: "Hello, Writer! Multiple writes.",
			expectError:  false,
		},
		{
			name:         "empty-write",
			writeData:    [][]byte{[]byte("")},
			expectedData: "",
			expectError:  false,
		},
		{
			name: "mixed-empty-and-data-writes",
			writeData: [][]byte{
				[]byte("Start"),
				[]byte(""),
				[]byte(" Middle "),
				[]byte(""),
				[]byte("End"),
			},
			expectedData: "Start Middle End",
			expectError:  false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Setup test file
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "test_write.parquet")

			// Create writer
			writer, err := NewLocalFileWriter(tmpFile)
			require.NoError(t, err)

			// Perform writes
			var totalWritten int
			for _, data := range testCase.writeData {
				n, writeErr := writer.Write(data)

				// Check individual write errors
				if testCase.expectError {
					require.Error(t, writeErr)
				}
				if !testCase.expectError {
					require.NoError(t, writeErr)
				}

				// Check bytes written
				require.Equal(t, len(data), n)

				totalWritten += n
			}

			// Close writer
			err = writer.Close()
			require.NoError(t, err)

			// Verify the written data
			if !testCase.expectError {
				writtenData, readErr := os.ReadFile(tmpFile)
				require.NoError(t, readErr)

				actualData := string(writtenData)
				require.Equal(t, testCase.expectedData, actualData)

				require.Equal(t, totalWritten, len(writtenData))
			}
		})
	}
}

func TestNewLocalFileWriter(t *testing.T) {
	testCases := []struct {
		name             string
		setupPath        func(t *testing.T) string
		expectError      bool
		expectNilWriter  bool
		shouldCreateFile bool
	}{
		{
			name: "valid-path-in-temp-directory",
			setupPath: func(t *testing.T) string {
				tmpDir := t.TempDir()
				return filepath.Join(tmpDir, "test_writer.parquet")
			},
			expectError:      false,
			expectNilWriter:  false,
			shouldCreateFile: true,
		},
		{
			name: "invalid-path-non-existent-directory",
			setupPath: func(t *testing.T) string {
				return "/non/existent/dir/test.parquet"
			},
			expectError:      true,
			expectNilWriter:  true,
			shouldCreateFile: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			filePath := testCase.setupPath(t)

			writer, err := NewLocalFileWriter(filePath)

			// Check error expectation
			if testCase.expectError {
				require.Error(t, err)
			}
			if !testCase.expectError {
				require.NoError(t, err)
			}

			// Check writer nullness expectation
			if testCase.expectNilWriter {
				require.Nil(t, writer)
			}
			if !testCase.expectNilWriter {
				require.NotNil(t, writer)
			}

			// Clean up and verify file creation if writer was created successfully
			if writer != nil {
				require.NoError(t, writer.Close())

				// Check file creation expectation
				if testCase.shouldCreateFile {
					_, statErr := os.Stat(filePath)
					require.False(t, os.IsNotExist(statErr))
				}
			}
		})
	}
}
