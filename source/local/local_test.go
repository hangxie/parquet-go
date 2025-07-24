package local

import (
	"os"
	"path/filepath"
	"testing"
)

func Test_NewLocalFileReader(t *testing.T) {
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
				if err != nil {
					t.Fatalf("Failed to create test file: %v", err)
				}
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
			if testCase.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !testCase.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			// Check reader nullness expectation
			if testCase.expectNilReader && reader != nil {
				t.Error("Expected nil reader but got non-nil")
			}
			if !testCase.expectNilReader && reader == nil {
				t.Error("Expected non-nil reader but got nil")
			}

			// Clean up if reader was created successfully
			if reader != nil {
				if closeErr := reader.Close(); closeErr != nil {
					t.Errorf("Failed to close reader: %v", closeErr)
				}
			}
		})
	}
}

func Test_LocalReader_Open(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.parquet")

	err := os.WriteFile(tmpFile, []byte("test data"), 0o644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	reader := &localReader{}
	opened, err := reader.Open(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	if opened == nil {
		t.Fatal("Expected non-nil opened reader")
	}

	err = opened.Close()
	if err != nil {
		t.Errorf("Failed to close reader: %v", err)
	}
}

func Test_LocalReader_Clone(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.parquet")

	err := os.WriteFile(tmpFile, []byte("test data"), 0o644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	reader, err := NewLocalFileReader(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("Failed to close reader: %v", err)
		}
	}()

	cloned, err := reader.Clone()
	if err != nil {
		t.Fatalf("Failed to clone reader: %v", err)
	}
	if cloned == nil {
		t.Fatal("Expected non-nil cloned reader")
	}

	err = cloned.Close()
	if err != nil {
		t.Errorf("Failed to close cloned reader: %v", err)
	}
}

func Test_LocalReader_Read(t *testing.T) {
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
			if err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}

			// Create reader
			reader, err := NewLocalFileReader(tmpFile)
			if err != nil {
				t.Fatalf("Failed to open file: %v", err)
			}
			defer func() {
				if err := reader.Close(); err != nil {
					t.Errorf("Failed to close reader: %v", err)
				}
			}()

			// Test reading
			buffer := make([]byte, testCase.bufferSize)
			n, err := reader.Read(buffer)

			// Check error expectation
			if testCase.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !testCase.expectError && err != nil && err.Error() != "EOF" {
				t.Errorf("Unexpected error: %v", err)
			}

			// Check bytes read
			if n != testCase.expectedRead {
				t.Errorf("Expected to read %d bytes, got %d", testCase.expectedRead, n)
			}

			// Verify data correctness (only for non-empty reads)
			if n > 0 {
				expectedData := testCase.testData[:n]
				actualData := buffer[:n]
				if string(actualData) != string(expectedData) {
					t.Errorf("Expected %q, got %q", string(expectedData), string(actualData))
				}
			}
		})
	}
}

func Test_LocalReader_Seek(t *testing.T) {
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
			if err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}

			// Create reader
			reader, err := NewLocalFileReader(tmpFile)
			if err != nil {
				t.Fatalf("Failed to open file: %v", err)
			}
			defer func() {
				if err := reader.Close(); err != nil {
					t.Errorf("Failed to close reader: %v", err)
				}
			}()

			// Test seeking
			actualOffset, err := reader.Seek(testCase.seekOffset, testCase.seekWhence)

			// Check error expectation
			if testCase.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !testCase.expectError && err != nil {
				t.Errorf("Unexpected seek error: %v", err)
			}

			// Check offset
			if actualOffset != testCase.expectedOffset {
				t.Errorf("Expected offset %d, got %d", testCase.expectedOffset, actualOffset)
			}

			// Test reading from the seeked position
			if testCase.readSize > 0 {
				buffer := make([]byte, testCase.readSize)
				n, readErr := reader.Read(buffer)

				// Check read data
				actualRead := string(buffer[:n])
				if actualRead != testCase.expectedRead {
					t.Errorf("Expected to read %q, got %q", testCase.expectedRead, actualRead)
				}

				// EOF is expected when reading past end, so don't treat it as error
				if readErr != nil && readErr.Error() != "EOF" && testCase.expectedRead != "" {
					t.Errorf("Unexpected read error after seek: %v", readErr)
				}
			}
		})
	}
}

func Test_LocalReader_PartialRead(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test.parquet")
	testData := []byte("Hello, World!")

	err := os.WriteFile(tmpFile, testData, 0o644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	reader, err := NewLocalFileReader(tmpFile)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Errorf("Failed to close reader: %v", err)
		}
	}()

	// Test reading with a larger buffer than the file
	buffer := make([]byte, 20)
	n, err := reader.Read(buffer)
	if err != nil && err.Error() != "EOF" {
		t.Errorf("Unexpected error: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to read %d bytes, got %d", len(testData), n)
	}
	if string(buffer[:n]) != string(testData) {
		t.Errorf("Expected %q, got %q", string(testData), string(buffer[:n]))
	}
}

func Test_NewLocalFileWriter(t *testing.T) {
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
			if testCase.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !testCase.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			// Check writer nullness expectation
			if testCase.expectNilWriter && writer != nil {
				t.Error("Expected nil writer but got non-nil")
			}
			if !testCase.expectNilWriter && writer == nil {
				t.Error("Expected non-nil writer but got nil")
			}

			// Clean up and verify file creation if writer was created successfully
			if writer != nil {
				if closeErr := writer.Close(); closeErr != nil {
					t.Errorf("Failed to close writer: %v", closeErr)
				}

				// Check file creation expectation
				if testCase.shouldCreateFile {
					if _, statErr := os.Stat(filePath); os.IsNotExist(statErr) {
						t.Error("Expected file to be created but it was not")
					}
				}
			}
		})
	}
}

func Test_LocalWriter_Create(t *testing.T) {
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test_create.parquet")

	writer := &localWriter{}
	created, err := writer.Create(tmpFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	if created == nil {
		t.Fatal("Expected non-nil created writer")
	}

	err = created.Close()
	if err != nil {
		t.Errorf("Failed to close writer: %v", err)
	}

	// Check that file was created
	if _, err := os.Stat(tmpFile); os.IsNotExist(err) {
		t.Error("File was not created")
	}
}

func Test_LocalWriter_Write(t *testing.T) {
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
			if err != nil {
				t.Fatalf("Failed to create writer: %v", err)
			}

			// Perform writes
			var totalWritten int
			for i, data := range testCase.writeData {
				n, writeErr := writer.Write(data)

				// Check individual write errors
				if testCase.expectError && writeErr == nil {
					t.Errorf("Write %d: Expected error but got none", i)
				}
				if !testCase.expectError && writeErr != nil {
					t.Errorf("Write %d: Unexpected error: %v", i, writeErr)
				}

				// Check bytes written
				if n != len(data) {
					t.Errorf("Write %d: Expected to write %d bytes, got %d", i, len(data), n)
				}

				totalWritten += n
			}

			// Close writer
			err = writer.Close()
			if err != nil {
				t.Errorf("Failed to close writer: %v", err)
			}

			// Verify the written data
			if !testCase.expectError {
				writtenData, readErr := os.ReadFile(tmpFile)
				if readErr != nil {
					t.Fatalf("Failed to read written file: %v", readErr)
				}

				actualData := string(writtenData)
				if actualData != testCase.expectedData {
					t.Errorf("Expected %q, got %q", testCase.expectedData, actualData)
				}

				if len(writtenData) != totalWritten {
					t.Errorf("Expected %d total bytes written, got %d", totalWritten, len(writtenData))
				}
			}
		})
	}
}
