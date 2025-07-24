package mem

import (
	"io"
	"testing"

	"github.com/spf13/afero"
)

func Test_SetAndGetInMemFileFs(t *testing.T) {
	// Reset memFs to nil first
	memFs = nil

	// Create a new file system
	newFs := afero.NewMemMapFs()
	SetInMemFileFs(&newFs)

	// Test that it was set correctly
	retrievedFs := GetMemFileFs()
	if retrievedFs == nil {
		t.Fatal("Expected non-nil file system")
	}

	// Test that it's the same file system by creating a file in one and reading from the other
	testFile, err := newFs.Create("test.txt")
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	_, err = testFile.WriteString("test content")
	if err != nil {
		t.Fatalf("Failed to write to test file: %v", err)
	}
	err = testFile.Close()
	if err != nil {
		t.Errorf("Failed to close test file: %v", err)
	}

	// Check if we can read from retrieved fs
	readFile, err := retrievedFs.Open("test.txt")
	if err != nil {
		t.Fatalf("Failed to open test file from retrieved fs: %v", err)
	}
	content, err := io.ReadAll(readFile)
	if err != nil {
		t.Fatalf("Failed to read content: %v", err)
	}
	if string(content) != "test content" {
		t.Errorf("Expected 'test content', got %q", string(content))
	}
	err = readFile.Close()
	if err != nil {
		t.Errorf("Failed to close read file: %v", err)
	}
}

func Test_NewMemFileWriter(t *testing.T) {
	// Reset memFs to nil to test initialization
	memFs = nil

	onCloseCalled := false
	var closedFilename string
	var closedReader io.Reader

	onCloseFunc := func(filename string, reader io.Reader) error {
		onCloseCalled = true
		closedFilename = filename
		closedReader = reader
		return nil
	}

	writer, err := NewMemFileWriter("test.parquet", onCloseFunc)
	if err != nil {
		t.Fatalf("Failed to create mem writer: %v", err)
	}
	if writer == nil {
		t.Fatal("Expected non-nil writer")
	}

	// Test that memFs was initialized
	if memFs == nil {
		t.Error("Expected memFs to be initialized")
	}

	// Write some data
	testData := []byte("Hello, Mem Writer!")
	n, err := writer.Write(testData)
	if err != nil {
		t.Errorf("Failed to write data: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to write %d bytes, got %d", len(testData), n)
	}

	// Close the writer
	err = writer.Close()
	if err != nil {
		t.Errorf("Failed to close writer: %v", err)
	}

	// Check that onClose was called
	if !onCloseCalled {
		t.Error("Expected onClose function to be called")
	}
	if closedFilename != "test.parquet" {
		t.Errorf("Expected filename 'test.parquet', got %q", closedFilename)
	}
	if closedReader == nil {
		t.Error("Expected non-nil reader in onClose")
	}

	// Read the data from the closed reader to verify
	if closedReader != nil {
		content, err := io.ReadAll(closedReader)
		if err != nil {
			t.Errorf("Failed to read from closed reader: %v", err)
		}
		if string(content) != string(testData) {
			t.Errorf("Expected %q, got %q", string(testData), string(content))
		}
	}
}

func Test_NewMemFileWriterWithExistingFs(t *testing.T) {
	// Set up an existing file system
	existingFs := afero.NewMemMapFs()
	memFs = existingFs

	writer, err := NewMemFileWriter("test2.parquet", nil)
	if err != nil {
		t.Fatalf("Failed to create mem writer: %v", err)
	}

	// Should use the existing file system, not create a new one
	if memFs != existingFs {
		t.Error("Expected to use existing file system")
	}

	err = writer.Close()
	if err != nil {
		t.Errorf("Failed to close writer: %v", err)
	}
}

func Test_MemWriter_Create(t *testing.T) {
	memFs = afero.NewMemMapFs()

	writer := &memWriter{}
	created, err := writer.Create("created.parquet")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	if created == nil {
		t.Fatal("Expected non-nil created writer")
	}

	// Cast back to memWriter to check internal state
	memWriter := created.(*memWriter)
	if memWriter.filePath != "created.parquet" {
		t.Errorf("Expected filePath 'created.parquet', got %q", memWriter.filePath)
	}
	if memWriter.file == nil {
		t.Error("Expected non-nil file")
	}

	err = created.Close()
	if err != nil {
		t.Errorf("Failed to close created file: %v", err)
	}
}

func Test_MemWriter_Write(t *testing.T) {
	memFs = afero.NewMemMapFs()

	writer := &memWriter{}
	created, err := writer.Create("write_test.parquet")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	defer func() {
		if err := created.Close(); err != nil {
			t.Errorf("Failed to close created file: %v", err)
		}
	}()

	testData := []byte("Test write data")
	n, err := created.Write(testData)
	if err != nil {
		t.Errorf("Failed to write data: %v", err)
	}
	if n != len(testData) {
		t.Errorf("Expected to write %d bytes, got %d", len(testData), n)
	}

	// Close and verify the data was written
	err = created.Close()
	if err != nil {
		t.Errorf("Failed to close created file: %v", err)
	}

	// Read back from the file system
	file, err := memFs.Open("write_test.parquet")
	if err != nil {
		t.Fatalf("Failed to open written file: %v", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			t.Errorf("Failed to close file: %v", err)
		}
	}()

	content, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("Failed to read content: %v", err)
	}
	if string(content) != string(testData) {
		t.Errorf("Expected %q, got %q", string(testData), string(content))
	}
}

func Test_MemWriter_CloseWithoutOnCloseFunc(t *testing.T) {
	memFs = afero.NewMemMapFs()

	writer := &memWriter{}
	created, err := writer.Create("no_onclose.parquet")
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}

	// Write some data
	testData := []byte("No onClose test")
	_, err = created.Write(testData)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// Close without onClose function - should not error
	err = created.Close()
	if err != nil {
		t.Errorf("Close should not error without onClose function: %v", err)
	}
}

func Test_MemWriter_OnCloseError(t *testing.T) {
	memFs = afero.NewMemMapFs()

	onCloseError := func(filename string, reader io.Reader) error {
		return io.ErrUnexpectedEOF
	}

	writer, err := NewMemFileWriter("error_test.parquet", onCloseError)
	if err != nil {
		t.Fatalf("Failed to create mem writer: %v", err)
	}

	_, err = writer.Write([]byte("test"))
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// Close should return the error from onClose
	err = writer.Close()
	if err != io.ErrUnexpectedEOF {
		t.Errorf("Expected ErrUnexpectedEOF, got %v", err)
	}
}

func Test_MemWriter_WithSubdirectoryPath(t *testing.T) {
	memFs = afero.NewMemMapFs()

	onCloseCalled := false
	var closedFilename string

	onCloseFunc := func(filename string, reader io.Reader) error {
		onCloseCalled = true
		closedFilename = filename
		return nil
	}

	// Create writer with subdirectory path
	writer, err := NewMemFileWriter("subdir/test.parquet", onCloseFunc)
	if err != nil {
		t.Fatalf("Failed to create mem writer: %v", err)
	}

	_, err = writer.Write([]byte("subdir test"))
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}
	err = writer.Close()
	if err != nil {
		t.Errorf("Failed to close writer: %v", err)
	}

	// Check that onClose was called with just the base filename
	if !onCloseCalled {
		t.Error("Expected onClose function to be called")
	}
	if closedFilename != "test.parquet" {
		t.Errorf("Expected filename 'test.parquet', got %q", closedFilename)
	}
}
