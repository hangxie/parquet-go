package http

import (
	"bytes"
	"io"
	"mime/multipart"
	nethttp "net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/source"
)

// mockMultipartFile implements multipart.File for testing
type mockMultipartFile struct {
	*bytes.Reader
	closed bool
}

func newMockMultipartFile(data []byte) *mockMultipartFile {
	return &mockMultipartFile{
		Reader: bytes.NewReader(data),
		closed: false,
	}
}

func (m *mockMultipartFile) Close() error {
	m.closed = true
	return nil
}

func (m *mockMultipartFile) ReadAt(p []byte, off int64) (n int, err error) {
	// Seek to offset and read
	_, seekErr := m.Seek(off, io.SeekStart)
	if seekErr != nil {
		return 0, seekErr
	}
	return m.Read(p)
}

// createMockFileHeader creates a mock multipart.FileHeader for testing
func createMockFileHeader(filename string, data []byte) *multipart.FileHeader {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)

	// Create a form file
	formFile, err := writer.CreateFormFile("file", filename)
	if err != nil {
		panic(err)
	}

	// Write test data
	_, err = formFile.Write(data)
	if err != nil {
		panic(err)
	}

	err = writer.Close()
	if err != nil {
		panic(err)
	}

	// Parse the multipart data to get the FileHeader
	reader := multipart.NewReader(&buf, writer.Boundary())
	part, err := reader.NextPart()
	if err != nil {
		panic(err)
	}

	// Read the data to ensure it's complete
	_, err = io.ReadAll(part)
	if err != nil {
		panic(err)
	}

	// Create a simple FileHeader for testing
	return &multipart.FileHeader{
		Filename: filename,
		Header:   part.Header,
		Size:     int64(len(data)),
	}
}

func TestNewMultipartFileWrapper(t *testing.T) {
	testData := []byte("test multipart file content")
	mockFile := newMockMultipartFile(testData)
	fileHeader := createMockFileHeader("test.parquet", testData)

	reader := NewMultipartFileWrapper(fileHeader, mockFile)

	require.NotNil(t, reader)

	// Verify it implements the interface
	var _ source.ParquetFileReader = reader
}

func TestMultipartFileReader_InterfaceCompliance(t *testing.T) {
	var _ source.ParquetFileReader = (*multipartFileReader)(nil)
}

func TestMultipartFileReader_Clone(t *testing.T) {
	testData := []byte("test data for cloning")
	mockFile := newMockMultipartFile(testData)
	fileHeader := createMockFileHeader("test.parquet", testData)

	reader := NewMultipartFileWrapper(fileHeader, mockFile)

	clonedReader, err := reader.Clone()
	require.NoError(t, err)
	require.NotNil(t, clonedReader)

	// Verify clone is a different instance but with same data
	require.NotSame(t, reader, clonedReader)

	// Verify cloned reader implements the interface
	var _ source.ParquetFileReader = clonedReader
}

func TestMultipartFileReader_Read(t *testing.T) {
	testData := []byte("Hello, this is test data for multipart reader!")
	mockFile := newMockMultipartFile(testData)
	fileHeader := createMockFileHeader("test.parquet", testData)

	reader := NewMultipartFileWrapper(fileHeader, mockFile)

	// Test reading part of the data
	buffer := make([]byte, 10)
	n, err := reader.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.Equal(t, "Hello, thi", string(buffer))

	// Test reading more data
	buffer2 := make([]byte, 20)
	n2, err := reader.Read(buffer2)
	require.NoError(t, err)
	require.Equal(t, 20, n2)
	require.Equal(t, "s is test data for m", string(buffer2))

	// Test reading remaining data
	remaining := make([]byte, 100)
	n3, err := reader.Read(remaining)
	if err != nil {
		require.Equal(t, io.EOF, err)
	}
	expectedRemaining := "ultipart reader!"
	require.Equal(t, len(expectedRemaining), n3)
	require.Equal(t, expectedRemaining, string(remaining[:n3]))
}

func TestMultipartFileReader_Read_EmptyFile(t *testing.T) {
	testData := []byte("")
	mockFile := newMockMultipartFile(testData)
	fileHeader := createMockFileHeader("empty.parquet", testData)

	reader := NewMultipartFileWrapper(fileHeader, mockFile)

	buffer := make([]byte, 10)
	n, err := reader.Read(buffer)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)
}

func TestMultipartFileReader_Seek(t *testing.T) {
	testData := []byte("0123456789ABCDEFGHIJ")
	mockFile := newMockMultipartFile(testData)
	fileHeader := createMockFileHeader("test.parquet", testData)

	reader := NewMultipartFileWrapper(fileHeader, mockFile)

	// Test SeekStart
	offset, err := reader.Seek(5, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(5), offset)

	// Read from seeked position
	buffer := make([]byte, 5)
	n, err := reader.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, "56789", string(buffer))

	// Test SeekCurrent
	offset, err = reader.Seek(2, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(12), offset)

	// Read from new position
	buffer2 := make([]byte, 3)
	n2, err := reader.Read(buffer2)
	require.NoError(t, err)
	require.Equal(t, 3, n2)
	require.Equal(t, "CDE", string(buffer2))

	// Test SeekEnd
	offset, err = reader.Seek(-5, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(15), offset)

	// Read from end position
	buffer3 := make([]byte, 10)
	n3, err := reader.Read(buffer3)
	if err != nil && err != io.EOF {
		require.NoError(t, err)
	}
	require.Equal(t, 5, n3)
	require.Equal(t, "FGHIJ", string(buffer3[:n3]))
}

func TestMultipartFileReader_Seek_InvalidOffset(t *testing.T) {
	testData := []byte("test data")
	mockFile := newMockMultipartFile(testData)
	fileHeader := createMockFileHeader("test.parquet", testData)

	reader := NewMultipartFileWrapper(fileHeader, mockFile)

	// Test seeking to negative position from start (should be handled by underlying reader)
	offset, err := reader.Seek(-1, io.SeekStart)
	// The behavior depends on the underlying bytes.Reader implementation
	if err != nil {
		require.Error(t, err)
	} else {
		// If no error, verify the offset
		require.GreaterOrEqual(t, offset, int64(0))
	}
}

func TestMultipartFileReader_Close(t *testing.T) {
	testData := []byte("test data for close")
	mockFile := newMockMultipartFile(testData)
	fileHeader := createMockFileHeader("test.parquet", testData)

	reader := NewMultipartFileWrapper(fileHeader, mockFile)

	// Verify file is not closed initially
	require.False(t, mockFile.closed)

	// Close the reader
	err := reader.Close()
	require.NoError(t, err)

	// Verify file is closed
	require.True(t, mockFile.closed)
}

func TestMultipartFileReader_Open(t *testing.T) {
	testData := []byte("test data for open operation")

	// Build a real multipart form and parse it via http to get a valid FileHeader
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	formFile, err := writer.CreateFormFile("file", "test.parquet")
	require.NoError(t, err)
	_, err = formFile.Write(testData)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	req, err := nethttp.NewRequest(nethttp.MethodPost, "/", &buf)
	require.NoError(t, err)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	require.NoError(t, req.ParseMultipartForm(1<<20))

	fh := req.MultipartForm.File["file"][0]
	f, err := fh.Open()
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	mfr := NewMultipartFileWrapper(fh, f)

	// Open should succeed and return a new reader
	opened, err := mfr.Open("ignored")
	require.NoError(t, err)
	require.NotNil(t, opened)

	// The opened reader should be able to read the same data
	content, err := io.ReadAll(opened)
	require.NoError(t, err)
	require.Equal(t, testData, content)
	require.NoError(t, opened.Close())
}

func TestMultipartFileReader_Open_ErrorHandling(t *testing.T) {
	// Create a fileHeader that will fail to open
	testData := []byte("test data")
	mockFile := newMockMultipartFile(testData)

	// Create a fileHeader with nil or invalid data that will cause Open to fail
	fileHeader := &multipart.FileHeader{
		Filename: "test.parquet",
		Size:     int64(len(testData)),
	}

	reader := &multipartFileReader{
		fileHeader: fileHeader,
		file:       mockFile,
	}

	// Open should fail due to invalid fileHeader
	_, err := reader.Open("new-file.parquet")
	require.Error(t, err)
}

func TestMultipartFileReader_ReadWriteOperations(t *testing.T) {
	testData := []byte("Sequential read test data for multipart reader functionality")
	mockFile := newMockMultipartFile(testData)
	fileHeader := createMockFileHeader("test.parquet", testData)

	reader := NewMultipartFileWrapper(fileHeader, mockFile)

	// Test multiple sequential reads
	chunk1 := make([]byte, 10)
	n1, err := reader.Read(chunk1)
	require.NoError(t, err)
	require.Equal(t, 10, n1)

	chunk2 := make([]byte, 15)
	n2, err := reader.Read(chunk2)
	require.NoError(t, err)
	require.Equal(t, 15, n2)

	// Seek back to beginning
	offset, err := reader.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)

	// Read entire content
	fullContent := make([]byte, len(testData))
	n3, err := reader.Read(fullContent)
	require.NoError(t, err)
	require.Equal(t, len(testData), n3)
	require.Equal(t, string(testData), string(fullContent))
}

func TestMultipartFileReader_LargeDataHandling(t *testing.T) {
	// Create larger test data
	largeData := make([]byte, 1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	mockFile := newMockMultipartFile(largeData)
	fileHeader := createMockFileHeader("large.parquet", largeData)

	reader := NewMultipartFileWrapper(fileHeader, mockFile)

	// Read in chunks
	totalRead := 0
	chunkSize := 100

	for totalRead < len(largeData) {
		chunk := make([]byte, chunkSize)
		n, err := reader.Read(chunk)

		if err == io.EOF {
			require.Equal(t, len(largeData)-totalRead, n)
			totalRead += n
			break
		}

		require.NoError(t, err)
		require.Greater(t, n, 0)

		// Verify data integrity
		for i := 0; i < n; i++ {
			expected := byte((totalRead + i) % 256)
			require.Equal(t, expected, chunk[i])
		}

		totalRead += n
	}

	require.Equal(t, len(largeData), totalRead)
}

func TestMultipartFileReader_SeekBoundaryConditions(t *testing.T) {
	testData := []byte("Boundary test data")
	mockFile := newMockMultipartFile(testData)
	fileHeader := createMockFileHeader("test.parquet", testData)

	reader := NewMultipartFileWrapper(fileHeader, mockFile)

	// Seek to exact end
	offset, err := reader.Seek(0, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(len(testData)), offset)

	// Try to read at EOF
	buffer := make([]byte, 10)
	n, err := reader.Read(buffer)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 0, n)

	// Seek to beginning
	offset, err = reader.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)

	// Read first byte
	firstByte := make([]byte, 1)
	n, err = reader.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, testData[0], firstByte[0])
}
