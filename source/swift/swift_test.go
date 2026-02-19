package swiftsource

import (
	"io"
	"testing"

	"github.com/ncw/swift"
	"github.com/ncw/swift/swifttest"
	"github.com/stretchr/testify/require"
)

const testContainer = "test-container"

func setupSwiftServer(t *testing.T) *swift.Connection {
	t.Helper()
	srv, err := swifttest.NewSwiftServer("localhost")
	require.NoError(t, err)
	t.Cleanup(func() { srv.Close() })

	conn := &swift.Connection{
		UserName: swifttest.TEST_ACCOUNT,
		ApiKey:   swifttest.TEST_ACCOUNT,
		AuthUrl:  srv.AuthURL,
	}
	require.NoError(t, conn.Authenticate())
	require.NoError(t, conn.ContainerCreate(testContainer, nil))
	return conn
}

func TestSwiftReaderRoundTrip(t *testing.T) {
	conn := setupSwiftServer(t)
	testData := []byte("hello parquet swift reader")

	// Upload test object
	err := conn.ObjectPutBytes(testContainer, "test.parquet", testData, "application/octet-stream")
	require.NoError(t, err)

	reader, err := NewSwiftFileReader(testContainer, "test.parquet", conn)
	require.NoError(t, err)
	require.NotNil(t, reader)

	// Read full content
	buf, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, testData, buf)

	// Seek back to start
	offset, err := reader.Seek(0, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)

	// Read partial
	buf2 := make([]byte, 5)
	n, err := reader.Read(buf2)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	require.Equal(t, testData[:5], buf2)

	// Close
	require.NoError(t, reader.Close())
}

func TestSwiftReaderOpen(t *testing.T) {
	conn := setupSwiftServer(t)
	err := conn.ObjectPutBytes(testContainer, "a.parquet", []byte("aaa"), "")
	require.NoError(t, err)
	err = conn.ObjectPutBytes(testContainer, "b.parquet", []byte("bbb"), "")
	require.NoError(t, err)

	reader, err := NewSwiftFileReader(testContainer, "a.parquet", conn)
	require.NoError(t, err)

	// Open another file
	reader2, err := reader.Open("b.parquet")
	require.NoError(t, err)

	buf, err := io.ReadAll(reader2)
	require.NoError(t, err)
	require.Equal(t, []byte("bbb"), buf)

	require.NoError(t, reader.Close())
	require.NoError(t, reader2.Close())
}

func TestSwiftReaderClone(t *testing.T) {
	conn := setupSwiftServer(t)
	testData := []byte("clone test data")
	err := conn.ObjectPutBytes(testContainer, "clone.parquet", testData, "")
	require.NoError(t, err)

	reader, err := NewSwiftFileReader(testContainer, "clone.parquet", conn)
	require.NoError(t, err)

	cloned, err := reader.Clone()
	require.NoError(t, err)
	require.NotNil(t, cloned)

	buf, err := io.ReadAll(cloned)
	require.NoError(t, err)
	require.Equal(t, testData, buf)

	require.NoError(t, reader.Close())
	require.NoError(t, cloned.Close())
}

func TestSwiftReaderOpenError(t *testing.T) {
	conn := setupSwiftServer(t)

	_, err := NewSwiftFileReader(testContainer, "nonexistent.parquet", conn)
	require.Error(t, err)
	require.Contains(t, err.Error(), "swift object open")
}

func TestSwiftWriterRoundTrip(t *testing.T) {
	conn := setupSwiftServer(t)
	testData := []byte("hello parquet swift writer")

	writer, err := NewSwiftFileWriter(testContainer, "output.parquet", conn)
	require.NoError(t, err)
	require.NotNil(t, writer)

	n, err := writer.Write(testData)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)

	require.NoError(t, writer.Close())

	// Verify data was written
	data, err := conn.ObjectGetBytes(testContainer, "output.parquet")
	require.NoError(t, err)
	require.Equal(t, testData, data)
}

func TestSwiftWriterCreate(t *testing.T) {
	conn := setupSwiftServer(t)

	writer, err := NewSwiftFileWriter(testContainer, "first.parquet", conn)
	require.NoError(t, err)

	// Create another file through the writer
	writer2, err := writer.Create("second.parquet")
	require.NoError(t, err)

	_, err = writer2.Write([]byte("second"))
	require.NoError(t, err)
	require.NoError(t, writer2.Close())

	data, err := conn.ObjectGetBytes(testContainer, "second.parquet")
	require.NoError(t, err)
	require.Equal(t, []byte("second"), data)

	require.NoError(t, writer.Close())
}

func TestSwiftReaderCloseError(t *testing.T) {
	conn := setupSwiftServer(t)
	err := conn.ObjectPutBytes(testContainer, "close.parquet", []byte("data"), "")
	require.NoError(t, err)

	reader, err := NewSwiftFileReader(testContainer, "close.parquet", conn)
	require.NoError(t, err)

	// Close twice — second close on the underlying swift reader returns an error
	require.NoError(t, reader.Close())
}
