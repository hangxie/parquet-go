package s3v2

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetBytesRange_KnownFileSize(t *testing.T) {
	reader := &s3Reader{
		offset:   10,
		fileSize: 1000,
		whence:   io.SeekStart,
	}

	rangeStr := reader.getBytesRange(100)
	expected := "bytes=10-109"
	require.Equal(t, expected, rangeStr)

	// Test range beyond file size
	reader.offset = 950
	rangeStr = reader.getBytesRange(100)
	expected = "bytes=950-999" // Should clamp to file size
	require.Equal(t, expected, rangeStr)
}

func TestGetBytesRange_NegativeBegin(t *testing.T) {
	reader := &s3Reader{
		offset:   -10,
		fileSize: 1000,
		whence:   io.SeekEnd,
	}

	rangeStr := reader.getBytesRange(100)
	expected := "bytes=990-999"
	require.Equal(t, expected, rangeStr)
}

func TestGetBytesRange_SeekEnd(t *testing.T) {
	reader := &s3Reader{
		offset:   -50,
		fileSize: 1000,
		whence:   io.SeekEnd,
	}

	rangeStr := reader.getBytesRange(100)
	expected := "bytes=950-999" // fileSize + offset = 1000 + (-50) = 950
	require.Equal(t, expected, rangeStr)
}

func TestGetBytesRange_UnknownFileSize(t *testing.T) {
	reader := &s3Reader{
		offset:   10,
		fileSize: 0, // Unknown file size
		whence:   io.SeekStart,
	}

	rangeStr := reader.getBytesRange(100)
	expected := "bytes=10-109"
	require.Equal(t, expected, rangeStr)

	// Test SeekEnd with unknown file size
	reader.whence = io.SeekEnd
	reader.offset = -50
	rangeStr = reader.getBytesRange(100)
	expected = "bytes=-50"
	require.Equal(t, expected, rangeStr)
}

func TestGetBytesRange_InvalidWhence(t *testing.T) {
	reader := &s3Reader{
		offset:   10,
		fileSize: 1000,
		whence:   99,
	}

	require.Empty(t, reader.getBytesRange(100))
}

// errReader is a reader that always returns an error.
type errReader struct{ err error }

func (e *errReader) Read([]byte) (int, error) { return 0, e.err }

// eofReader returns data and io.EOF together on the final read,
// simulating a chunked socket that signals end-of-stream.
type eofReader struct {
	data []byte
	pos  int
}

func (r *eofReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	if r.pos >= len(r.data) {
		return n, io.EOF
	}
	return n, nil
}
