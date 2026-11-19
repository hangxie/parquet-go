package hdfs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/writer"
)

// mockFileReader implements hdfsFileReaderIface for testing.
type mockFileReader struct {
	data     []byte
	cursor   bytes.Reader
	seeks    int
	seekErr  error
	closeErr error
	closed   int
}

func (m *mockFileReader) Read(p []byte) (int, error) { return m.cursor.Read(p) }
func (m *mockFileReader) Seek(off int64, whence int) (int64, error) {
	m.seeks++
	if m.seekErr != nil {
		return 0, m.seekErr
	}
	return m.cursor.Seek(off, whence)
}

type mockFileInfo struct {
	dir bool
	os.FileInfo
	size int64
}

func (i mockFileInfo) IsDir() bool { return i.dir }

func (i mockFileInfo) Size() int64 { return i.size }

func (m *mockFileReader) Stat() os.FileInfo { return mockFileInfo{size: int64(len(m.data))} }

func (m *mockFileReader) Close() error {
	m.closed++
	return m.closeErr
}

// mockFileWriter implements hdfsFileWriterIface for testing.
type mockFileWriter struct {
	written  []byte
	writeErr error
	closeErr error
}

func (m *mockFileWriter) Write(b []byte) (int, error) {
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	m.written = append(m.written, b...)
	return len(b), nil
}

func (m *mockFileWriter) Close() error { return m.closeErr }

// mockHdfsClient implements hdfsClientIface for testing.
type mockHdfsClient struct {
	mu           sync.Mutex
	opens        int
	statFn       func(string) (os.FileInfo, error)
	openFn       func(string) (hdfsFileReaderIface, error)
	openResult   hdfsFileReaderIface
	openErr      error
	createResult hdfsFileWriterIface
	createErr    error
	closeErr     error
	closed       int
}

func (m *mockHdfsClient) Stat(path string) (os.FileInfo, error) {
	if m.statFn != nil {
		return m.statFn(path)
	}
	if m.openErr != nil {
		return nil, m.openErr
	}
	return m.openResult.(*mockFileReader).Stat(), nil
}

func (m *mockHdfsClient) Open(path string) (hdfsFileReaderIface, error) {
	m.mu.Lock()
	m.opens++
	m.mu.Unlock()
	if m.openFn != nil {
		return m.openFn(path)
	}
	if f, ok := m.openResult.(*mockFileReader); ok {
		f.cursor.Reset(f.data)
	}
	return m.openResult, m.openErr
}

func (m *mockHdfsClient) Create(_ string) (hdfsFileWriterIface, error) {
	return m.createResult, m.createErr
}

func (m *mockHdfsClient) Close() error {
	m.closed++
	return m.closeErr
}

func TestHdfsFileInterfaceCompliance(t *testing.T) {
	var _ source.ParquetFileReader = (*hdfsReader)(nil)
	var _ source.ParquetFileWriter = (*hdfsWriter)(nil)
}

func TestHdfsFileStructure(t *testing.T) {
	file := hdfsFile{
		hosts:    []string{"localhost:9000"},
		user:     "test-user",
		filePath: "test.parquet",
		client:   nil,
	}

	require.Equal(t, []string{"localhost:9000"}, file.hosts)
	require.Equal(t, "test-user", file.user)
	require.Equal(t, "test.parquet", file.filePath)
}

func TestHdfsWideCloneLifetime(t *testing.T) {
	file := &mockFileReader{data: []byte("parquet")}
	client := &mockHdfsClient{openResult: file}
	client.openFn = func(string) (hdfsFileReaderIface, error) { return bytesHandle(file.data), nil }
	root, err := newReaderPool(client, defaultStreamLimit).open("main")
	require.NoError(t, err)
	clones := make([]source.ParquetFileReader, 1024)
	for i := range clones {
		clone, err := root.Clone()
		require.NoError(t, err)
		clones[i] = clone
	}
	require.Zero(t, client.opens, "metadata and cloning should not open data streams")
	require.NoError(t, root.Close())
	require.Zero(t, file.closed)
	for i, clone := range clones {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			offset := int64(i % 7)
			_, err := clone.Seek(offset, io.SeekStart)
			require.NoError(t, err)
			var buf [1]byte
			_, err = clone.Read(buf[:])
			require.NoError(t, err)
			require.Equal(t, "parquet"[offset], buf[0])
			require.NoError(t, clone.Close())
			require.NoError(t, clone.Close())
		})
	}
	t.Cleanup(func() {
		require.Equal(t, len(clones), client.opens)
		require.Equal(t, 1, client.closed)
	})
}

func TestHdfsCursor(t *testing.T) {
	client := &mockHdfsClient{openResult: &mockFileReader{data: []byte("abc")}}
	root, err := newReaderPool(client, defaultStreamLimit).open("root")
	require.NoError(t, err)
	_, err = root.Seek(1, io.SeekStart)
	require.NoError(t, err)
	clone, err := root.Clone()
	require.NoError(t, err)
	for _, tc := range []struct {
		off     int64
		whence  int
		want    int64
		invalid bool
	}{
		{0, io.SeekCurrent, 0, false},
		{-1, io.SeekEnd, 2, false},
		{-1, io.SeekCurrent, 1, false},
		{-2, io.SeekCurrent, 1, true},
		{4, io.SeekStart, 1, true},
		{0, 42, 1, true},
	} {
		pos, err := clone.Seek(tc.off, tc.whence)
		require.Equal(t, tc.want, pos)
		require.Equal(t, tc.invalid, err != nil)
	}
	n, err := clone.Read(nil)
	require.NoError(t, err)
	require.Zero(t, n)
	buf := make([]byte, 3)
	n, err = clone.Read(buf)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, "bc", string(buf[:n]))
	pos, err := clone.Seek(0, io.SeekCurrent)
	require.NoError(t, err)
	require.EqualValues(t, 3, pos)
	require.NoError(t, clone.Close())
	_, err = clone.Read(buf)
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = clone.Seek(0, io.SeekStart)
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = clone.Clone()
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = clone.Open("other")
	require.ErrorIs(t, err, os.ErrClosed)
	require.NoError(t, root.Close())
}

func TestHdfsOpenLifetime(t *testing.T) {
	first := &mockFileReader{}
	client := &mockHdfsClient{openResult: first}
	root, err := newReaderPool(client, defaultStreamLimit).open("first")
	require.NoError(t, err)
	second := &mockFileReader{data: []byte("other")}
	client.openResult = second
	other, err := root.Open("second")
	require.NoError(t, err)
	require.NoError(t, root.Close())
	require.Zero(t, first.closed)
	require.Zero(t, client.closed)
	data, err := io.ReadAll(other)
	require.NoError(t, err)
	require.Equal(t, "other", string(data))
	require.NoError(t, other.Close())
	require.Equal(t, 1, second.closed)
	require.Equal(t, 1, client.closed)
}

func TestHdfsReaderErrors(t *testing.T) {
	openErr := errors.New("open failed")
	client := &mockHdfsClient{openErr: openErr}
	_, err := newReaderPool(client, defaultStreamLimit).open("missing")
	require.ErrorIs(t, err, openErr)
	require.Equal(t, 1, client.closed)
	fileErr, clientErr := errors.New("file close"), errors.New("client close")
	client = &mockHdfsClient{openResult: &mockFileReader{data: []byte("abc"), closeErr: fileErr}, closeErr: clientErr}
	r, err := newReaderPool(client, defaultStreamLimit).open("file")
	require.NoError(t, err)
	_, err = r.Read(make([]byte, 1))
	require.NoError(t, err)
	client.openErr = openErr
	_, err = r.Open("missing")
	require.ErrorIs(t, err, openErr)
	require.Zero(t, client.closed)
	err = r.Close()
	require.ErrorIs(t, err, fileErr)
	require.ErrorIs(t, err, clientErr)
}

func TestHdfsWideParquetReader(t *testing.T) {
	ctx := t.Context()
	var data bytes.Buffer
	metadata := make([]string, 200)
	row := make([]any, len(metadata))
	for i := range metadata {
		metadata[i] = fmt.Sprintf("name=c%d, type=INT64", i)
		row[i] = int64(i)
	}
	w, err := writer.NewCSVWriterFromWriterWithContext(ctx, metadata, &data)
	require.NoError(t, err)
	require.NoError(t, w.WriteWithContext(ctx, row))
	require.NoError(t, w.WriteStopWithContext(ctx))
	file := &mockFileReader{data: data.Bytes()}
	client := &mockHdfsClient{openResult: file, openFn: func(string) (hdfsFileReaderIface, error) { return bytesHandle(file.data), nil }}
	root, err := newReaderPool(client, defaultStreamLimit).open("wide.parquet")
	require.NoError(t, err)
	columns, err := reader.NewParquetColumnReaderWithContext(ctx, root)
	require.NoError(t, err)
	metadataOpens := client.opens
	require.LessOrEqual(t, metadataOpens, 2)
	for _, index := range []int64{0, 199} {
		values, _, _, err := columns.ReadColumnByIndexWithContext(ctx, index, 1)
		require.NoError(t, err)
		require.Equal(t, []any{index}, values)
	}
	require.Equal(t, metadataOpens+2, client.opens)
	require.NoError(t, columns.ReadStopWithContext(ctx))
	require.Zero(t, file.closed)
	require.NoError(t, root.Close())
	require.Equal(t, 1, client.closed)
}

func bytesHandle(data []byte) *mockFileReader {
	f := &mockFileReader{data: data}
	f.cursor.Reset(data)
	return f
}

func TestHdfsReadDirectory(t *testing.T) {
	client := &mockHdfsClient{statFn: func(string) (os.FileInfo, error) { return mockFileInfo{dir: true}, nil }}
	r, err := newReaderPool(client, 1).open("dir")
	require.NoError(t, err)
	_, err = r.Read(make([]byte, 1))
	require.ErrorContains(t, err, "is a directory")
	require.Zero(t, client.opens)
	require.NoError(t, r.Close())
}

type latencyHandle struct{ hdfsFileReaderIface }

func (h latencyHandle) Read(p []byte) (int, error) {
	time.Sleep(time.Millisecond)
	return h.hdfsFileReaderIface.Read(p)
}

// BenchmarkHdfsColumnReads measures Parquet row reads with 1 ms of latency per backend read.
func BenchmarkHdfsColumnReads(b *testing.B) {
	ctx := b.Context()
	var buf bytes.Buffer
	metadata := make([]string, 32)
	row := make([]any, len(metadata))
	for i := range metadata {
		metadata[i] = fmt.Sprintf("name=c%d, type=INT64, encoding=PLAIN", i)
	}
	w, err := writer.NewCSVWriterFromWriterWithContext(ctx, metadata, &buf)
	require.NoError(b, err)
	for i := range 131072 {
		for j := range row {
			row[j] = int64(uint64(i*31+j) * 11400714819323198485)
		}
		require.NoError(b, w.WriteWithContext(ctx, row))
	}
	require.NoError(b, w.WriteStopWithContext(ctx))
	for _, np := range []int64{4, 16} {
		b.Run(fmt.Sprintf("NP%d", np), func(b *testing.B) {
			client := &mockHdfsClient{openResult: bytesHandle(buf.Bytes()), openFn: func(string) (hdfsFileReaderIface, error) {
				return latencyHandle{bytesHandle(buf.Bytes())}, nil
			}}
			for b.Loop() {
				root, err := newReaderPool(client, defaultStreamLimit).open("bench")
				require.NoError(b, err)
				r, err := reader.NewParquetReaderWithContext(ctx, root, nil, reader.WithNP(np))
				require.NoError(b, err)
				for range 128 {
					rows, err := r.ReadByNumberWithContext(ctx, 1024)
					require.NoError(b, err)
					require.Len(b, rows, 1024)
				}
				require.NoError(b, r.ReadStopWithContext(ctx))
				require.NoError(b, root.Close())
			}
		})
	}
}
