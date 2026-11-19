package hdfs

import (
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/source"
)

func TestPoolRetainsSequentialStreams(t *testing.T) {
	data := []byte("0123456789")
	var opened []*mockFileReader
	client := &mockHdfsClient{openResult: bytesHandle(data), openFn: func(string) (hdfsFileReaderIface, error) {
		f := bytesHandle(data)
		opened = append(opened, f)
		return f, nil
	}}
	a, err := newReaderPool(client, 2).open("path")
	require.NoError(t, err)
	b, err := a.Clone()
	require.NoError(t, err)
	c, err := a.Clone()
	require.NoError(t, err)
	read := func(r source.ParquetFileReader, want byte) {
		t.Helper()
		var buf [1]byte
		_, err := r.Read(buf[:])
		require.NoError(t, err)
		require.Equal(t, want, buf[0])
	}
	read(a, '0')
	read(b, '0')
	read(a, '1') // B is now the least recently used.
	read(c, '0')
	require.Len(t, opened, 3)
	require.Equal(t, 1, opened[1].closed)
	require.Zero(t, opened[0].seeks)
	read(b, '1') // reopen B and resume at its saved offset
	read(b, '2')
	require.Len(t, opened, 4)
	require.Equal(t, 1, opened[3].seeks)
	for _, r := range []source.ParquetFileReader{a, b, c} {
		require.NoError(t, r.Close())
	}
	for _, f := range opened {
		require.Equal(t, 1, f.closed)
	}
}

type gatedHandle struct {
	*mockFileReader
	started chan<- struct{}
	proceed <-chan struct{}
}

func (h gatedHandle) Read(p []byte) (int, error) {
	h.started <- struct{}{}
	<-h.proceed
	return h.mockFileReader.Read(p)
}

func TestPoolParallelReads(t *testing.T) {
	started := make(chan struct{}, 3)
	proceed := make(chan struct{}, 3)
	client := &mockHdfsClient{openResult: bytesHandle([]byte("ab")), openFn: func(string) (hdfsFileReaderIface, error) {
		return gatedHandle{bytesHandle([]byte("ab")), started, proceed}, nil
	}}
	root, err := newReaderPool(client, 2).open("file")
	require.NoError(t, err)
	done := make(chan error, 3)
	for range 3 {
		clone, err := root.Clone()
		require.NoError(t, err)
		go func() { _, err := clone.Read(make([]byte, 1)); done <- errors.Join(err, clone.Close()) }()
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("two reads could not overlap")
		}
	}
	select {
	case <-started:
		t.Fatal("exceeded two open streams")
	case <-time.After(20 * time.Millisecond):
	}
	for range 3 {
		proceed <- struct{}{}
	}
	for range 3 {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("reader waiting for slot")
		}
	}
	require.NoError(t, root.Close())
}

func TestConcurrentOpenClose(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "stat failure"
		}
		t.Run(name, func(t *testing.T) {
			client := &mockHdfsClient{openResult: bytesHandle([]byte("hello"))}
			root, err := newReaderPool(client, 2).open("first")
			require.NoError(t, err)
			derived, err := root.Clone()
			require.NoError(t, err)
			entered, proceed := make(chan struct{}), make(chan struct{})
			client.statFn = func(string) (os.FileInfo, error) {
				close(entered)
				<-proceed
				if fail {
					return nil, os.ErrNotExist
				}
				return mockFileInfo{size: 5}, nil
			}
			done := make(chan error, 1)
			go func() {
				r, err := derived.Open("other")
				if err == nil {
					_, err = io.ReadAll(r)
					err = errors.Join(err, r.Close())
				}
				done <- err
			}()
			<-entered
			closed := make(chan error, 1)
			go func() { closed <- errors.Join(root.Close(), derived.Close()) }()
			select {
			case err := <-closed:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				close(proceed)
				t.Fatal("Close blocked behind metadata RPC")
			}
			require.Zero(t, client.closed)
			close(proceed)
			err = <-done
			if fail {
				require.ErrorIs(t, err, os.ErrNotExist)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, client.closed)
		})
	}
}

func TestPoolSlotRecovery(t *testing.T) {
	_, err := NewHdfsFileReaderWithStreamLimit(nil, "", "", 0)
	require.Error(t, err)
	data := []byte("abc")
	client := &mockHdfsClient{openResult: bytesHandle(data)}
	root, err := newReaderPool(client, 1).open("file")
	require.NoError(t, err)
	clone, err := root.Clone()
	require.NoError(t, err)
	injected := errors.New("injected")
	victim := client.openResult.(*mockFileReader)
	victim.closeErr = injected
	_, err = root.Read(make([]byte, 1))
	require.NoError(t, err)
	client.openResult = bytesHandle(data)
	buf := make([]byte, len(data))
	_, err = io.ReadFull(clone, buf)
	require.NoError(t, err)
	require.Equal(t, data, buf)
	require.Equal(t, 1, victim.closed)
	_, err = clone.Seek(0, io.SeekStart)
	require.NoError(t, err)
	client.openFn = func(string) (hdfsFileReaderIface, error) { return nil, injected }
	_, err = clone.Read(make([]byte, 1))
	require.ErrorIs(t, err, injected)
	client.openFn = nil
	f := bytesHandle(data)
	f.seekErr = injected
	client.openResult = f
	_, err = clone.Seek(1, io.SeekStart)
	require.NoError(t, err)
	_, err = clone.Read(make([]byte, 1))
	require.ErrorIs(t, err, injected)
	f.seekErr = nil
	f.closeErr = injected
	_, err = clone.Read(make([]byte, 2))
	require.ErrorIs(t, err, injected)
	require.NoError(t, root.Close())
	require.NoError(t, clone.Close())
}
