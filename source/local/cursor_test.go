package local

import (
	"bytes"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestFile(t *testing.T, data []byte) *os.File {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	file, err := os.Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = file.Close() })
	return file
}

func TestCursor(t *testing.T) {
	file := newTestFile(t, []byte("abcdef"))
	original := newFileCursor(file)
	clone, err := original.Clone()
	require.NoError(t, err)
	_, err = original.Seek(3, io.SeekStart)
	require.NoError(t, err)
	data := make([]byte, 3)
	_, err = clone.Read(data)
	require.NoError(t, err)
	require.Equal(t, "abc", string(data))
	_, err = original.Read(data)
	require.NoError(t, err)
	require.Equal(t, "def", string(data))
	pos, err := clone.Seek(-1, io.SeekCurrent)
	require.NoError(t, err)
	require.EqualValues(t, 2, pos)
	pos, err = clone.Seek(-1, io.SeekEnd)
	require.NoError(t, err)
	require.EqualValues(t, 5, pos)
	require.NoError(t, original.Close())
	_, err = original.Clone()
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = file.Stat()
	require.NoError(t, err)
	_, err = io.ReadFull(clone, data)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.Equal(t, byte('f'), data[0])
	require.NoError(t, clone.Close())
	_, err = file.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
	require.NoError(t, clone.Close())
	_, err = clone.Read(data)
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = clone.Seek(0, io.SeekStart)
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = clone.Clone()
	require.ErrorIs(t, err, os.ErrClosed)
}

func TestCursorSeek(t *testing.T) {
	tests := []struct {
		name    string
		offset  int64
		whence  int
		want    int64
		wantErr string
	}{
		{name: "start", offset: 1, whence: io.SeekStart, want: 1},
		{name: "current", offset: 1, whence: io.SeekCurrent, want: 3},
		{name: "end", offset: -1, whence: io.SeekEnd, want: 5},
		{name: "at-end", whence: io.SeekEnd, want: 6},
		{name: "past-end", offset: 4, whence: io.SeekEnd, want: 10},
		{name: "negative-start", offset: -1, whence: io.SeekStart, wantErr: "negative"},
		{name: "negative-current", offset: -3, whence: io.SeekCurrent, wantErr: "negative"},
		{name: "negative-end", offset: -7, whence: io.SeekEnd, wantErr: "negative"},
		{name: "invalid-whence", whence: -1, wantErr: "whence"},
		{name: "current-overflow", offset: math.MaxInt64, whence: io.SeekCurrent, wantErr: "overflows"},
		{name: "end-overflow", offset: math.MaxInt64, whence: io.SeekEnd, wantErr: "overflows"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			file := newTestFile(t, []byte("abcdef"))
			c := newFileCursor(file)
			defer func() { require.NoError(t, c.Close()) }()
			_, err := c.Seek(2, io.SeekStart)
			require.NoError(t, err)
			pos, err := c.Seek(tt.offset, tt.whence)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				data := make([]byte, 1)
				_, err = c.Read(data)
				require.NoError(t, err)
				require.Equal(t, "c", string(data), "failed seek must preserve the position")
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.want, pos)
				if tt.want >= 6 {
					_, err = c.Read(make([]byte, 1))
					require.ErrorIs(t, err, io.EOF)
				}
			}
			offset, err := file.Seek(0, io.SeekCurrent)
			require.NoError(t, err)
			require.Zero(t, offset, "cursor operations must leave the OS file offset unchanged")
		})
	}
}

func TestCursorFileErrors(t *testing.T) {
	file := newTestFile(t, nil)
	c := newFileCursor(file)
	require.NoError(t, file.Close())
	_, err := c.Seek(0, io.SeekEnd)
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = c.Read(make([]byte, 1))
	require.ErrorIs(t, err, os.ErrClosed)
	require.ErrorIs(t, c.Close(), os.ErrClosed)
}

func TestCursorConcurrentReads(t *testing.T) {
	data := bytes.Repeat([]byte("abcdef"), 100)
	file := newTestFile(t, data)
	root := newFileCursor(file)
	var wg sync.WaitGroup
	for i := range 20 {
		c, err := root.Clone()
		require.NoError(t, err)
		wg.Go(func() {
			defer func() { assert.NoError(t, c.Close()) }()
			_, err := c.Seek(int64(i), io.SeekStart)
			assert.NoError(t, err)
			buf := make([]byte, 6)
			for range 50 {
				_, err := io.ReadFull(c, buf)
				assert.NoError(t, err)
				assert.Equal(t, data[i%6:i%6+6], buf)
			}
		})
	}
	wg.Wait()
	require.NoError(t, root.Close())
	_, err := file.Stat()
	require.ErrorIs(t, err, os.ErrClosed)
}

func TestCursorConcurrentCloneClose(t *testing.T) {
	for range 100 {
		file := newTestFile(t, []byte("data"))
		root := newFileCursor(file)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			<-start
			clone, err := root.Clone()
			if err != nil {
				assert.ErrorIs(t, err, os.ErrClosed)
				return
			}
			defer func() { assert.NoError(t, clone.Close()) }()
			data, err := io.ReadAll(clone)
			assert.NoError(t, err)
			assert.Equal(t, "data", string(data))
		})
		wg.Go(func() { <-start; assert.NoError(t, root.Close()) })
		close(start)
		wg.Wait()
		_, err := file.Stat()
		require.ErrorIs(t, err, os.ErrClosed)
	}
}
