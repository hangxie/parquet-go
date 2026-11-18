package swiftsource

import (
	"io"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSwiftCursorSeek(t *testing.T) {
	tests := []struct {
		name     string
		offset   int64
		whence   int
		position int64
		wantErr  string
	}{
		{name: "start", offset: 1, whence: io.SeekStart, position: 1},
		{name: "current", offset: 1, whence: io.SeekCurrent, position: 3},
		{name: "end", offset: -1, whence: io.SeekEnd, position: 5},
		{name: "at-end", whence: io.SeekEnd, position: 6},
		{name: "past-end", offset: 1, whence: io.SeekEnd, position: 7},
		{name: "negative", offset: -1, whence: io.SeekStart, wantErr: "negative"},
		{name: "bad-whence", whence: -1, wantErr: "whence"},
		{name: "overflow", offset: math.MaxInt64, whence: io.SeekCurrent, wantErr: "overflows"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := &requestTracker{}
			conn := setupSwiftServer(t, tracker)
			require.NoError(t, conn.ObjectPutBytes(testContainer, "seek", []byte("abcdef"), ""))
			root, err := NewSwiftFileReader(testContainer, "seek", conn)
			require.NoError(t, err)
			defer func() { require.NoError(t, root.Close()) }()
			_, err = root.Seek(2, io.SeekStart)
			require.NoError(t, err)
			pos, err := root.Seek(tt.offset, tt.whence)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				pos, err = root.Seek(0, io.SeekCurrent)
				require.NoError(t, err)
				require.EqualValues(t, 2, pos)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.position, pos)
			}
			gets, _, _ := tracker.counts()
			require.Zero(t, gets, "Seek must not issue requests")
			if tt.wantErr == "" && pos >= 6 {
				_, err = root.Read(make([]byte, 1))
				require.ErrorIs(t, err, io.EOF)
				after, _, _ := tracker.counts()
				require.Equal(t, gets, after, "reads at EOF must not issue a range GET")
			} else {
				data := make([]byte, 1)
				_, err = io.ReadFull(root, data)
				require.NoError(t, err)
				require.Equal(t, byte("abcdef"[pos]), data[0])
			}
		})
	}
}

func TestSwiftCursorClosed(t *testing.T) {
	conn := setupSwiftServer(t)
	require.NoError(t, conn.ObjectPutBytes(testContainer, "closed", []byte("data"), ""))
	root, err := NewSwiftFileReader(testContainer, "closed", conn)
	require.NoError(t, err)
	require.NoError(t, root.Close())
	require.NoError(t, root.Close())
	_, err = root.Read(nil)
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = root.Seek(0, io.SeekStart)
	require.ErrorIs(t, err, os.ErrClosed)
	_, err = root.Clone()
	require.ErrorIs(t, err, os.ErrClosed)
}

func TestSwiftCursorEmptyRead(t *testing.T) {
	conn := setupSwiftServer(t)
	require.NoError(t, conn.ObjectPutBytes(testContainer, "empty-read", []byte("data"), ""))
	root, err := NewSwiftFileReader(testContainer, "empty-read", conn)
	require.NoError(t, err)
	n, err := root.Read(nil)
	require.NoError(t, err)
	require.Zero(t, n)
	require.NoError(t, root.Close())
}
