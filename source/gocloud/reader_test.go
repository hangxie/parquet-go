package gocloud

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
)

func TestBlobReader_Read(t *testing.T) {
	b := memblob.OpenBucket(nil)
	defer func() {
		_ = b.Close()
	}()

	ctx := context.Background()
	key := "test"
	testData := []byte("test data")
	err := b.WriteAll(ctx, key, testData, nil)
	require.NoError(t, err)

	bf, err := NewBlobReader(ctx, b, key)
	require.NoError(t, err)

	buf := make([]byte, 1)
	n, err := bf.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, testData[:n], buf[:n])

	buf = make([]byte, 7)
	n, err = bf.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(buf), n)
	require.Equal(t, testData[1:8], buf[:])

	buf = make([]byte, 7)
	n, err = bf.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, testData[8:], buf[:n])

	buf = make([]byte, 1)
	n, err = bf.Read(buf)
	require.Equal(t, io.EOF, err)
	require.Equal(t, n, 0)

	// Ensure Read operates as expected if we seek
	_, _ = bf.Seek(-1, io.SeekEnd)
	n, err = bf.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, testData[8:], buf[:n])

	n, err = bf.Read(buf)
	require.Equal(t, io.EOF, err)
	require.Equal(t, n, 0)
}

func TestBlobReader_Close(t *testing.T) {
	b := memblob.OpenBucket(nil)
	defer func() {
		_ = b.Close()
	}()

	ctx := context.Background()
	err := b.WriteAll(ctx, "test", []byte("data"), nil)
	require.NoError(t, err)

	bf, err := NewBlobReader(ctx, b, "test")
	require.NoError(t, err)

	// Close is a no-op, should always succeed
	require.NoError(t, bf.Close())
}

func TestBlobReader_Clone(t *testing.T) {
	b := memblob.OpenBucket(nil)
	defer func() {
		_ = b.Close()
	}()

	ctx := context.Background()
	testData := []byte("clone test data")
	err := b.WriteAll(ctx, "test", testData, nil)
	require.NoError(t, err)

	bf, err := NewBlobReader(ctx, b, "test")
	require.NoError(t, err)

	// Read some data to advance offset
	buf := make([]byte, 5)
	_, err = bf.Read(buf)
	require.NoError(t, err)

	// Clone should reset offset to 0
	cloned, err := bf.Clone()
	require.NoError(t, err)
	require.NotNil(t, cloned)

	// Read from cloned reader should start from beginning
	all, err := io.ReadAll(cloned)
	require.NoError(t, err)
	require.Equal(t, testData, all)
}

func TestBlobReader_OpenNonexistent(t *testing.T) {
	b := memblob.OpenBucket(nil)
	defer func() {
		_ = b.Close()
	}()

	ctx := context.Background()
	bf := &blobReader{
		blobFile: blobFile{
			ctx:    ctx,
			bucket: b,
		},
	}

	_, err := bf.Open("nonexistent")
	require.Error(t, err)
	require.Contains(t, err.Error(), "blob does not exist")
}

func TestBlobReader_Seek(t *testing.T) {
	bf := &blobReader{}

	// Out of range whence
	_, err := bf.Seek(0, io.SeekEnd+1)
	require.Error(t, err)

	// Filesize is inconsequential for SeekStart and SeekCurrent
	_, err = bf.Seek(-1, io.SeekStart)
	require.Error(t, err)

	offset, err := bf.Seek(10, io.SeekStart)
	require.NoError(t, err)
	require.Equal(t, int64(10), offset)

	offset, err = bf.Seek(10, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(20), offset)

	offset, err = bf.Seek(-20, io.SeekCurrent)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)

	_, err = bf.Seek(-1, io.SeekCurrent)
	require.Error(t, err)

	// Ensure SeekEnd works correctly with zero sized files
	_, err = bf.Seek(-1, io.SeekEnd)
	require.Error(t, err)

	offset, err = bf.Seek(1, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(1), offset)

	// Ensure SeekEnd works correctly with non-zero file sizes
	bf.size = 1
	offset, err = bf.Seek(-1, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(0), offset)

	_, err = bf.Seek(-2, io.SeekEnd)
	require.Error(t, err)

	offset, err = bf.Seek(1, io.SeekEnd)
	require.NoError(t, err)
	require.Equal(t, int64(2), offset)
}
