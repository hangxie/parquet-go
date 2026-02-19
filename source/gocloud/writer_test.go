package gocloud

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"gocloud.dev/blob/memblob"
)

func TestBlobWriter_Write(t *testing.T) {
	b := memblob.OpenBucket(nil)
	defer func() {
		_ = b.Close()
	}()

	ctx := context.Background()
	key := "test"
	testData := []byte("test data")

	bf, err := NewBlobWriter(ctx, b, key)
	require.NoError(t, err)

	n, err := bf.Write(testData)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)

	// All data is not guaranteed to exist prior to calling Close()
	err = bf.Close()
	require.NoError(t, err)

	data, err := b.ReadAll(ctx, key)
	require.NoError(t, err)
	require.Equal(t, testData, data)

	// Opening an existing blob and writing to it replaces the contents
	bf, err = NewBlobWriter(ctx, b, key)
	require.NoError(t, err)
	testOverwrite := []byte("overwritten")
	n, err = bf.Write(testOverwrite)
	require.NoError(t, err)
	require.Equal(t, len(testOverwrite), n)

	// All data is not guaranteed to exist prior to calling Close()
	err = bf.Close()
	require.NoError(t, err)

	data, err = b.ReadAll(ctx, key)
	require.NoError(t, err)
	require.Equal(t, testOverwrite, data)

	// Don't write to things that don't exist
	bf = &blobWriter{}
	n, err = bf.Write(testData)
	require.Error(t, err)
	require.Equal(t, 0, n)
}

func TestBlobWriter_CloseNilWriter(t *testing.T) {
	// Close on a writer with no underlying writer should succeed
	w := &blobWriter{}
	require.NoError(t, w.Close())
}

func TestBlobWriter_CreateEmptyName(t *testing.T) {
	b := memblob.OpenBucket(nil)
	defer func() {
		_ = b.Close()
	}()

	ctx := context.Background()
	w := &blobWriter{
		blobFile: blobFile{
			ctx:    ctx,
			bucket: b,
		},
	}

	_, err := w.Create("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "file name empty")
}

func TestBlobWriter_WriteLazyInit(t *testing.T) {
	b := memblob.OpenBucket(nil)
	defer func() {
		_ = b.Close()
	}()

	ctx := context.Background()
	testData := []byte("lazy init write")

	// Create a writer with a key but nil writer to trigger lazy initialization
	w := &blobWriter{
		blobFile: blobFile{
			ctx:    ctx,
			bucket: b,
			key:    "lazy-test",
		},
	}

	n, err := w.Write(testData)
	require.NoError(t, err)
	require.Equal(t, len(testData), n)

	require.NoError(t, w.Close())

	data, err := b.ReadAll(ctx, "lazy-test")
	require.NoError(t, err)
	require.Equal(t, testData, data)
}
