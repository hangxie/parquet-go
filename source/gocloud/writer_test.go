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
