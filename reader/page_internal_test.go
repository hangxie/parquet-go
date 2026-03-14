package reader

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/source/buffer"
)

func TestReadAllPageHeaders_NilMetadata(t *testing.T) {
	// Test readAllPageHeaders with nil metadata returns error
	buf := buffer.NewBufferReaderFromBytesNoAlloc(bytes.Repeat([]byte{0}, 100))

	// Create a column chunk with nil metadata
	cc := &parquet.ColumnChunk{
		MetaData: nil,
	}

	_, err := readAllPageHeaders(buf, cc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata is nil")
}
