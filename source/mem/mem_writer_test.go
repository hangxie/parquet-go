package mem

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/source"
)

// Compile-time check: *memWriter satisfies source.ParquetFileWriter.
var _ source.ParquetFileWriter = (*memWriter)(nil)

func TestMemWriter_InterfaceCompliance(t *testing.T) {
	w, err := NewMemFileWriter("test.parquet", nil)
	require.NoError(t, err)
	require.NotNil(t, w)
}

func TestMemWriter_WriteAndRead(t *testing.T) {
	var received []byte
	w, err := NewMemFileWriter("test.parquet", func(_ string, r io.Reader) error {
		buf := make([]byte, 1024)
		n, _ := r.Read(buf)
		received = buf[:n]
		return nil
	})
	require.NoError(t, err)

	payload := []byte("hello parquet")
	n, writeErr := w.Write(payload)
	require.NoError(t, writeErr)
	require.Equal(t, len(payload), n)

	require.NoError(t, w.Close())
	require.Equal(t, payload, received)
}
