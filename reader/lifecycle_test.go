package reader

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/source"
)

type closeErrorReader struct{}

func (closeErrorReader) Read([]byte) (int, error) { return 0, io.EOF }

func (closeErrorReader) Seek(int64, int) (int64, error) { return 0, nil }

func (closeErrorReader) Close() error { return io.ErrClosedPipe }

func (r closeErrorReader) Open(string) (source.ParquetFileReader, error) { return r, nil }

func (r closeErrorReader) Clone() (source.ParquetFileReader, error) { return r, nil }

type closeTrackingFile struct {
	closeErrorReader
	closed bool
}

func (f *closeTrackingFile) Close() error {
	f.closed = true
	return nil
}

func TestLifecycleWithContextCanceledClosesBuffers(t *testing.T) {
	tests := []struct {
		name string
		run  func(*ParquetReader, context.Context) error
	}{
		{name: "reset", run: func(pr *ParquetReader, ctx context.Context) error { return pr.ResetWithContext(ctx) }},
		{name: "stop", run: func(pr *ParquetReader, ctx context.Context) error { return pr.ReadStopWithContext(ctx) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			files := []*closeTrackingFile{{}, {}}
			pr := &ParquetReader{ColumnBuffers: map[string]*ColumnBufferType{
				"first":  {PFile: files[0]},
				"second": {PFile: files[1]},
			}}
			require.ErrorIs(t, tt.run(pr, ctx), context.Canceled)
			for _, file := range files {
				require.True(t, file.closed)
			}
		})
	}
}

func TestLifecycleWithContextCloseErrors(t *testing.T) {
	tests := []struct {
		name string
		run  func(*ParquetReader) error
	}{
		{name: "reset", run: func(pr *ParquetReader) error { return pr.ResetWithContext(context.Background()) }},
		{name: "stop", run: func(pr *ParquetReader) error { return pr.ReadStopWithContext(context.Background()) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := &ParquetReader{ColumnBuffers: map[string]*ColumnBufferType{
				"column": {PFile: closeErrorReader{}},
			}}
			err := tt.run(pr)
			require.Error(t, err)
			require.True(t, errors.Is(err, io.ErrClosedPipe))
		})
	}
}
