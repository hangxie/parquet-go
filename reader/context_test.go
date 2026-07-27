package reader

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/writer"
)

type contextTrackingReader struct {
	source.ParquetFileReader
	contexts *[]context.Context
}

func (r *contextTrackingReader) ReadContext(ctx context.Context, p []byte) (int, error) {
	*r.contexts = append(*r.contexts, ctx)
	return r.Read(p)
}

func (r *contextTrackingReader) SeekContext(ctx context.Context, offset int64, whence int) (int64, error) {
	*r.contexts = append(*r.contexts, ctx)
	return r.Seek(offset, whence)
}

func (r *contextTrackingReader) CloneContext(ctx context.Context) (source.ParquetFileReader, error) {
	*r.contexts = append(*r.contexts, ctx)
	clone, err := r.Clone()
	if err != nil {
		return nil, err
	}
	return &contextTrackingReader{ParquetFileReader: clone, contexts: r.contexts}, nil
}

func (r *contextTrackingReader) OpenContext(ctx context.Context, name string) (source.ParquetFileReader, error) {
	*r.contexts = append(*r.contexts, ctx)
	opened, err := r.Open(name)
	if err != nil {
		return nil, err
	}
	return &contextTrackingReader{ParquetFileReader: opened, contexts: r.contexts}, nil
}

func TestParquetReaderWithContext(t *testing.T) {
	file := buffer.NewBufferWriter()
	//nolint:staticcheck
	pw, err := writer.NewParquetWriter(file, new(Record), writer.WithNP(1))
	require.NoError(t, err)
	//nolint:staticcheck
	require.NoError(t, pw.Write(Record{Str1: "context"}))
	//nolint:staticcheck
	require.NoError(t, pw.Write(Record{Str1: "context"}))
	//nolint:staticcheck
	require.NoError(t, pw.WriteStop())

	var contexts []context.Context
	tracking := &contextTrackingReader{ParquetFileReader: buffer.NewBufferReaderFromBytes(file.Bytes()), contexts: &contexts}
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "constructor")
	pr, err := NewParquetReaderWithContext(ctx, tracking, new(Record), WithNP(1))
	require.NoError(t, err)
	require.NotEmpty(t, contexts)
	for _, got := range contexts {
		require.Equal(t, "constructor", got.Value(contextKey{}))
	}

	contexts = nil
	rows := make([]Record, 1)
	require.NoError(t, pr.Read(&rows))
	require.NotEmpty(t, contexts)
	for _, got := range contexts {
		require.Equal(t, "constructor", got.Value(contextKey{}))
	}

	contexts = nil
	readCtx := context.WithValue(context.Background(), contextKey{}, "read")
	require.NoError(t, pr.ReadWithContext(readCtx, &rows))
	require.Equal(t, "context", rows[0].Str1)
	require.Equal(t, "read", pr.context().Value(contextKey{}))

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, pr.ReadWithContext(canceled, &rows), context.Canceled)
}

func TestParquetReaderContextVariantsRejectCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pr := new(ParquetReader)
	_, err := NewParquetReaderWithContext(ctx, nil, nil)
	require.ErrorIs(t, err, context.Canceled)
	_, err = NewParquetColumnReaderWithContext(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)
	nilCtx := map[string]context.Context{}["missing"]
	_, err = NewParquetReaderWithContext(nilCtx, nil, nil)
	require.ErrorContains(t, err, "context is nil")
	_, err = NewParquetColumnReaderWithContext(nilCtx, nil)
	require.ErrorContains(t, err, "context is nil")

	tests := []struct {
		name string
		run  func() error
	}{
		{name: "footer size", run: func() error { _, err := pr.GetFooterSizeWithContext(ctx); return err }},
		{name: "footer", run: func() error { return pr.ReadFooterWithContext(ctx) }},
		{name: "read", run: func() error { return pr.ReadWithContext(ctx, nil) }},
		{name: "read by number", run: func() error { _, err := pr.ReadByNumberWithContext(ctx, 1); return err }},
		{name: "read partial", run: func() error { return pr.ReadPartialWithContext(ctx, nil, "") }},
		{name: "read partial by number", run: func() error { _, err := pr.ReadPartialByNumberWithContext(ctx, 1, ""); return err }},
		{name: "skip", run: func() error { return pr.SkipRowsWithContext(ctx, 1) }},
		{name: "skip by path", run: func() error { return pr.SkipRowsByPathWithContext(ctx, "", 1) }},
		{name: "skip by index", run: func() error { return pr.SkipRowsByIndexWithContext(ctx, 0, 1) }},
		{name: "column by path", run: func() error { _, _, _, err := pr.ReadColumnByPathWithContext(ctx, "", 1); return err }},
		{name: "column by index", run: func() error { _, _, _, err := pr.ReadColumnByIndexWithContext(ctx, 0, 1); return err }},
		{name: "bloom filter", run: func() error { _, err := pr.BloomFilterCheckWithContext(ctx, "", 0, nil); return err }},
		{name: "column index", run: func() error { _, err := pr.ReadColumnIndexWithContext(ctx, 0, 0); return err }},
		{name: "offset index", run: func() error { _, err := pr.ReadOffsetIndexWithContext(ctx, 0, 0); return err }},
		{name: "all page headers", run: func() error { _, err := pr.GetAllPageHeadersWithContext(ctx, 0, 0); return err }},
		{name: "first page header", run: func() error { _, err := pr.GetFirstDataPageHeaderWithContext(ctx, 0, 0); return err }},
		{name: "dictionary values", run: func() error { _, err := pr.ReadDictionaryPageValuesWithContext(ctx, 0, 0, 0); return err }},
		{name: "column dictionary values", run: func() error { _, err := pr.ReadDictionaryPageValuesInColumnWithContext(ctx, 0, 0); return err }},
		{name: "reset", run: func() error { return pr.ResetWithContext(ctx) }},
		{name: "stop", run: func() error { return pr.ReadStopWithContext(ctx) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.ErrorIs(t, tt.run(), context.Canceled)
		})
	}

	ctx = nil
	for _, tt := range tests {
		t.Run("nil/"+tt.name, func(t *testing.T) {
			require.ErrorContains(t, tt.run(), "context is nil")
		})
	}
}
