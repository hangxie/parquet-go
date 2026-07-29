package reader

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
)

// NewParquetColumnReader creates a parquet column reader.
//
// Deprecated: use NewParquetColumnReaderWithContext.
func NewParquetColumnReader(pFile source.ParquetFileReader, opts ...ReaderOption) (*ParquetReader, error) {
	return NewParquetColumnReaderWithContext(context.Background(), pFile, opts...)
}

// NewParquetColumnReaderWithContext creates a parquet column reader using ctx.
func NewParquetColumnReaderWithContext(ctx context.Context, pFile source.ParquetFileReader, opts ...ReaderOption) (*ParquetReader, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context is nil")
	}
	res := new(ParquetReader)
	res.PFile = pFile
	res.defaultCtx = ctx
	res.ctx = ctx

	if err := applyReaderDefaults(res, opts); err != nil {
		return nil, fmt.Errorf("apply reader options: %w", err)
	}
	if err := res.ReadFooterWithContext(ctx); err != nil {
		return nil, fmt.Errorf("read footer: %w", err)
	}
	res.ColumnBuffers = make(map[string]*ColumnBufferType)
	res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(res.Footer.GetSchema())
	if err := res.validateColumnKeyPaths(); err != nil {
		return res, err
	}

	return res, nil
}

// SkipRows skips num rows across all value columns in parallel.
//
// Deprecated: use SkipRowsWithContext.
func (pr *ParquetReader) SkipRows(num int64) error {
	return pr.SkipRowsWithContext(pr.defaultContext(), num)
}

// SkipRowsWithContext skips num rows across all value columns using ctx.
func (pr *ParquetReader) SkipRowsWithContext(ctx context.Context, num int64) error {
	if err := pr.setContext(ctx); err != nil {
		return err
	}
	var err error
	if num <= 0 {
		return nil
	}

	for _, pathStr := range pr.SchemaHandler.ValueColumns {
		if _, ok := pr.ColumnBuffers[pathStr]; !ok {
			if pr.ColumnBuffers[pathStr], err = pr.newColumnBuffer(pathStr); err != nil {
				return fmt.Errorf("create column buffer for %s: %w", pathStr, err)
			}
		}
	}

	g, ctx := errgroup.WithContext(ctx)
	sem := make(chan struct{}, max(1, int(pr.np)))
	var launchErr error
launch:
	for key := range pr.ColumnBuffers {
		pathStr := key
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			launchErr = ctx.Err()
			break launch
		}
		g.Go(func() error {
			defer func() { <-sem }()
			// SkipRows returns nil on normal completion (including skipping past the end
			// of the column), so any error here is real — e.g. a truncated page — and
			// must surface rather than be swallowed.
			if _, err := pr.ColumnBuffers[pathStr].SkipRows(int64(num)); err != nil {
				return fmt.Errorf("skip rows for column %s: %w", pathStr, err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return launchErr
}

// SkipRowsByPath skips num rows in the column identified by pathStr. pathStr
// components must be separated by common.ParGoPathDelimiter (build it with
// common.PathToStr); "." is an ordinary character in a name, not a separator.
//
// Deprecated: use SkipRowsByPathWithContext.
func (pr *ParquetReader) SkipRowsByPath(pathStr string, num int64) error {
	return pr.SkipRowsByPathWithContext(pr.defaultContext(), pathStr, num)
}

// SkipRowsByPathWithContext skips rows in a column using ctx.
func (pr *ParquetReader) SkipRowsByPathWithContext(ctx context.Context, pathStr string, num int64) error {
	if err := pr.setContext(ctx); err != nil {
		return err
	}
	errPathNotFound := fmt.Errorf("path %v not found", pathStr)

	if pr.SchemaHandler == nil {
		return fmt.Errorf("SchemaHandler is nil")
	}

	pathStr, err := pr.SchemaHandler.ConvertToInPathStr(pathStr)
	if num <= 0 || len(pathStr) <= 0 || err != nil {
		if err != nil {
			return fmt.Errorf("convert path: %w", err)
		}
		return nil
	}

	if _, ok := pr.SchemaHandler.MapIndex[pathStr]; !ok {
		return errPathNotFound
	}

	if pr.ColumnBuffers == nil {
		return fmt.Errorf("ColumnBuffers is nil")
	}

	if _, ok := pr.ColumnBuffers[pathStr]; !ok {
		var err error
		if pr.ColumnBuffers[pathStr], err = pr.newColumnBuffer(pathStr); err != nil {
			return fmt.Errorf("init column buffer for %v: %w", pathStr, err)
		}
	}

	if cb, ok := pr.ColumnBuffers[pathStr]; !ok {
		return errPathNotFound
	} else if _, err := cb.SkipRows(int64(num)); err != nil {
		return fmt.Errorf("skip rows by path %v: %w", pathStr, err)
	}

	return nil
}

// SkipRowsByIndex skips rows by index and returns any errors encountered.
// This is the error-returning version of SkipRowsByIndex.
//
// Deprecated: use SkipRowsByIndexWithContext.
func (pr *ParquetReader) SkipRowsByIndex(index, num int64) error {
	return pr.SkipRowsByIndexWithContext(pr.defaultContext(), index, num)
}

// SkipRowsByIndexWithContext skips rows by column index using ctx.
func (pr *ParquetReader) SkipRowsByIndexWithContext(ctx context.Context, index, num int64) error {
	if err := pr.setContext(ctx); err != nil {
		return err
	}
	if pr.SchemaHandler == nil {
		return fmt.Errorf("SchemaHandler is nil")
	}
	if pr.SchemaHandler.ValueColumns == nil {
		return fmt.Errorf("ValueColumns is nil")
	}
	if index >= int64(len(pr.SchemaHandler.ValueColumns)) {
		return fmt.Errorf("index %d out of range (max: %d)", index, len(pr.SchemaHandler.ValueColumns)-1)
	}
	pathStr := pr.SchemaHandler.ValueColumns[index]
	if err := pr.SkipRowsByPathWithContext(ctx, pathStr, num); err != nil {
		return fmt.Errorf("skip rows by path %s: %w", pathStr, err)
	}
	return nil
}

// ReadColumnByPath reads column by path in schema. pathStr components must be
// separated by common.ParGoPathDelimiter (build it with common.PathToStr); "." is
// an ordinary character in a name, not a separator.
//
// Deprecated: use ReadColumnByPathWithContext.
func (pr *ParquetReader) ReadColumnByPath(pathStr string, num int64) (values []any, rls, dls []int32, err error) {
	return pr.ReadColumnByPathWithContext(pr.defaultContext(), pathStr, num)
}

// ReadColumnByPathWithContext reads a column by path using ctx.
func (pr *ParquetReader) ReadColumnByPathWithContext(ctx context.Context, pathStr string, num int64) (values []any, rls, dls []int32, err error) {
	if err := pr.setContext(ctx); err != nil {
		return nil, nil, nil, err
	}
	errPathNotFound := fmt.Errorf("path %v not found", pathStr)

	pathStr, err = pr.SchemaHandler.ConvertToInPathStr(pathStr)
	if num <= 0 || len(pathStr) <= 0 || err != nil {
		if err != nil {
			return []any{}, []int32{}, []int32{}, fmt.Errorf("convert path %v: %w", pathStr, err)
		}
		return []any{}, []int32{}, []int32{}, nil
	}

	if _, ok := pr.SchemaHandler.MapIndex[pathStr]; !ok {
		return []any{}, []int32{}, []int32{}, errPathNotFound
	}

	if _, ok := pr.ColumnBuffers[pathStr]; !ok {
		var err error
		if pr.ColumnBuffers[pathStr], err = pr.newColumnBuffer(pathStr); err != nil {
			return []any{}, []int32{}, []int32{}, fmt.Errorf("init column buffer for %s: %w", pathStr, err)
		}
	}

	if cb, ok := pr.ColumnBuffers[pathStr]; ok {
		table, _, rerr := cb.ReadRows(int64(num))
		if rerr != nil {
			return []any{}, []int32{}, []int32{}, fmt.Errorf("read rows %v: %w", pathStr, rerr)
		}
		return table.Values, table.RepetitionLevels, table.DefinitionLevels, nil
	}
	return []any{}, []int32{}, []int32{}, errPathNotFound
}

// ReadColumnByIndex reads column by index. The index of first column is 0.
//
// Deprecated: use ReadColumnByIndexWithContext.
func (pr *ParquetReader) ReadColumnByIndex(index, num int64) ([]any, []int32, []int32, error) {
	return pr.ReadColumnByIndexWithContext(pr.defaultContext(), index, num)
}

// ReadColumnByIndexWithContext reads a column by index using ctx.
func (pr *ParquetReader) ReadColumnByIndexWithContext(ctx context.Context, index, num int64) ([]any, []int32, []int32, error) {
	if err := pr.setContext(ctx); err != nil {
		return nil, nil, nil, err
	}
	if index < 0 || index >= int64(len(pr.SchemaHandler.ValueColumns)) {
		return nil, nil, nil, fmt.Errorf("index %v out of range [0, %v)", index, len(pr.SchemaHandler.ValueColumns))
	}
	pathStr := pr.SchemaHandler.ValueColumns[index]
	values, rls, dls, err := pr.ReadColumnByPathWithContext(ctx, pathStr, num)
	if err != nil {
		return values, rls, dls, fmt.Errorf("read column by index %v: %w", index, err)
	}
	return values, rls, dls, nil
}
