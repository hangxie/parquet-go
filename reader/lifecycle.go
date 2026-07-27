package reader

import (
	"context"
	"errors"
	"fmt"

	"github.com/hangxie/parquet-go/v3/source"
)

// GetNumRows returns the number of rows declared by the file footer.
func (pr *ParquetReader) GetNumRows() int64 {
	return pr.Footer.GetNumRows()
}

// Reset closes and recreates all column buffers, allowing the file to be
// re-read from the beginning without creating a new reader.
//
// Deprecated: use ResetWithContext.
func (pr *ParquetReader) Reset() error {
	return pr.ResetWithContext(pr.defaultContext())
}

// ResetWithContext closes and recreates column buffers using ctx. Cancellation
// is never allowed to interrupt the close step, but a cancellation that occurs
// during closing is still returned together with the close errors.
func (pr *ParquetReader) ResetWithContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	var errs []error
	for pathStr, cb := range pr.ColumnBuffers {
		if cb == nil || cb.PFile == nil {
			continue
		}
		if err := source.CloseWithContext(ctx, cb.PFile); err != nil {
			errs = append(errs, fmt.Errorf("close column buffer for path %s: %w", pathStr, err))
		}
	}
	if err := errors.Join(append(errs, ctx.Err())...); err != nil {
		return err
	}
	if err := pr.setContext(ctx); err != nil {
		return err
	}
	for pathStr := range pr.ColumnBuffers {
		newCB, err := pr.newColumnBuffer(pathStr)
		if err != nil {
			return fmt.Errorf("recreate column buffer for %s: %w", pathStr, err)
		}
		pr.ColumnBuffers[pathStr] = newCB
	}
	return nil
}

// ReadStop closes all column buffer file handles.
//
// Deprecated: use ReadStopWithContext.
func (pr *ParquetReader) ReadStop() error {
	return pr.ReadStopWithContext(pr.defaultContext())
}

// ReadStopWithContext closes all column-buffer file handles using ctx.
// Cancellation is never allowed to interrupt closing, but a cancellation that
// occurs during closing is still returned together with the close errors.
func (pr *ParquetReader) ReadStopWithContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	var errs []error
	for pathStr, cb := range pr.ColumnBuffers {
		if cb == nil || cb.PFile == nil {
			continue
		}
		if err := source.CloseWithContext(ctx, cb.PFile); err != nil {
			errs = append(errs, fmt.Errorf("close column buffer for path %s: %w", pathStr, err))
		}
	}
	return errors.Join(append(errs, ctx.Err())...)
}
