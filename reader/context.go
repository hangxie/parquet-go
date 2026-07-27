package reader

import (
	"context"
	"fmt"
)

func (pr *ParquetReader) context() context.Context {
	if pr.ctx == nil {
		return context.Background()
	}
	return pr.ctx
}

func (pr *ParquetReader) defaultContext() context.Context {
	if pr.defaultCtx == nil {
		return context.Background()
	}
	return pr.defaultCtx
}

func (pr *ParquetReader) setContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	pr.ctx = ctx
	for _, cb := range pr.ColumnBuffers {
		if cb == nil {
			continue
		}
		cb.PageReadOptions.Context = ctx
	}
	return nil
}
