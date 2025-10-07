package reader

import (
	"fmt"

	"github.com/hangxie/parquet-go/v2/schema"
	"github.com/hangxie/parquet-go/v2/source"
)

// NewParquetColumnReader creates a parquet column reader
func NewParquetColumnReader(pFile source.ParquetFileReader, np int64) (*ParquetReader, error) {
	res := new(ParquetReader)
	res.NP = np
	res.PFile = pFile
	if err := res.ReadFooter(); err != nil {
		return nil, fmt.Errorf("read footer: %w", err)
	}
	res.ColumnBuffers = make(map[string]*ColumnBufferType)
	res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(res.Footer.GetSchema())
	res.RenameSchema()

	return res, nil
}

func (pr *ParquetReader) SkipRowsByPath(pathStr string, num int64) error {
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
		if pr.ColumnBuffers[pathStr], err = NewColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr); err != nil {
			return fmt.Errorf("init column buffer for %v: %w", pathStr, err)
		}
	}

	if cb, ok := pr.ColumnBuffers[pathStr]; !ok {
		return errPathNotFound
	} else if _, err := cb.SkipRowsWithError(int64(num)); err != nil {
		return fmt.Errorf("skip rows by path %v: %w", pathStr, err)
	}

	return nil
}

func (pr *ParquetReader) SkipRowsByIndex(index, num int64) {
	if pr.SchemaHandler == nil || pr.SchemaHandler.ValueColumns == nil {
		return
	}
	if index >= int64(len(pr.SchemaHandler.ValueColumns)) {
		return
	}
	pathStr := pr.SchemaHandler.ValueColumns[index]
	// return error till we can change function signature
	_ = pr.SkipRowsByPath(pathStr, num)
}

// ReadColumnByPath reads column by path in schema.
func (pr *ParquetReader) ReadColumnByPath(pathStr string, num int64) (values []any, rls, dls []int32, err error) {
	errPathNotFound := fmt.Errorf("path %v not found", pathStr)

	pathStr, err = pr.SchemaHandler.ConvertToInPathStr(pathStr)
	if num <= 0 || len(pathStr) <= 0 || err != nil {
		if err != nil {
			return []any{}, []int32{}, []int32{}, fmt.Errorf("convert path %v: %w", pathStr, err)
		}
		return []any{}, []int32{}, []int32{}, err
	}

	if _, ok := pr.SchemaHandler.MapIndex[pathStr]; !ok {
		return []any{}, []int32{}, []int32{}, errPathNotFound
	}

	if _, ok := pr.ColumnBuffers[pathStr]; !ok {
		var err error
		if pr.ColumnBuffers[pathStr], err = NewColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr); err != nil {
			return []any{}, []int32{}, []int32{}, fmt.Errorf("init column buffer for %s: %w", pathStr, err)
		}
	}

	if cb, ok := pr.ColumnBuffers[pathStr]; ok {
		table, _, rerr := cb.ReadRowsWithError(int64(num))
		if rerr != nil {
			return []any{}, []int32{}, []int32{}, fmt.Errorf("read rows %v: %w", pathStr, rerr)
		}
		return table.Values, table.RepetitionLevels, table.DefinitionLevels, nil
	}
	return []any{}, []int32{}, []int32{}, errPathNotFound
}

// ReadColumnByIndex reads column by index. The index of first column is 0.
func (pr *ParquetReader) ReadColumnByIndex(index, num int64) ([]any, []int32, []int32, error) {
	if index < 0 || index >= int64(len(pr.SchemaHandler.ValueColumns)) {
		return nil, nil, nil, fmt.Errorf("index %v out of range [0, %v)", index, len(pr.SchemaHandler.ValueColumns))
	}
	pathStr := pr.SchemaHandler.ValueColumns[index]
	values, rls, dls, err := pr.ReadColumnByPath(pathStr, num)
	if err != nil {
		return values, rls, dls, fmt.Errorf("read column by index %v: %w", index, err)
	}
	return values, rls, dls, nil
}
