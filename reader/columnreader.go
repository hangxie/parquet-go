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
		return nil, err
	}
	res.ColumnBuffers = make(map[string]*ColumnBufferType)
	res.SchemaHandler = schema.NewSchemaHandlerFromSchemaList(res.Footer.GetSchema())
	res.RenameSchema()

	return res, nil
}

func (pr *ParquetReader) SkipRowsByPath(pathStr string, num int64) error {
	errPathNotFound := fmt.Errorf("path %v not found", pathStr)

	pathStr, err := pr.SchemaHandler.ConvertToInPathStr(pathStr)
	if num <= 0 || len(pathStr) <= 0 || err != nil {
		return err
	}

	if _, ok := pr.SchemaHandler.MapIndex[pathStr]; !ok {
		return errPathNotFound
	}

	if _, ok := pr.ColumnBuffers[pathStr]; !ok {
		var err error
		if pr.ColumnBuffers[pathStr], err = NewColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr); err != nil {
			return err
		}
	}

	if cb, ok := pr.ColumnBuffers[pathStr]; ok {
		cb.SkipRows(int64(num))
	} else {
		return errPathNotFound
	}

	return nil
}

func (pr *ParquetReader) SkipRowsByIndex(index, num int64) {
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
		return []any{}, []int32{}, []int32{}, err
	}

	if _, ok := pr.SchemaHandler.MapIndex[pathStr]; !ok {
		return []any{}, []int32{}, []int32{}, errPathNotFound
	}

	if _, ok := pr.ColumnBuffers[pathStr]; !ok {
		var err error
		if pr.ColumnBuffers[pathStr], err = NewColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr); err != nil {
			return []any{}, []int32{}, []int32{}, err
		}
	}

	if cb, ok := pr.ColumnBuffers[pathStr]; ok {
		table, _ := cb.ReadRows(int64(num))
		return table.Values, table.RepetitionLevels, table.DefinitionLevels, nil
	}
	return []any{}, []int32{}, []int32{}, errPathNotFound
}

// ReadColumnByIndex reads column by index. The index of first column is 0.
func (pr *ParquetReader) ReadColumnByIndex(index, num int64) (values []any, rls, dls []int32, err error) {
	if index >= int64(len(pr.SchemaHandler.ValueColumns)) {
		err = fmt.Errorf("index %v out of range %v", index, len(pr.SchemaHandler.ValueColumns))
		return
	}
	pathStr := pr.SchemaHandler.ValueColumns[index]
	return pr.ReadColumnByPath(pathStr, num)
}
