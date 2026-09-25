package reader

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/marshal"
)

// Read reads rows of the parquet file and unmarshals them into dst.
//
// Deprecated: use ReadWithContext.
func (pr *ParquetReader) Read(dstInterface any) error {
	return pr.ReadWithContext(pr.defaultContext(), dstInterface)
}

// ReadWithContext reads rows of the parquet file using ctx.
func (pr *ParquetReader) ReadWithContext(ctx context.Context, dstInterface any) error {
	if err := pr.setContext(ctx); err != nil {
		return err
	}
	return pr.read(dstInterface, "")
}

// ReadByNumber reads up to maxReadNumber objects.
//
// Deprecated: use ReadByNumberWithContext.
func (pr *ParquetReader) ReadByNumber(maxReadNumber int) ([]any, error) {
	return pr.ReadByNumberWithContext(pr.defaultContext(), maxReadNumber)
}

// ReadByNumberWithContext reads up to maxReadNumber objects using ctx.
func (pr *ParquetReader) ReadByNumberWithContext(ctx context.Context, maxReadNumber int) ([]any, error) {
	if err := pr.setContext(ctx); err != nil {
		return nil, err
	}
	if maxReadNumber < 0 {
		return nil, fmt.Errorf("negative maxReadNumber: %d", maxReadNumber)
	}

	var err error
	if pr.ObjType == nil {
		if pr.ObjType, err = pr.SchemaHandler.GetType(pr.SchemaHandler.GetRootInName()); err != nil {
			return nil, fmt.Errorf("get type: %w", err)
		}
	}

	vs := reflect.MakeSlice(reflect.SliceOf(pr.ObjType), maxReadNumber, maxReadNumber)
	res := reflect.New(vs.Type())
	res.Elem().Set(vs)

	if err = pr.ReadWithContext(ctx, res.Interface()); err != nil {
		return nil, fmt.Errorf("read by number: %w", err)
	}

	ln := res.Elem().Len()
	ret := make([]any, ln)
	for i := range ln {
		ret[i] = res.Elem().Index(i).Interface()
	}
	return ret, nil
}

// ReadPartial reads rows and unmarshals only the subtree rooted at prefixPath.
// prefixPath components must be separated by common.ParGoPathDelimiter (build it
// with common.PathToStr, e.g. common.PathToStr([]string{"parquet_go_root", "name"})).
//
// Deprecated: use ReadPartialWithContext.
func (pr *ParquetReader) ReadPartial(dstInterface any, prefixPath string) error {
	return pr.ReadPartialWithContext(pr.defaultContext(), dstInterface, prefixPath)
}

// ReadPartialWithContext reads a subtree rooted at prefixPath using ctx.
func (pr *ParquetReader) ReadPartialWithContext(ctx context.Context, dstInterface any, prefixPath string) error {
	if err := pr.setContext(ctx); err != nil {
		return err
	}
	prefixPath, err := pr.SchemaHandler.ConvertToInPathStr(prefixPath)
	if err != nil {
		return fmt.Errorf("convert path: %w", err)
	}
	if err := pr.read(dstInterface, prefixPath); err != nil {
		return fmt.Errorf("read partial: %w", err)
	}
	return nil
}

// ReadPartialByNumber reads up to maxReadNumber partial objects rooted at prefixPath.
// prefixPath components must be separated by common.ParGoPathDelimiter (build it
// with common.PathToStr, e.g. common.PathToStr([]string{"parquet_go_root", "name"})).
//
// Deprecated: use ReadPartialByNumberWithContext.
func (pr *ParquetReader) ReadPartialByNumber(maxReadNumber int, prefixPath string) ([]any, error) {
	return pr.ReadPartialByNumberWithContext(pr.defaultContext(), maxReadNumber, prefixPath)
}

// ReadPartialByNumberWithContext reads up to maxReadNumber partial objects using ctx.
func (pr *ParquetReader) ReadPartialByNumberWithContext(ctx context.Context, maxReadNumber int, prefixPath string) ([]any, error) {
	if err := pr.setContext(ctx); err != nil {
		return nil, err
	}
	if maxReadNumber < 0 {
		return nil, fmt.Errorf("negative maxReadNumber: %d", maxReadNumber)
	}

	var err error
	if pr.ObjPartialType == nil {
		if pr.ObjPartialType, err = pr.SchemaHandler.GetType(prefixPath); err != nil {
			return nil, fmt.Errorf("get type for prefix: %w", err)
		}
	}

	vs := reflect.MakeSlice(reflect.SliceOf(pr.ObjPartialType), maxReadNumber, maxReadNumber)
	res := reflect.New(vs.Type())
	res.Elem().Set(vs)

	if err = pr.ReadPartialWithContext(ctx, res.Interface(), prefixPath); err != nil {
		return nil, fmt.Errorf("read partial by number: %w", err)
	}

	ln := res.Elem().Len()
	ret := make([]any, ln)
	for i := range ln {
		ret[i] = res.Elem().Index(i).Interface()
	}
	return ret, nil
}

func (pr *ParquetReader) read(dstInterface any, prefixPath string) error {
	if dstInterface == nil {
		return fmt.Errorf("dstInterface is nil")
	}
	tmap := make(map[string]*layout.Table)
	num := reflect.ValueOf(dstInterface).Elem().Len()
	if num <= 0 {
		return nil
	}
	if err := pr.fetchColumnData(num, prefixPath, tmap); err != nil {
		return fmt.Errorf("fetch column data: %w", err)
	}
	return pr.unmarshalToResult(num, tmap, dstInterface, prefixPath)
}

func (pr *ParquetReader) fetchColumnData(num int, prefixPath string, tmap map[string]*layout.Table) error {
	var locker sync.Mutex
	taskChan := make(chan string)
	var wgCols sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	worker := func() {
		defer wgCols.Done()
		for pathStr := range taskChan {
			cb := pr.ColumnBuffers[pathStr]
			table, _, rerr := cb.ReadRows(int64(num))
			if rerr != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = rerr
				}
				errMu.Unlock()
				continue
			}
			locker.Lock()
			if _, ok := tmap[pathStr]; ok {
				tmap[pathStr].Merge(table)
			} else {
				tmap[pathStr] = layout.NewTableFromTable(table)
				tmap[pathStr].Merge(table)
			}
			locker.Unlock()
		}
	}

	for i := int64(0); i < pr.np; i++ {
		wgCols.Add(1)
		go worker()
	}
	for key := range pr.ColumnBuffers {
		if prefixPath == "" || common.IsChildPath(prefixPath, key) {
			if err := pr.context().Err(); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				break
			}
			taskChan <- key
		}
	}
	close(taskChan)
	wgCols.Wait()
	return firstErr
}

func (pr *ParquetReader) unmarshalToResult(num int, tmap map[string]*layout.Table, dstInterface any, prefixPath string) error {
	ot := reflect.TypeOf(dstInterface).Elem().Elem()
	dstList := make([]any, pr.np)
	delta := (int64(num) + pr.np - 1) / pr.np

	var firstErr error
	var errMu sync.Mutex
	var wg sync.WaitGroup
	for c := range pr.np {
		bgn := c * delta
		end := min(bgn+delta, int64(num))
		if bgn >= int64(num) {
			bgn, end = int64(num), int64(num)
		}
		wg.Add(1)
		go func(b, e, index int) {
			defer wg.Done()
			dstList[index] = reflect.New(reflect.SliceOf(ot)).Interface()
			if err2 := marshal.Unmarshal(&tmap, b, e, dstList[index], pr.SchemaHandler, prefixPath); err2 != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err2
				}
				errMu.Unlock()
			}
		}(int(bgn), int(end), int(c))
	}
	wg.Wait()

	dstValue := reflect.ValueOf(dstInterface).Elem()
	dstValue.SetLen(0)
	for _, dst := range dstList {
		dstValue.Set(reflect.AppendSlice(dstValue, reflect.ValueOf(dst).Elem()))
	}
	return firstErr
}
