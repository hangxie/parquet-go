package reader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/marshal"
)

// Skip rows of parquet file
func (pr *ParquetReader) SkipRows(num int64) error {
	var err error
	if num <= 0 {
		return nil
	}

	// Ensure column buffers exist
	for _, pathStr := range pr.SchemaHandler.ValueColumns {
		if _, ok := pr.ColumnBuffers[pathStr]; !ok {
			if pr.ColumnBuffers[pathStr], err = NewColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr, &layout.PageReadOptions{CRCMode: pr.crcMode, MaxPageSize: layout.DefaultMaxPageSize}); err != nil {
				return fmt.Errorf("create column buffer for %s: %w", pathStr, err)
			}
		}
	}

	// Use errgroup with a semaphore to cap concurrency at pr.np
	g, _ := errgroup.WithContext(context.Background())
	sem := make(chan struct{}, max(1, int(pr.np)))
	for key := range pr.ColumnBuffers {
		pathStr := key
		sem <- struct{}{}
		g.Go(func() error {
			defer func() { <-sem }()
			if _, err := pr.ColumnBuffers[pathStr].SkipRows(int64(num)); err != nil && err == io.EOF {
				return nil
			} else if err != nil {
				return fmt.Errorf("skip rows for column %s: %w", pathStr, err)
			}
			return nil
		})
	}

	// Wait for all tasks; returns first non-nil error
	return g.Wait()
}

// Read rows of parquet file and unmarshal all to dst
func (pr *ParquetReader) Read(dstInterface any) error {
	return pr.read(dstInterface, "")
}

// ReadByNumber reads up to maxReadNumber objects.
func (pr *ParquetReader) ReadByNumber(maxReadNumber int) ([]any, error) {
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

	if err = pr.Read(res.Interface()); err != nil {
		return nil, fmt.Errorf("read by number: %w", err)
	}

	ln := res.Elem().Len()
	ret := make([]any, ln)
	for i := range ln {
		ret[i] = res.Elem().Index(i).Interface()
	}

	return ret, nil
}

// Read rows of parquet file and unmarshal all to dst
func (pr *ParquetReader) ReadPartial(dstInterface any, prefixPath string) error {
	prefixPath, err := pr.SchemaHandler.ConvertToInPathStr(prefixPath)
	if err != nil {
		return fmt.Errorf("convert path: %w", err)
	}

	if err := pr.read(dstInterface, prefixPath); err != nil {
		return fmt.Errorf("read partial: %w", err)
	}
	return nil
}

// Read maxReadNumber partial objects
func (pr *ParquetReader) ReadPartialByNumber(maxReadNumber int, prefixPath string) ([]any, error) {
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

	if err = pr.ReadPartial(res.Interface(), prefixPath); err != nil {
		return nil, fmt.Errorf("read partial by number: %w", err)
	}

	ln := res.Elem().Len()
	ret := make([]any, ln)
	for i := range ln {
		ret[i] = res.Elem().Index(i).Interface()
	}

	return ret, nil
}

// Read rows of parquet file with a prefixPath
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
		if strings.HasPrefix(key, prefixPath) {
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
		return err
	}

	return pr.unmarshalToResult(num, tmap, dstInterface, prefixPath)
}

// Reset resets all column buffers back to the beginning of the file.
// After calling Reset(), the next Read() will start from the first row.
// This allows re-reading the parquet file without creating a new reader.
func (pr *ParquetReader) Reset() error {
	// Close all existing column buffer resources
	for _, cb := range pr.ColumnBuffers {
		if cb == nil || cb.PFile == nil {
			continue
		}

		if err := cb.PFile.Close(); err != nil {
			return fmt.Errorf("close column buffer: %w", err)
		}
	}

	// Recreate all column buffers from scratch
	for pathStr := range pr.ColumnBuffers {
		newCB, err := NewColumnBuffer(pr.PFile, pr.Footer, pr.SchemaHandler, pathStr, &layout.PageReadOptions{CRCMode: pr.crcMode, MaxPageSize: layout.DefaultMaxPageSize})
		if err != nil {
			return fmt.Errorf("recreate column buffer for %s: %w", pathStr, err)
		}
		pr.ColumnBuffers[pathStr] = newCB
	}
	return nil
}

// Stop Read
// ReadStop closes all column buffer file handles and returns any errors encountered.
// This is the error-returning version of ReadStop.
func (pr *ParquetReader) ReadStop() error {
	var errs []error
	for pathStr, cb := range pr.ColumnBuffers {
		if cb == nil {
			continue
		}

		if err := cb.PFile.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close column buffer for path %s: %w", pathStr, err))
		}
	}
	return errors.Join(errs...)
}
