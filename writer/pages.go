package writer

import (
	"fmt"
	"sync"

	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func (pw *ParquetWriter) tableCompressionCodec(table *layout.Table) parquet.CompressionCodec {
	if table.Info != nil && table.Info.CompressionCodec != nil {
		return *table.Info.CompressionCodec
	}
	return pw.compressionType
}

func (pw *ParquetWriter) tableToPlainPages(table *layout.Table, compressionType parquet.CompressionCodec, compressor *compress.Compressor) ([]*layout.Page, error) {
	pages, _, err := layout.TableToDataPagesWithOption(table, layout.PageWriteOption{
		Context:         pw.context(),
		PageSize:        int32(pw.pageSize),
		CompressType:    compressionType,
		DataPageVersion: pw.dataPageVersion,
		WriteCRC:        pw.writeCRC,
		Compressor:      compressor,
	})
	if err != nil {
		return nil, fmt.Errorf("build data pages: %w", err)
	}
	return pages, nil
}

func (pw *ParquetWriter) convertTableToPages(name string, table *layout.Table) ([]*layout.Page, error) {
	compressionType := pw.tableCompressionCodec(table)
	compressor := pw.compressorForColumn(name)
	if usesDictionaryEncoding(table) {
		return pw.tableToDictPages(name, table, compressionType, compressor)
	}
	return pw.tableToPlainPages(table, compressionType, compressor)
}

func (pw *ParquetWriter) mergePageResults(pagesMapList []map[string][]*layout.Page) {
	for _, pagesMap := range pagesMapList {
		for name, pages := range pagesMap {
			if _, ok := pw.pagesMapBuf[name]; !ok {
				pw.pagesMapBuf[name] = pages
			} else {
				pw.pagesMapBuf[name] = append(pw.pagesMapBuf[name], pages...)
			}
			for _, page := range pages {
				pw.size += int64(len(page.RawData))
				if !isDictionaryDataPage(page) {
					page.DataTable = nil // release memory
				}
			}
		}
	}
}

func (pw *ParquetWriter) marshalAndConvertPlain(b, e int, pagesMap map[string][]*layout.Page, bloomMu *sync.Mutex) (map[string]*layout.Table, error) {
	if e <= b {
		return nil, nil
	}
	tableMap, err := pw.marshalFunc(pw.objs[b:e], pw.SchemaHandler)
	if err != nil {
		return nil, err
	}
	for name, table := range *tableMap {
		pw.insertBloomValues(name, table, bloomMu)
		if usesDictionaryEncoding(table) {
			continue
		}
		pages, err := pw.convertTableToPages(name, table)
		if err != nil {
			return nil, err
		}
		pagesMap[name] = pages
	}
	return *tableMap, nil
}

func firstPageConversionError(errs []error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (pw *ParquetWriter) convertDictionaryTables(tableMaps []map[string]*layout.Table, pagesMaps []map[string][]*layout.Page) error {
	for index, tableMap := range tableMaps {
		for name, table := range tableMap {
			if !usesDictionaryEncoding(table) {
				continue
			}
			pages, err := pw.convertTableToPages(name, table)
			if err != nil {
				return err
			}
			pagesMaps[index][name] = pages
		}
	}
	return nil
}

func (pw *ParquetWriter) flushObjs() error {
	l := int64(len(pw.objs))
	if l <= 0 {
		return nil
	}
	pagesMapList := make([]map[string][]*layout.Page, pw.np)
	tableMapList := make([]map[string]*layout.Table, pw.np)
	for i := range pw.np {
		pagesMapList[i] = make(map[string][]*layout.Page)
	}

	delta := (l + pw.np - 1) / pw.np
	var bloomMu sync.Mutex
	var wg sync.WaitGroup
	errs := make([]error, pw.np)

	for c := range pw.np {
		bgn := c * delta
		end := bgn + delta
		if end > l {
			end = l
		}
		if bgn >= l {
			bgn, end = l, l
		}

		wg.Add(1)
		go func(b, e int, index int64) {
			defer wg.Done()
			tableMapList[index], errs[index] = pw.marshalAndConvertPlain(b, e, pagesMapList[index], &bloomMu)
		}(int(bgn), int(end), c)
	}

	wg.Wait()

	err := firstPageConversionError(errs)
	if err == nil {
		err = pw.convertDictionaryTables(tableMapList, pagesMapList)
	}

	pw.mergePageResults(pagesMapList)
	pw.numRows += int64(len(pw.objs))
	if err != nil {
		return fmt.Errorf("flush objects: %w", err)
	}
	return nil
}
