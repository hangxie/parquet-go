package writer

import (
	"fmt"
	"math/bits"

	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func usesDictionaryEncoding(table *layout.Table) bool {
	return table.Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY ||
		table.Info.Encoding == parquet.Encoding_RLE_DICTIONARY
}

func (pw *ParquetWriter) tableToDictPages(name string, table *layout.Table, compressionType parquet.CompressionCodec, compressor *compress.Compressor) ([]*layout.Page, error) {
	var dictRec *layout.DictRecType
	if v, ok := pw.DictRecs.Load(name); ok {
		dictRec = v.(*layout.DictRecType)
	} else {
		newRec := layout.NewDictRecWithLimit(*table.Schema.Type, pw.maxDictionarySize)
		actual, _ := pw.DictRecs.LoadOrStore(name, newRec)
		dictRec = actual.(*layout.DictRecType)
	}

	pages, _, err := layout.TableToDictDataPagesWithOption(dictRec, table, layout.PageWriteOption{
		Context:         pw.context(),
		PageSize:        int32(pw.pageSize),
		CompressType:    compressionType,
		WriteCRC:        pw.writeCRC,
		Compressor:      compressor,
		DataPageVersion: pw.dataPageVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("build dict pages: %w", err)
	}
	return pages, nil
}

func hasDictionaryDataPage(pages []*layout.Page) bool {
	for _, page := range pages {
		if page == nil {
			continue
		}
		if page.Header != nil && page.Header.DataPageHeader != nil &&
			page.Header.DataPageHeader.Encoding == parquet.Encoding_RLE_DICTIONARY {
			return true
		}
		if page.Header == nil && page.Info != nil &&
			(page.Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY ||
				page.Info.Encoding == parquet.Encoding_RLE_DICTIONARY) {
			return true
		}
	}
	return false
}

func (pw *ParquetWriter) buildDictionaryChunk(name string, pages []*layout.Page, compressionType parquet.CompressionCodec) (*layout.Chunk, error) {
	v, ok := pw.DictRecs.Load(name)
	if !ok {
		return nil, fmt.Errorf("missing dictionary recorder for column %s", name)
	}
	dictRec := v.(*layout.DictRecType)
	bitWidth := int32(0)
	if len(dictRec.DictSlice) > 1 {
		bitWidth = int32(bits.Len(uint(len(dictRec.DictSlice) - 1)))
	}
	if err := layout.FinalizeDictDataPagesWithOption(pages, bitWidth, layout.PageWriteOption{
		Context:      pw.context(),
		CompressType: compressionType,
		WriteCRC:     pw.writeCRC,
		Compressor:   pw.compressorForColumn(name),
	}); err != nil {
		return nil, fmt.Errorf("finalize dict data pages for column %s: %w", name, err)
	}
	dictPage, _, err := layout.DictRecToDictPageWithOption(dictRec, layout.PageWriteOption{
		Context:      pw.context(),
		PageSize:     int32(pw.pageSize),
		CompressType: compressionType,
		WriteCRC:     pw.writeCRC,
		Compressor:   pw.compressorForColumn(name),
	})
	if err != nil {
		return nil, fmt.Errorf("convert dict rec to dict page for column %s: %w", name, err)
	}
	chunk, err := layout.PagesToDictChunk(append([]*layout.Page{dictPage}, pages...))
	if err != nil {
		return nil, fmt.Errorf("convert pages to dict chunk for column %s: %w", name, err)
	}
	return chunk, nil
}
