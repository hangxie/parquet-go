package writer

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/bloomfilter"
	"github.com/hangxie/parquet-go/v3/layout"
)

// Write the footer and stop writing
func (pw *ParquetWriter) writeColumnIndexes(ts *thrift.TSerializer) error {
	if len(pw.columnIndexes) == 0 {
		return nil
	}
	idx := 0
	for _, rowGroup := range pw.Footer.RowGroups {
		for _, columnChunk := range rowGroup.Columns {
			columnIndexBuf, err := ts.Write(context.TODO(), pw.columnIndexes[idx])
			if err != nil {
				return fmt.Errorf("serialize column index: %w", err)
			}
			if _, err = pw.PFile.Write(columnIndexBuf); err != nil {
				return fmt.Errorf("write column index: %w", err)
			}

			idx++

			pos := pw.offset
			columnChunk.ColumnIndexOffset = &pos
			columnIndexBufSize := int32(len(columnIndexBuf))
			columnChunk.ColumnIndexLength = &columnIndexBufSize

			pw.offset += int64(columnIndexBufSize)
		}
	}
	return nil
}

func (pw *ParquetWriter) writeOffsetIndexes(ts *thrift.TSerializer) error {
	if len(pw.offsetIndexes) == 0 {
		return nil
	}
	idx := 0
	for _, rowGroup := range pw.Footer.RowGroups {
		for _, columnChunk := range rowGroup.Columns {
			offsetIndexBuf, err := ts.Write(context.TODO(), pw.offsetIndexes[idx])
			if err != nil {
				return fmt.Errorf("serialize offset index: %w", err)
			}
			if _, err = pw.PFile.Write(offsetIndexBuf); err != nil {
				return fmt.Errorf("write offset index: %w", err)
			}

			idx++

			pos := pw.offset
			columnChunk.OffsetIndexOffset = &pos
			offsetIndexBufSize := int32(len(offsetIndexBuf))
			columnChunk.OffsetIndexLength = &offsetIndexBufSize

			pw.offset += int64(offsetIndexBufSize)
		}
	}
	return nil
}

func (pw *ParquetWriter) writeBloomFilters() error {
	if len(pw.bloomFilterData) == 0 {
		return nil
	}
	idx := 0
	for _, rowGroup := range pw.Footer.RowGroups {
		for _, columnChunk := range rowGroup.Columns {
			data := pw.bloomFilterData[idx]
			idx++
			if data == nil {
				continue
			}
			if _, err := pw.PFile.Write(data); err != nil {
				return fmt.Errorf("write bloom filter: %w", err)
			}
			pos := pw.offset
			columnChunk.MetaData.BloomFilterOffset = &pos
			totalLen := int32(len(data))
			columnChunk.MetaData.BloomFilterLength = &totalLen
			pw.offset += int64(totalLen)
		}
	}
	return nil
}

func (pw *ParquetWriter) insertBloomValues(name string, table *layout.Table, bloomMu *sync.Mutex) {
	bf, ok := pw.bloomFilters[name]
	if !ok || table.Schema.Type == nil {
		return
	}
	bloomMu.Lock()
	defer bloomMu.Unlock()
	for _, val := range table.Values {
		if val == nil {
			continue
		}
		if h, hashErr := bloomfilter.HashValue(val, *table.Schema.Type); hashErr == nil {
			bf.Insert(h)
		}
	}
}

func (pw *ParquetWriter) serializeBloomFilters() error {
	if len(pw.bloomFilters) == 0 {
		return nil
	}
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	for k := range len(pw.SchemaHandler.SchemaElements) {
		se := pw.SchemaHandler.SchemaElements[k]
		if se.GetNumChildren() > 0 {
			continue
		}
		path := pw.SchemaHandler.IndexMap[int32(k)]
		bf, ok := pw.bloomFilters[path]
		if !ok {
			pw.bloomFilterData = append(pw.bloomFilterData, nil)
			continue
		}
		headerBuf, err := ts.Write(context.TODO(), bf.Header())
		if err != nil {
			return fmt.Errorf("serialize bloom filter header for column %s: %w", path, err)
		}
		pw.bloomFilterData = append(pw.bloomFilterData, append(headerBuf, bf.Bitset()...))
	}
	pw.initBloomFilters()
	return nil
}

// initBloomFilters creates bloom filters for columns that have bloomfilter=true in their tags.
func (pw *ParquetWriter) initBloomFilters() {
	if pw.SchemaHandler == nil {
		return
	}
	pw.bloomFilters = make(map[string]*bloomfilter.Filter)
	for i, info := range pw.SchemaHandler.Infos {
		if info == nil || !info.BloomFilter {
			continue
		}
		path := pw.SchemaHandler.IndexMap[int32(i)]
		numBytes := int(info.BloomFilterSize)
		if numBytes <= 0 {
			numBytes = bloomfilter.DefaultNumBytes
		}
		pw.bloomFilters[path] = bloomfilter.New(numBytes)
	}
}
