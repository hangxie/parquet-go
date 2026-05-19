package writer

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/bloomfilter"
	"github.com/hangxie/parquet-go/v3/internal/layout"
)

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
	rowGroup := pw.Footer.RowGroups[len(pw.Footer.RowGroups)-1]
	// Iterate rg.Columns so serialized bloom filters stay aligned with the
	// footer columns. Walking SchemaElements would count dropped leaves and
	// diverge from the reader when buildRowGroup skips a column.
	for _, cc := range rowGroup.Columns {
		path := common.PathToStr(cc.MetaData.GetPathInSchema())
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
	return pw.initBloomFilters()
}

// initBloomFilters creates bloom filters for columns that have bloomfilter=true in their tags.
// It returns an error if any column specifies an explicit bloomfiltersize outside the valid
// range [bloomfilter.MinBytes, bloomfilter.MaxBytes].
func (pw *ParquetWriter) initBloomFilters() error {
	if pw.SchemaHandler == nil {
		return nil
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
		} else if numBytes < bloomfilter.MinBytes || numBytes > bloomfilter.MaxBytes {
			return fmt.Errorf("column %q: bloomfiltersize %d is out of valid range [%d, %d]",
				path, numBytes, bloomfilter.MinBytes, bloomfilter.MaxBytes)
		}
		pw.bloomFilters[path] = bloomfilter.New(numBytes)
	}
	return nil
}
