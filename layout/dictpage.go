package layout

import (
	"context"
	"fmt"
	"math/bits"
	"slices"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/compress"
	"github.com/hangxie/parquet-go/v3/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
)

type DictRecType struct {
	DictMap   map[any]int32
	DictSlice []any
	Type      parquet.Type
}

func NewDictRec(pT parquet.Type) *DictRecType {
	res := new(DictRecType)
	res.DictMap = make(map[any]int32)
	res.Type = pT
	return res
}

// Deprecated: Use DictRecToDictPageWithOption instead.
func DictRecToDictPage(dictRec *DictRecType, pageSize int32, compressType parquet.CompressionCodec) (*Page, int64, error) {
	return DictRecToDictPageWithOption(dictRec, PageWriteOption{
		PageSize:     pageSize,
		CompressType: compressType,
	})
}

// DictRecToDictPageWithOption converts a dictionary record to a dictionary page using the provided options.
func DictRecToDictPageWithOption(dictRec *DictRecType, opt PageWriteOption) (*Page, int64, error) {
	var totSize int64 = 0

	page := NewDataPage()
	page.PageSize = opt.PageSize
	page.Header.DataPageHeader.NumValues = int32(len(dictRec.DictSlice))
	page.Header.Type = parquet.PageType_DICTIONARY_PAGE

	page.DataTable = new(Table)
	page.DataTable.Values = dictRec.DictSlice
	dataType := parquet.Type_INT32
	page.Schema = &parquet.SchemaElement{
		Type: &dataType,
	}
	page.CompressType = opt.CompressType

	compressedData, err := page.dictPageCompress(opt.CompressType, dictRec.Type)
	if err != nil {
		return nil, 0, err
	}
	if err = serializePage(page, opt, compressedData); err != nil {
		return nil, 0, err
	}
	totSize += int64(len(page.RawData))
	return page, totSize, nil
}

// Deprecated: Use DictRecToDictPageWithOption instead.
//
// Before (manually build page, compress, then use page.RawData):
//
//	page := NewDataPage()
//	page.DataTable = &Table{Values: dictRec.DictSlice}
//	// ... set up page fields ...
//	_, err := page.DictPageCompress(compressType, dictRec.Type)
//	// serialized data is in page.RawData
//
// After (returns a ready-to-use page with RawData already set):
//
//	page, size, err := DictRecToDictPageWithOption(dictRec, PageWriteOption{
//	    PageSize:     pageSize,
//	    CompressType: compressType,
//	})
func (page *Page) DictPageCompress(compressType parquet.CompressionCodec, pT parquet.Type) ([]byte, error) {
	compressedData, err := page.dictPageCompress(compressType, pT)
	if err != nil {
		return nil, err
	}
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	pageHeaderBuf, err := ts.Write(context.TODO(), page.Header)
	if err != nil {
		return nil, err
	}

	page.RawData = slices.Concat(pageHeaderBuf, compressedData)
	return page.RawData, nil
}

func (page *Page) dictPageCompress(compressType parquet.CompressionCodec, pT parquet.Type) ([]byte, error) {
	dataBuf, err := encoding.WritePlain(page.DataTable.Values, pT)
	if err != nil {
		return nil, err
	}
	dataEncodeBuf, err := compress.CompressWithError(dataBuf, compressType)
	if err != nil {
		return nil, err
	}

	page.Header = parquet.NewPageHeader()
	page.Header.Type = parquet.PageType_DICTIONARY_PAGE
	page.Header.CompressedPageSize = int32(len(dataEncodeBuf))
	page.Header.UncompressedPageSize = int32(len(dataBuf))
	page.Header.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
	page.Header.DictionaryPageHeader.NumValues = int32(len(page.DataTable.Values))
	page.Header.DictionaryPageHeader.Encoding = parquet.Encoding_PLAIN

	return dataEncodeBuf, nil
}

// Deprecated: Use TableToDictDataPagesWithOption instead.
func TableToDictDataPages(dictRec *DictRecType, table *Table, pageSize, bitWidth int32, compressType parquet.CompressionCodec) ([]*Page, int64, error) {
	return TableToDictDataPagesWithOption(dictRec, table, bitWidth, PageWriteOption{
		PageSize:     pageSize,
		CompressType: compressType,
	})
}

// TableToDictDataPagesWithOption converts a table to dictionary-encoded data pages using the provided options.
func TableToDictDataPagesWithOption(dictRec *DictRecType, table *Table, bitWidth int32, opt PageWriteOption) ([]*Page, int64, error) {
	var totSize int64 = 0
	totalLn := len(table.Values)
	if totalLn == 0 {
		return []*Page{}, 0, nil
	}
	res := make([]*Page, 0)
	i := 0

	pT, cT, logT, omitStats := table.Schema.Type, table.Schema.ConvertedType, table.Schema.LogicalType, table.Info.OmitStats

	for i < totalLn {
		j := i
		var size int32 = 0
		var numValues int32 = 0

		var maxVal any = table.Values[i]
		var minVal any = table.Values[i]
		var nullCount int64 = 0
		values := make([]int32, 0)

		funcTable, err := common.FindFuncTable(pT, cT, logT)
		if err != nil {
			return nil, 0, fmt.Errorf("find func table for given types [%v, %v, %v]: %w", pT, cT, logT, err)
		}

		for j < totalLn && size < opt.PageSize {
			if table.Values[j] == nil {
				// Check if this is a required field with a value that should be present
				// DefinitionLevel == MaxDefinitionLevel means we're at a leaf that should have a value
				if table.Schema.GetRepetitionType() == parquet.FieldRepetitionType_REQUIRED &&
					table.DefinitionLevels[j] == table.MaxDefinitionLevel {
					return nil, 0, fmt.Errorf("nil value encountered for REQUIRED field %s at index %d", table.Path, j)
				}
				nullCount++
				j++
				continue
			}
			if table.DefinitionLevels[j] == table.MaxDefinitionLevel {
				numValues++
				var elSize int32
				if omitStats {
					_, _, elSize = funcTable.MinMaxSize(nil, nil, table.Values[j])
				} else {
					minVal, maxVal, elSize = funcTable.MinMaxSize(minVal, maxVal, table.Values[j])
				}
				size += elSize
				if idx, ok := dictRec.DictMap[table.Values[j]]; ok {
					values = append(values, idx)
				} else {
					dictRec.DictSlice = append(dictRec.DictSlice, table.Values[j])
					idx := int32(len(dictRec.DictSlice) - 1)
					dictRec.DictMap[table.Values[j]] = idx
					values = append(values, idx)
				}
			}
			j++
		}

		page := NewDataPage()
		page.PageSize = opt.PageSize
		page.Header.DataPageHeader.NumValues = numValues
		page.Header.Type = parquet.PageType_DATA_PAGE

		page.DataTable = new(Table)
		page.DataTable.RepetitionType = table.RepetitionType
		page.DataTable.Path = table.Path
		page.DataTable.MaxDefinitionLevel = table.MaxDefinitionLevel
		page.DataTable.MaxRepetitionLevel = table.MaxRepetitionLevel
		page.DataTable.DefinitionLevels = table.DefinitionLevels[i:j]
		page.DataTable.RepetitionLevels = table.RepetitionLevels[i:j]

		// Values in DataTable of a DictPage is nil for optimization.
		// page.DataTable.Values = values

		if !omitStats {
			page.MaxVal = maxVal
			page.MinVal = minVal
			page.NullCount = &nullCount
		}
		page.Schema = table.Schema
		page.CompressType = opt.CompressType
		page.Path = table.Path
		page.Info = table.Info

		page.computeLevelHistograms()

		compressedData, compressErr := page.dictDataPageCompress(opt.CompressType, bitWidth, values)
		if compressErr != nil {
			return nil, 0, compressErr
		}
		if err = serializePage(page, opt, compressedData); err != nil {
			return nil, 0, err
		}

		totSize += int64(len(page.RawData))
		res = append(res, page)
		i = j
	}
	return res, totSize, nil
}

// Deprecated: Use TableToDictDataPagesWithOption instead.
//
// Before (manually build page, compress, then use page.RawData):
//
//	page := NewDataPage()
//	// ... set up page fields from table ...
//	_, err := page.DictDataPageCompress(compressType, bitWidth, values)
//	// serialized data is in page.RawData
//
// After (returns ready-to-use pages with RawData already set):
//
//	pages, size, err := TableToDictDataPagesWithOption(dictRec, table, bitWidth, PageWriteOption{
//	    PageSize:     pageSize,
//	    CompressType: compressType,
//	})
func (page *Page) DictDataPageCompress(compressType parquet.CompressionCodec, bitWidth int32, values []int32) ([]byte, error) {
	compressedData, err := page.dictDataPageCompress(compressType, bitWidth, values)
	if err != nil {
		return nil, err
	}
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	pageHeaderBuf, err := ts.Write(context.TODO(), page.Header)
	if err != nil {
		return nil, err
	}

	page.RawData = slices.Concat(pageHeaderBuf, compressedData)
	return page.RawData, nil
}

func (page *Page) dictDataPageCompress(compressType parquet.CompressionCodec, bitWidth int32, values []int32) ([]byte, error) {
	valuesRawBuf := []byte{byte(bitWidth)}
	valuesRawBuf = append(valuesRawBuf, encoding.WriteRLEInt32(values, bitWidth)...)

	var definitionLevelBuf []byte
	var err error
	if page.DataTable.MaxDefinitionLevel > 0 {
		definitionLevelBuf, err = encoding.WriteRLEBitPackedHybridInt32(
			page.DataTable.DefinitionLevels,
			int32(bits.Len32(uint32(page.DataTable.MaxDefinitionLevel))))
		if err != nil {
			return nil, err
		}
	}

	var repetitionLevelBuf []byte
	if page.DataTable.MaxRepetitionLevel > 0 {
		repetitionLevelBuf, err = encoding.WriteRLEBitPackedHybridInt32(
			page.DataTable.RepetitionLevels,
			int32(bits.Len32(uint32(page.DataTable.MaxRepetitionLevel))))
		if err != nil {
			return nil, err
		}
	}

	// dataBuf = repetitionBuf + definitionBuf + valuesRawBuf
	var dataBuf []byte
	dataBuf = append(dataBuf, repetitionLevelBuf...)
	dataBuf = append(dataBuf, definitionLevelBuf...)
	dataBuf = append(dataBuf, valuesRawBuf...)

	dataEncodeBuf, err := compress.CompressWithError(dataBuf, compressType)
	if err != nil {
		return nil, err
	}

	page.Header = parquet.NewPageHeader()
	page.Header.Type = parquet.PageType_DATA_PAGE
	page.Header.CompressedPageSize = int32(len(dataEncodeBuf))
	page.Header.UncompressedPageSize = int32(len(dataBuf))
	page.Header.DataPageHeader = parquet.NewDataPageHeader()
	page.Header.DataPageHeader.NumValues = int32(len(page.DataTable.DefinitionLevels))
	page.Header.DataPageHeader.DefinitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.RepetitionLevelEncoding = parquet.Encoding_RLE
	page.Header.DataPageHeader.Encoding = parquet.Encoding_RLE_DICTIONARY

	page.Header.DataPageHeader.Statistics = parquet.NewStatistics()
	if page.MaxVal != nil {
		tmpBuf, err := encoding.WritePlain([]any{page.MaxVal}, *page.Schema.Type)
		if err != nil {
			return nil, err
		}
		if *page.Schema.Type == parquet.Type_BYTE_ARRAY {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeader.Statistics.Max = tmpBuf
		page.Header.DataPageHeader.Statistics.MaxValue = tmpBuf
	}
	if page.MinVal != nil {
		tmpBuf, err := encoding.WritePlain([]any{page.MinVal}, *page.Schema.Type)
		if err != nil {
			return nil, err
		}
		if *page.Schema.Type == parquet.Type_BYTE_ARRAY {
			tmpBuf = tmpBuf[4:]
		}
		page.Header.DataPageHeader.Statistics.Min = tmpBuf
		page.Header.DataPageHeader.Statistics.MinValue = tmpBuf
	}

	page.Header.DataPageHeader.Statistics.NullCount = page.NullCount

	return dataEncodeBuf, nil
}
