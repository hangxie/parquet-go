package layout

import (
	"fmt"
	"math/bits"

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

	compressedData, err := page.dictPageCompress(opt.CompressType, dictRec.Type, opt.Compressor)
	if err != nil {
		return nil, 0, err
	}
	if err = serializePage(page, opt, compressedData); err != nil {
		return nil, 0, err
	}
	totSize += int64(len(page.RawData))
	return page, totSize, nil
}

func (page *Page) dictPageCompress(compressType parquet.CompressionCodec, pT parquet.Type, c *compress.Compressor) ([]byte, error) {
	dataBuf, err := encoding.WritePlain(page.DataTable.Values, pT)
	if err != nil {
		return nil, err
	}
	dataEncodeBuf, err := resolveCompressor(c).Compress(dataBuf, compressType)
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

// dictPageValueResult holds the results from scanning one page's worth of values for dictionary encoding.
type dictPageValueResult struct {
	endIdx    int
	numValues int32
	size      int32
	minVal    any
	maxVal    any
	nullCount int64
	values    []int32
}

// scanDictPageValues scans table values from startIdx to build one page, collecting dictionary indices and stats.
func scanDictPageValues(table *Table, dictRec *DictRecType, startIdx int, pageSize int32, omitStats bool, funcTable common.FuncTable) (dictPageValueResult, error) {
	totalLn := len(table.Values)
	r := dictPageValueResult{
		endIdx: startIdx,
		minVal: table.Values[startIdx],
		maxVal: table.Values[startIdx],
	}

	for r.endIdx < totalLn && r.size < pageSize {
		if table.Values[r.endIdx] == nil {
			if err := checkRequiredNil(table, r.endIdx); err != nil {
				return r, err
			}
			r.nullCount++
			r.endIdx++
			continue
		}
		if table.DefinitionLevels[r.endIdx] == table.MaxDefinitionLevel {
			r.numValues++
			var elSize int32
			if omitStats {
				_, _, elSize = funcTable.MinMaxSize(nil, nil, table.Values[r.endIdx])
			} else {
				r.minVal, r.maxVal, elSize = funcTable.MinMaxSize(r.minVal, r.maxVal, table.Values[r.endIdx])
			}
			r.size += elSize
			r.values = append(r.values, dictRec.lookupOrInsert(table.Values[r.endIdx]))
		}
		r.endIdx++
	}
	return r, nil
}

// lookupOrInsert returns the dictionary index for the given value, inserting it if not already present.
func (d *DictRecType) lookupOrInsert(val any) int32 {
	if idx, ok := d.DictMap[val]; ok {
		return idx
	}
	d.DictSlice = append(d.DictSlice, val)
	idx := int32(len(d.DictSlice) - 1)
	d.DictMap[val] = idx
	return idx
}

// checkRequiredNil returns an error if a nil value appears at a position where a REQUIRED field should have a value.
func checkRequiredNil(table *Table, idx int) error {
	if table.Schema.GetRepetitionType() == parquet.FieldRepetitionType_REQUIRED &&
		table.DefinitionLevels[idx] == table.MaxDefinitionLevel {
		return fmt.Errorf("nil value encountered for REQUIRED field %s at index %d", table.Path, idx)
	}
	return nil
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
		funcTable, err := common.FindFuncTable(pT, cT, logT)
		if err != nil {
			return nil, 0, fmt.Errorf("find func table for given types [%v, %v, %v]: %w", pT, cT, logT, err)
		}

		scan, err := scanDictPageValues(table, dictRec, i, opt.PageSize, omitStats, funcTable)
		if err != nil {
			return nil, 0, err
		}

		page := NewDataPage()
		page.PageSize = opt.PageSize
		page.Header.DataPageHeader.NumValues = scan.numValues
		page.Header.Type = parquet.PageType_DATA_PAGE

		page.DataTable = new(Table)
		page.DataTable.RepetitionType = table.RepetitionType
		page.DataTable.Path = table.Path
		page.DataTable.MaxDefinitionLevel = table.MaxDefinitionLevel
		page.DataTable.MaxRepetitionLevel = table.MaxRepetitionLevel
		page.DataTable.DefinitionLevels = table.DefinitionLevels[i:scan.endIdx]
		page.DataTable.RepetitionLevels = table.RepetitionLevels[i:scan.endIdx]

		// Values in DataTable of a DictPage is nil for optimization.
		// page.DataTable.Values = values

		if !omitStats {
			page.MaxVal = scan.maxVal
			page.MinVal = scan.minVal
			page.NullCount = &scan.nullCount
		}
		page.Schema = table.Schema
		page.CompressType = opt.CompressType
		page.Path = table.Path
		page.Info = table.Info

		page.computeLevelHistograms()

		compressedData, compressErr := page.dictDataPageCompress(opt.CompressType, bitWidth, scan.values, opt.Compressor)
		if compressErr != nil {
			return nil, 0, compressErr
		}
		if err = serializePage(page, opt, compressedData); err != nil {
			return nil, 0, err
		}

		totSize += int64(len(page.RawData))
		res = append(res, page)
		i = scan.endIdx
	}
	return res, totSize, nil
}

func (page *Page) dictDataPageCompress(compressType parquet.CompressionCodec, bitWidth int32, values []int32, c *compress.Compressor) ([]byte, error) {
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

	dataEncodeBuf, err := resolveCompressor(c).Compress(dataBuf, compressType)
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
