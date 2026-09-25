package layout

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
)

type DictRecType struct {
	DictMap   map[any]int32
	DictSlice []any
	Type      parquet.Type
	size      int64
	maxSize   int64
	full      bool
}

func NewDictRec(pT parquet.Type) *DictRecType {
	res := new(DictRecType)
	res.DictMap = make(map[any]int32)
	res.Type = pT
	return res
}

// NewDictRecWithLimit creates a size-limited dictionary recorder.
func NewDictRecWithLimit(pT parquet.Type, maxSize int64) *DictRecType {
	res := NewDictRec(pT)
	res.maxSize = maxSize
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
		return nil, 0, fmt.Errorf("compress dictionary page: %w", err)
	}
	if err = serializePage(page, opt, compressedData); err != nil {
		return nil, 0, fmt.Errorf("serialize dictionary page: %w", err)
	}
	totSize += int64(len(page.RawData))
	return page, totSize, nil
}

func (page *Page) dictPageCompress(compressType parquet.CompressionCodec, pT parquet.Type, c *compress.Compressor) ([]byte, error) {
	dataBuf, err := encoding.WritePlain(page.DataTable.Values, pT)
	if err != nil {
		return nil, fmt.Errorf("encode dictionary values: %w", err)
	}
	dataEncodeBuf, err := resolveCompressor(c).Compress(dataBuf, compressType)
	if err != nil {
		return nil, fmt.Errorf("compress dictionary buffer: %w", err)
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
	endIdx         int
	numValues      int32
	size           int32
	minVal         any
	maxVal         any
	nullCount      int64
	values         []int32
	dictionaryFull bool
}

// scanDictPageValues scans table values from startIdx to build one page, collecting dictionary indices and stats.
func scanDictPageValues(table *Table, dictRec *DictRecType, startIdx int, pageSize int32, omitStats bool, funcTable common.FuncTable) (dictPageValueResult, error) {
	totalLn := len(table.Values)
	r := dictPageValueResult{
		endIdx: startIdx,
		minVal: table.Values[startIdx],
		maxVal: table.Values[startIdx],
	}

	for r.endIdx < totalLn {
		// A data page may only end on a row boundary — a repetition-level-0
		// entry. A repeated column contributes a run of values per row (until
		// the next rep-0 entry); splitting that run across pages would leave a
		// page starting mid-row, which the offset index cannot address (its
		// first_row_index must name a row start and increase strictly). Once
		// the size budget is met, keep scanning to the next rep-0 entry before
		// ending the page. Flat columns have a rep-0 entry at every value, so
		// this never extends a page.
		if r.endIdx > startIdx && r.size >= pageSize && table.RepetitionLevels[r.endIdx] == 0 {
			break
		}
		if table.Values[r.endIdx] == nil {
			if err := checkRequiredNil(table, r.endIdx); err != nil {
				return r, fmt.Errorf("dictionary scan index %d: %w", r.endIdx, err)
			}
			r.nullCount++
			r.endIdx++
			continue
		}
		if table.DefinitionLevels[r.endIdx] == table.MaxDefinitionLevel {
			if err := checkFixedLenByteArrayWidth(table.Schema, table.Path, table.Values[r.endIdx]); err != nil {
				return r, err
			}
			r.numValues++
			var elSize int32
			if omitStats {
				_, _, elSize = funcTable.MinMaxSize(nil, nil, table.Values[r.endIdx])
			} else {
				r.minVal, r.maxVal, elSize = funcTable.MinMaxSize(r.minVal, r.maxVal, table.Values[r.endIdx])
			}
			r.size += elSize
			index, ok, err := dictRec.tryLookupOrInsert(table.Values[r.endIdx])
			if err != nil {
				return r, fmt.Errorf("measure dictionary value: %w", err)
			}
			if !ok {
				r.dictionaryFull = true
				return r, nil
			}
			r.values = append(r.values, index)
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

func (d *DictRecType) tryLookupOrInsert(val any) (int32, bool, error) {
	if idx, ok := d.DictMap[val]; ok {
		return idx, true, nil
	}
	encoded, err := encoding.WritePlain([]any{val}, d.Type)
	if err != nil {
		return 0, false, err
	}
	if d.maxSize > 0 && d.size+int64(len(encoded)) > d.maxSize {
		d.full = true
		return 0, false, nil
	}
	idx := d.lookupOrInsert(val)
	d.size += int64(len(encoded))
	return idx, true, nil
}

func (d *DictRecType) rollback(length int, size int64) {
	for _, value := range d.DictSlice[length:] {
		delete(d.DictMap, value)
	}
	d.DictSlice = d.DictSlice[:length]
	d.size = size
}
