package layout

import (
	"bytes"
	"fmt"
	"math/bits"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

// extractV2LevelBuffers extracts the level data from a V2 data page and reassembles it into
// a contiguous buffer with level-length prefixes followed by value data.
func (p *Page) extractV2LevelBuffers() ([]byte, error) {
	dll := p.Header.DataPageHeaderV2.GetDefinitionLevelsByteLength()
	rll := p.Header.DataPageHeaderV2.GetRepetitionLevelsByteLength()

	if dll < 0 || rll < 0 {
		return nil, fmt.Errorf("GetRLDLFromRawData: invalid level byte lengths (dll=%d, rll=%d)", dll, rll)
	}
	rawDataLen := int32(len(p.RawData))
	if dll+rll > rawDataLen {
		return nil, fmt.Errorf("GetRLDLFromRawData: level byte lengths exceed raw data size (dll=%d + rll=%d > %d)", dll, rll, rawDataLen)
	}

	bytesReader := bytes.NewReader(p.RawData)
	repetitionLevelsBuf, definitionLevelsBuf := make([]byte, rll), make([]byte, dll)
	dataBuf := make([]byte, len(p.RawData)-int(rll)-int(dll))
	if _, err := bytesReader.Read(repetitionLevelsBuf); err != nil {
		return nil, err
	}
	if _, err := bytesReader.Read(definitionLevelsBuf); err != nil {
		return nil, err
	}
	if _, err := bytesReader.Read(dataBuf); err != nil {
		return nil, err
	}

	buf := make([]byte, 0)
	if rll > 0 {
		tmpBuf, err := encoding.WritePlainINT32([]any{int32(rll)})
		if err != nil {
			return nil, err
		}
		buf = append(buf, tmpBuf...)
		buf = append(buf, repetitionLevelsBuf...)
	}
	if dll > 0 {
		tmpBuf, err := encoding.WritePlainINT32([]any{int32(dll)})
		if err != nil {
			return nil, err
		}
		buf = append(buf, tmpBuf...)
		buf = append(buf, definitionLevelsBuf...)
	}
	buf = append(buf, dataBuf...)
	return buf, nil
}

// readLevelValues reads either repetition or definition level values from a reader.
func readLevelValues(bytesReader *bytes.Reader, maxLevel int32, numValues uint64) ([]any, error) {
	if maxLevel > 0 {
		bitWidth := uint64(bits.Len32(uint32(maxLevel)))
		levels, err := ReadDataPageValues(bytesReader, parquet.Encoding_RLE, parquet.Type_INT64, -1, numValues, bitWidth)
		if err != nil {
			return nil, err
		}
		if uint64(len(levels)) > numValues {
			levels = levels[:numValues]
		}
		return levels, nil
	}
	levels := make([]any, numValues)
	for i := range len(levels) {
		levels[i] = int64(0)
	}
	return levels, nil
}

// Get RepetitionLevels and Definitions from RawData
func (p *Page) GetRLDLFromRawData(schemaHandler *schema.SchemaHandler) (int64, int64, error) {
	var buf []byte
	var err error

	if p.Header.GetType() == parquet.PageType_DATA_PAGE_V2 {
		buf, err = p.extractV2LevelBuffers()
		if err != nil {
			return 0, 0, err
		}
	} else if p.CompressType != parquet.CompressionCodec_UNCOMPRESSED {
		buf, err = resolveCompressor(p.compressor).UncompressWithExpectedSize(p.RawData, p.CompressType, int64(p.Header.GetUncompressedPageSize()))
		if err != nil {
			return 0, 0, fmt.Errorf("uncompress data page: %w", err)
		}
	} else {
		buf = p.RawData
	}

	switch p.Header.GetType() {
	case parquet.PageType_DATA_PAGE, parquet.PageType_DATA_PAGE_V2:
		return p.decodeDataPageLevels(buf, schemaHandler)
	case parquet.PageType_DICTIONARY_PAGE:
		table := new(Table)
		table.Path = p.Path
		p.DataTable = table
		p.RawData = buf
		return 0, 0, nil
	default:
		return 0, 0, fmt.Errorf("unsupported page type: %v", p.Header.GetType())
	}
}

func (p *Page) decodeDataPageLevels(buf []byte, schemaHandler *schema.SchemaHandler) (int64, int64, error) {
	var numValues uint64
	if p.Header.GetType() == parquet.PageType_DATA_PAGE {
		numValues = uint64(p.Header.DataPageHeader.GetNumValues())
	} else {
		numValues = uint64(p.Header.DataPageHeaderV2.GetNumValues())
	}

	maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(p.Path)
	maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(p.Path)

	bytesReader := bytes.NewReader(buf)
	repetitionLevels, err := readLevelValues(bytesReader, maxRepetitionLevel, numValues)
	if err != nil {
		return 0, 0, err
	}
	definitionLevels, err := readLevelValues(bytesReader, maxDefinitionLevel, numValues)
	if err != nil {
		return 0, 0, err
	}

	table := new(Table)
	table.Path = p.Path
	name := common.PathToStr(p.Path)
	table.RepetitionType = schemaHandler.SchemaElements[schemaHandler.MapIndex[name]].GetRepetitionType()
	table.MaxRepetitionLevel = maxRepetitionLevel
	table.MaxDefinitionLevel = maxDefinitionLevel
	table.Values = make([]any, len(definitionLevels))
	table.RepetitionLevels = make([]int32, len(definitionLevels))
	table.DefinitionLevels = make([]int32, len(definitionLevels))

	numRows := int64(0)
	for i := range len(definitionLevels) {
		dl, _ := definitionLevels[i].(int64)
		rl, _ := repetitionLevels[i].(int64)
		table.RepetitionLevels[i] = int32(rl)
		table.DefinitionLevels[i] = int32(dl)
		if table.RepetitionLevels[i] == 0 {
			numRows++
		}
	}
	p.DataTable = table
	p.RawData = buf[len(buf)-bytesReader.Len():]

	return int64(numValues), numRows, nil
}
