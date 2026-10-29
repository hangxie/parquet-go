package layout

import (
	"bytes"
	"fmt"

	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

// extractV2LevelBuffers reassembles a V2 data page into level-length prefixes followed by value data.
func (p *Page) extractV2LevelBuffers(maxRepetitionLevel, maxDefinitionLevel int32) ([]byte, error) {
	// The schema's max levels decide which sections get a prefix, matching what
	// decodeDataPageLevels reads back; writers pad the length even at level 0.
	dll := p.Header.DataPageHeaderV2.GetDefinitionLevelsByteLength()
	rll := p.Header.DataPageHeaderV2.GetRepetitionLevelsByteLength()

	if dll < 0 || rll < 0 {
		return nil, fmt.Errorf("GetRLDLFromRawData: invalid level byte lengths (dll=%d, rll=%d)", dll, rll)
	}
	// Widened before summing: two positive int32 lengths can wrap negative, slipping
	// past this check and reaching make() below with a negative length.
	levelsLen := int64(dll) + int64(rll)
	rawDataLen := int64(len(p.RawData))
	if levelsLen > rawDataLen {
		return nil, fmt.Errorf("GetRLDLFromRawData: level byte lengths exceed raw data size (dll=%d + rll=%d > %d)", dll, rll, rawDataLen)
	}
	if err := validateV2LevelSections(rll, dll, maxRepetitionLevel, maxDefinitionLevel,
		p.Header.DataPageHeaderV2.GetNumValues()); err != nil {
		return nil, err
	}

	bytesReader := bytes.NewReader(p.RawData)
	repetitionLevelsBuf, definitionLevelsBuf := make([]byte, rll), make([]byte, dll)
	dataBuf := make([]byte, rawDataLen-levelsLen)
	if _, err := bytesReader.Read(repetitionLevelsBuf); err != nil {
		return nil, fmt.Errorf("read v2 repetition levels: %w", err)
	}
	if _, err := bytesReader.Read(definitionLevelsBuf); err != nil {
		return nil, fmt.Errorf("read v2 definition levels: %w", err)
	}
	if _, err := bytesReader.Read(dataBuf); err != nil {
		return nil, fmt.Errorf("read v2 data: %w", err)
	}

	if p.Header.DataPageHeaderV2.GetIsCompressed() && len(dataBuf) > 0 {
		expected := int64(p.Header.GetUncompressedPageSize()) - levelsLen
		var err error
		dataBuf, err = resolveCompressor(p.compressor).UncompressWithExpectedSize(dataBuf, p.CompressType, expected)
		if err != nil {
			return nil, fmt.Errorf("uncompress data page v2: %w", err)
		}
	}
	if err := resolveCompressor(p.compressor).CheckSize(levelsLen + int64(len(dataBuf))); err != nil {
		return nil, fmt.Errorf("read v2 data: %w", err)
	}

	buf := make([]byte, 0)
	if rll > 0 && maxRepetitionLevel > 0 {
		tmpBuf, err := encoding.WritePlainINT32([]any{int32(rll)})
		if err != nil {
			return nil, fmt.Errorf("encode repetition level length: %w", err)
		}
		buf = append(buf, tmpBuf...)
		buf = append(buf, repetitionLevelsBuf...)
	}
	if dll > 0 && maxDefinitionLevel > 0 {
		tmpBuf, err := encoding.WritePlainINT32([]any{int32(dll)})
		if err != nil {
			return nil, fmt.Errorf("encode definition level length: %w", err)
		}
		buf = append(buf, tmpBuf...)
		buf = append(buf, definitionLevelsBuf...)
	}
	buf = append(buf, dataBuf...)
	return buf, nil
}

// Get RepetitionLevels and Definitions from RawData
func (p *Page) GetRLDLFromRawData(schemaHandler *schema.SchemaHandler) (int64, int64, error) {
	if p.decoder != nil {
		return p.ReadBatch(false)
	}
	var buf []byte
	var err error

	if p.Header.GetType() == parquet.PageType_DATA_PAGE_V2 {
		maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(p.Path)
		maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(p.Path)
		buf, err = p.extractV2LevelBuffers(maxRepetitionLevel, maxDefinitionLevel)
		if err != nil {
			return 0, 0, fmt.Errorf("extract v2 level buffers: %w", err)
		}
	} else if p.CompressType != parquet.CompressionCodec_UNCOMPRESSED {
		buf, err = resolveCompressor(p.compressor).UncompressWithExpectedSize(p.RawData, p.CompressType, int64(p.Header.GetUncompressedPageSize()))
		if err != nil {
			return 0, 0, fmt.Errorf("uncompress data page: %w", err)
		}
	} else {
		if err := resolveCompressor(p.compressor).CheckSize(int64(len(p.RawData))); err != nil {
			return 0, 0, fmt.Errorf("read data page: %w", err)
		}
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
	if err := p.initDecoder(buf, schemaHandler, p.chunkValues); err != nil {
		return 0, 0, err
	}
	p.RawData = nil
	return p.ReadBatch(false)
}
