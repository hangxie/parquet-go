package layout

import (
	"fmt"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

// extractV2LevelBuffers splits a V2 data page into length-prefixed levels and decompressed values.
func (p *Page) extractV2LevelBuffers(maxRepetitionLevel, maxDefinitionLevel int32) ([]byte, []byte, error) {
	// The schema's max levels decide which sections get a prefix, matching what
	// decodeDataPageLevels reads back; writers pad the length even at level 0.
	dll := p.Header.DataPageHeaderV2.GetDefinitionLevelsByteLength()
	rll := p.Header.DataPageHeaderV2.GetRepetitionLevelsByteLength()

	if dll < 0 || rll < 0 {
		return nil, nil, fmt.Errorf("GetRLDLFromRawData: invalid level byte lengths (dll=%d, rll=%d)", dll, rll)
	}
	// Widened before summing: two positive int32 lengths can wrap negative, slipping
	// past this check and slicing RawData out of range below.
	levelsLen := int64(dll) + int64(rll)
	rawDataLen := int64(len(p.RawData))
	if levelsLen > rawDataLen {
		return nil, nil, fmt.Errorf("GetRLDLFromRawData: level byte lengths exceed raw data size (dll=%d + rll=%d > %d)", dll, rll, rawDataLen)
	}
	if err := validateV2LevelSections(rll, dll, maxRepetitionLevel, maxDefinitionLevel,
		p.Header.DataPageHeaderV2.GetNumValues()); err != nil {
		return nil, nil, err
	}

	repetitionLevelsBuf, definitionLevelsBuf, dataBuf := p.RawData[:rll], p.RawData[rll:levelsLen], p.RawData[levelsLen:]

	if p.Header.DataPageHeaderV2.GetIsCompressed() && len(dataBuf) > 0 {
		expected := int64(p.Header.GetUncompressedPageSize()) - levelsLen
		var err error
		dataBuf, err = resolveCompressor(p.compressor).UncompressWithExpectedSize(dataBuf, p.CompressType, expected)
		if err != nil {
			return nil, nil, fmt.Errorf("uncompress data page v2: %w", err)
		}
	}
	if err := resolveCompressor(p.compressor).CheckSize(levelsLen + int64(len(dataBuf))); err != nil {
		return nil, nil, fmt.Errorf("read v2 data: %w", err)
	}

	levels, err := prefixV2Levels(rll, dll, repetitionLevelsBuf, definitionLevelsBuf, maxRepetitionLevel, maxDefinitionLevel)
	if err != nil {
		return nil, nil, err
	}
	return levels, dataBuf, nil
}

// Get RepetitionLevels and Definitions from RawData
func (p *Page) GetRLDLFromRawData(schemaHandler *schema.SchemaHandler) (int64, int64, error) {
	if p.decoder != nil {
		return p.ReadBatch(false)
	}
	var buf, values []byte
	var err error

	if p.Header.GetType() == parquet.PageType_DATA_PAGE_V2 {
		maxDefinitionLevel, _ := schemaHandler.MaxDefinitionLevel(p.Path)
		maxRepetitionLevel, _ := schemaHandler.MaxRepetitionLevel(p.Path)
		buf, values, err = p.extractV2LevelBuffers(maxRepetitionLevel, maxDefinitionLevel)
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
		return p.decodeDataPageLevels(buf, values, schemaHandler)
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

func (p *Page) decodeDataPageLevels(buf, values []byte, schemaHandler *schema.SchemaHandler) (int64, int64, error) {
	if err := p.initDecoder(buf, values, schemaHandler, p.chunkValues); err != nil {
		return 0, 0, err
	}
	p.RawData = nil
	return p.ReadBatch(false)
}
