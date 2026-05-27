package writer

import (
	"context"
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func (pw *ParquetWriter) WriteStop() error {
	if pw.stopped {
		return nil
	}
	pw.stopped = true

	var err error
	if err = pw.Flush(true); err != nil {
		return fmt.Errorf("flush before stop: %w", err)
	}
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	pw.RenameSchema()

	if err = pw.writeColumnIndexes(ts); err != nil {
		return fmt.Errorf("write column indexes: %w", err)
	}

	if err = pw.writeOffsetIndexes(ts); err != nil {
		return fmt.Errorf("write offset indexes: %w", err)
	}

	if err = pw.writeBloomFilters(); err != nil {
		return fmt.Errorf("write bloom filters: %w", err)
	}

	// Set ColumnOrders so readers can correctly interpret min/max statistics.
	// One TYPE_ORDER entry per leaf column, per the Parquet spec.
	if pw.SchemaHandler != nil {
		numCols := pw.SchemaHandler.GetColumnNum()
		pw.Footer.ColumnOrders = make([]*parquet.ColumnOrder, numCols)
		for i := range numCols {
			pw.Footer.ColumnOrders[i] = &parquet.ColumnOrder{
				TYPE_ORDER: parquet.NewTypeDefinedOrder(),
			}
		}
	}

	if pw.encryptionState != nil {
		if pw.encryptionState.plaintextFooter {
			pw.Footer.EncryptionAlgorithm = pw.encryptionState.parquetAlgorithm()
			pw.Footer.FooterSigningKeyMetadata = append([]byte(nil), pw.encryptionState.footerKeyMetadata...)
		}
		for rowGroupIndex, rowGroup := range pw.Footer.RowGroups {
			rowGroupOrdinal := int16(rowGroupIndex)
			if rowGroup.IsSetOrdinal() {
				rowGroupOrdinal = rowGroup.GetOrdinal()
			}
			for columnOrdinal, column := range rowGroup.Columns {
				if err := pw.encryptColumnMetadata(rowGroupOrdinal, int16(columnOrdinal), column); err != nil {
					return fmt.Errorf("encrypt column metadata row group %d column %d: %w", rowGroupOrdinal, columnOrdinal, err)
				}
			}
		}
	}

	footerBuf, err := ts.Write(context.TODO(), pw.Footer)
	if err != nil {
		return fmt.Errorf("serialize footer: %w", err)
	}
	footerBuf, tailMagic, err := pw.encryptFooter(footerBuf)
	if err != nil {
		return fmt.Errorf("encrypt footer: %w", err)
	}

	if _, err = pw.PFile.Write(footerBuf); err != nil {
		return fmt.Errorf("write footer: %w", err)
	}
	footerSizeBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerSizeBuf, uint32(len(footerBuf)))

	if _, err = pw.PFile.Write(footerSizeBuf); err != nil {
		return fmt.Errorf("write footer size: %w", err)
	}
	if _, err = pw.PFile.Write([]byte(tailMagic)); err != nil {
		return fmt.Errorf("write magic tail: %w", err)
	}

	return nil
}

// Write writes one object to the parquet file.
func (pw *ParquetWriter) Write(src any) error {
	if pw.stopped {
		return fmt.Errorf("writer stopped")
	}
	if pw.SchemaHandler == nil {
		return fmt.Errorf("schema handler not initialized")
	}

	// Validate encodings are compatible with data page version (once per writer)
	if !pw.encodingsValidated {
		if err := pw.SchemaHandler.ValidateEncodingsForDataPageVersion(pw.dataPageVersion); err != nil {
			return fmt.Errorf("encoding validation: %w", err)
		}
		pw.encodingsValidated = true
	}

	var err error
	ln := int64(len(pw.objs))

	val := reflect.ValueOf(src)
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
		src = val.Interface()
	}

	if pw.checkSizeCritical <= ln {
		pw.objSize = (pw.objSize+common.SizeOf(val))/2 + 1
	}
	pw.objsSize += pw.objSize
	pw.objs = append(pw.objs, src)

	criSize := pw.np * pw.pageSize * pw.SchemaHandler.GetColumnNum()

	if pw.objsSize >= criSize {
		err = pw.Flush(false)
	} else {
		dln := (criSize - pw.objsSize + pw.objSize - 1) / pw.objSize / 2
		pw.checkSizeCritical = dln + ln
	}
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	return nil
}

func (pw *ParquetWriter) buildChunkMap() (map[string]*layout.Chunk, error) {
	chunkMap := make(map[string]*layout.Chunk)
	for name, pages := range pw.pagesMapBuf {
		compressionType := pw.compressionType
		if idx, ok := pw.SchemaHandler.MapIndex[name]; ok {
			if info := pw.SchemaHandler.Infos[idx]; info != nil && info.CompressionCodec != nil {
				compressionType = *info.CompressionCodec
			}
		}

		if len(pages) > 0 && (pages[0].Info.Encoding == parquet.Encoding_PLAIN_DICTIONARY || pages[0].Info.Encoding == parquet.Encoding_RLE_DICTIONARY) {
			v, ok := pw.DictRecs.Load(name)
			if !ok {
				return nil, fmt.Errorf("missing dictionary recorder for column %s", name)
			}
			dictRec := v.(*layout.DictRecType)
			dictPage, _, err := layout.DictRecToDictPageWithOption(dictRec, layout.PageWriteOption{
				PageSize:     int32(pw.pageSize),
				CompressType: compressionType,
				WriteCRC:     pw.writeCRC,
			})
			if err != nil {
				return nil, fmt.Errorf("convert dict rec to dict page for column %s: %w", name, err)
			}
			tmp := append([]*layout.Page{dictPage}, pages...)
			var chunkErr error
			chunkMap[name], chunkErr = layout.PagesToDictChunk(tmp)
			if chunkErr != nil {
				return nil, fmt.Errorf("convert pages to dict chunk for column %s: %w", name, chunkErr)
			}
		} else {
			var err error
			chunkMap[name], err = layout.PagesToChunk(pages)
			if err != nil {
				return nil, fmt.Errorf("convert pages to chunk for column %s: %w", name, err)
			}
		}
	}
	return chunkMap, nil
}

func (pw *ParquetWriter) buildRowGroup(chunkMap map[string]*layout.Chunk) *layout.RowGroup {
	rowGroup := layout.NewRowGroup()
	rowGroup.RowGroupHeader.Columns = make([]*parquet.ColumnChunk, 0)

	for k := range len(pw.SchemaHandler.SchemaElements) {
		se := pw.SchemaHandler.SchemaElements[k]
		if se.GetNumChildren() > 0 {
			continue
		}
		chunk := chunkMap[pw.SchemaHandler.IndexMap[int32(k)]]
		if chunk == nil {
			continue
		}
		rowGroup.Chunks = append(rowGroup.Chunks, chunk)
		rowGroup.RowGroupHeader.TotalByteSize += chunk.ChunkHeader.MetaData.TotalUncompressedSize
		rowGroup.RowGroupHeader.Columns = append(rowGroup.RowGroupHeader.Columns, chunk.ChunkHeader)
	}
	rowGroup.RowGroupHeader.NumRows = pw.numRows
	pw.numRows = 0
	return rowGroup
}

// Flush the write buffer to parquet file
func (pw *ParquetWriter) Flush(flag bool) error {
	if err := pw.flushObjs(); err != nil {
		return fmt.Errorf("flush objects during flush: %w", err)
	}

	if (pw.size+pw.objsSize >= pw.rowGroupSize || flag) && len(pw.pagesMapBuf) > 0 {
		chunkMap, err := pw.buildChunkMap()
		if err != nil {
			return fmt.Errorf("build chunk map: %w", err)
		}
		pw.DictRecs.Clear()

		rowGroup := pw.buildRowGroup(chunkMap)

		rowGroupOrdinal := int16(len(pw.Footer.RowGroups))
		for k := range len(rowGroup.Chunks) {
			if err := pw.writeChunkPages(rowGroup.Chunks[k], rowGroupOrdinal, int16(k)); err != nil {
				return fmt.Errorf("write chunk pages row group %d column %d: %w", rowGroupOrdinal, k, err)
			}
		}

		pw.Footer.RowGroups = append(pw.Footer.RowGroups, rowGroup.RowGroupHeader)

		if err := pw.serializeBloomFilters(); err != nil {
			return fmt.Errorf("serialize bloom filters: %w", err)
		}

		pw.size = 0
		pw.pagesMapBuf = make(map[string][]*layout.Page)
	}
	pw.Footer.NumRows += int64(len(pw.objs))
	pw.objs = pw.objs[:0]
	pw.objsSize = 0
	return nil
}
