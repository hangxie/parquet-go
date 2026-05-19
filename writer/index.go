package writer

import (
	"context"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/internal/encryption"
)

// Write the footer and stop writing
func (pw *ParquetWriter) writeColumnIndexes(ts *thrift.TSerializer) error {
	if len(pw.columnIndexes) == 0 {
		return nil
	}
	idx := 0
	for rowGroupIndex, rowGroup := range pw.Footer.RowGroups {
		for columnOrdinal, columnChunk := range rowGroup.Columns {
			columnIndexBuf, err := ts.Write(context.TODO(), pw.columnIndexes[idx])
			if err != nil {
				return fmt.Errorf("serialize column index: %w", err)
			}
			if pw.encryptionState != nil && columnChunk.GetCryptoMetadata() != nil {
				key, err := pw.keyForEncryptedColumn(columnChunk)
				if err != nil {
					return err
				}
				rowGroupOrdinal := int16(rowGroupIndex)
				if rowGroup.IsSetOrdinal() {
					rowGroupOrdinal = rowGroup.GetOrdinal()
				}
				module, err := pw.encryptThriftModule(key, encryption.ModuleColumnIndex, rowGroupOrdinal, int16(columnOrdinal), columnIndexBuf)
				if err != nil {
					return err
				}
				columnIndexBuf = module
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
	for rowGroupIndex, rowGroup := range pw.Footer.RowGroups {
		for columnOrdinal, columnChunk := range rowGroup.Columns {
			offsetIndexBuf, err := ts.Write(context.TODO(), pw.offsetIndexes[idx])
			if err != nil {
				return fmt.Errorf("serialize offset index: %w", err)
			}
			if pw.encryptionState != nil && columnChunk.GetCryptoMetadata() != nil {
				key, err := pw.keyForEncryptedColumn(columnChunk)
				if err != nil {
					return err
				}
				rowGroupOrdinal := int16(rowGroupIndex)
				if rowGroup.IsSetOrdinal() {
					rowGroupOrdinal = rowGroup.GetOrdinal()
				}
				module, err := pw.encryptThriftModule(key, encryption.ModuleOffsetIndex, rowGroupOrdinal, int16(columnOrdinal), offsetIndexBuf)
				if err != nil {
					return err
				}
				offsetIndexBuf = module
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
