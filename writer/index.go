package writer

import (
	"context"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
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
