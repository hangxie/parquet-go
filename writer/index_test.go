package writer

import (
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestWriteOffsetIndexes(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		pw := &ParquetWriter{}
		ts := thrift.NewTSerializer()

		require.NoError(t, pw.writeOffsetIndexes(ts))
	})

	t.Run("write_error", func(t *testing.T) {
		pw := &ParquetWriter{
			PFile: &invalidFileWriter{},
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{
					{
						Columns: []*parquet.ColumnChunk{
							parquet.NewColumnChunk(),
						},
					},
				},
			},
			offsetIndexes: []*parquet.OffsetIndex{
				parquet.NewOffsetIndex(),
			},
		}
		ts := thrift.NewTSerializer()
		ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)

		err := pw.writeOffsetIndexes(ts)
		require.ErrorIs(t, err, errWrite)
		require.Contains(t, err.Error(), "write offset index")
	})
}

func TestWriteColumnIndexes(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		pw := &ParquetWriter{}
		ts := thrift.NewTSerializer()

		require.NoError(t, pw.writeColumnIndexes(ts))
	})

	t.Run("write_error", func(t *testing.T) {
		pw := &ParquetWriter{
			PFile: &invalidFileWriter{},
			Footer: &parquet.FileMetaData{
				RowGroups: []*parquet.RowGroup{
					{
						Columns: []*parquet.ColumnChunk{
							parquet.NewColumnChunk(),
						},
					},
				},
			},
			columnIndexes: []*parquet.ColumnIndex{
				parquet.NewColumnIndex(),
			},
		}
		ts := thrift.NewTSerializer()
		ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)

		err := pw.writeColumnIndexes(ts)
		require.ErrorIs(t, err, errWrite)
		require.Contains(t, err.Error(), "write column index")
	})
}
