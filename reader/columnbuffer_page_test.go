package reader

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/writer"
)

func tinyRunParquet(t *testing.T, count int32) []byte {
	t.Helper()
	run := append(encoding.WriteUnsignedVarInt(uint64(count)<<1), byte(1))
	body := append([]byte{byte(len(run)), 0, 0, 0}, run...)
	header := &parquet.PageHeader{Type: parquet.PageType_DATA_PAGE, CompressedPageSize: int32(len(body)), UncompressedPageSize: int32(len(body)), DataPageHeader: &parquet.DataPageHeader{NumValues: count, Encoding: parquet.Encoding_RLE}}
	mem := thrift.NewTMemoryBuffer()
	require.NoError(t, header.Write(context.Background(), thrift.NewTCompactProtocolConf(mem, nil)))
	page := append(bytes.Clone(mem.Bytes()), body...)
	footer := &parquet.FileMetaData{Version: 1, NumRows: int64(count), Schema: []*parquet.SchemaElement{
		{Name: "root", NumChildren: common.ToPtr(int32(1)), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
		{Name: "value", Type: common.ToPtr(parquet.Type_BOOLEAN), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
	}, RowGroups: []*parquet.RowGroup{{NumRows: int64(count), TotalByteSize: int64(len(page)), Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
		Type: parquet.Type_BOOLEAN, Encodings: []parquet.Encoding{parquet.Encoding_RLE}, PathInSchema: []string{"value"}, Codec: parquet.CompressionCodec_UNCOMPRESSED, NumValues: int64(count), DataPageOffset: 4, TotalCompressedSize: int64(len(page)), TotalUncompressedSize: int64(len(page)),
	}}}}}}
	footerMem := thrift.NewTMemoryBuffer()
	require.NoError(t, footer.Write(context.Background(), thrift.NewTCompactProtocolConf(footerMem, nil)))
	data := append([]byte("PAR1"), page...)
	data = append(data, footerMem.Bytes()...)
	data = binary.LittleEndian.AppendUint32(data, uint32(footerMem.Len()))
	return append(data, []byte("PAR1")...)
}

func TestColumnBufferHugeRunResumesAndSkips(t *testing.T) {
	type row struct {
		Value bool `parquet:"name=value, type=BOOLEAN"`
	}
	for _, count := range []int32{1<<20 - 1, 1<<20 + 1, 1<<31 - 1} {
		for _, column := range []bool{false, true} {
			t.Run(fmt.Sprintf("count=%d/column=%t", count, column), func(t *testing.T) {
				file := buffer.NewBufferReaderFromBytesNoAlloc(tinyRunParquet(t, count))
				var pr *ParquetReader
				var err error
				if column {
					pr, err = NewParquetColumnReaderWithContext(context.Background(), file, WithNP(1))
				} else {
					pr, err = NewParquetReaderWithContext(context.Background(), file, new(row), WithNP(1))
				}
				require.NoError(t, err)
				defer func() { require.NoError(t, pr.ReadStopWithContext(context.Background())) }()
				readOne := func() {
					if column {
						values, _, _, err := pr.ReadColumnByIndexWithContext(context.Background(), 0, 1)
						require.NoError(t, err)
						require.Equal(t, []any{true}, values)
					} else {
						rows := make([]row, 1)
						require.NoError(t, pr.ReadWithContext(context.Background(), &rows))
						require.Equal(t, []row{{Value: true}}, rows)
					}
				}
				readOne()
				require.NoError(t, pr.SkipRowsWithContext(context.Background(), int64(count)-3))
				readOne()
				readOne()
				values, _, _, err := pr.ReadColumnByIndexWithContext(context.Background(), 0, 1)
				require.NoError(t, err)
				require.Empty(t, values)
				for _, cb := range pr.ColumnBuffers {
					require.Nil(t, cb.pendingPage)
					require.LessOrEqual(t, len(cb.DataTable.Values), layout.PageBatchSize)
				}
			})
		}
	}
}

func TestColumnBufferBatchesPreserveRepeatedRows(t *testing.T) {
	type row struct {
		Values []int64 `parquet:"name=values, type=INT64, repetitiontype=REPEATED, encoding=PLAIN"`
		Name   *string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	}
	for _, version := range []int32{1, 2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			rows := make([]row, 19)
			for i := range rows {
				rows[i].Values = make([]int64, 1300+i)
				for j := range rows[i].Values {
					rows[i].Values[j] = int64(i*10000 + j)
				}
				if i%3 != 0 {
					rows[i].Name = common.ToPtr(fmt.Sprint(i))
				}
			}
			var data bytes.Buffer
			pw, err := writer.NewParquetWriterWithContext(context.Background(), writerfile.NewWriterFile(&data), new(row), writer.WithNP(1), writer.WithPageSize(1<<20), writer.WithDataPageVersion(version))
			require.NoError(t, err)
			for _, r := range rows {
				require.NoError(t, pw.WriteWithContext(context.Background(), r))
			}
			require.NoError(t, pw.WriteStopWithContext(context.Background()))
			pr, err := NewParquetReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytesNoAlloc(data.Bytes()), new(row), WithNP(1))
			require.NoError(t, err)
			defer func() { require.NoError(t, pr.ReadStopWithContext(context.Background())) }()
			for i := 0; i < len(rows); i++ {
				if i%3 == 1 {
					require.NoError(t, pr.SkipRowsWithContext(context.Background(), 1))
					continue
				}
				got := make([]row, 1)
				require.NoError(t, pr.ReadWithContext(context.Background(), &got))
				require.Equal(t, rows[i:i+1], got)
			}
		})
	}
}

func TestColumnBufferSkipDropsRepeatedRowContinuation(t *testing.T) {
	type row struct {
		Values []int64 `parquet:"name=values, type=INT64, repetitiontype=REPEATED"`
	}
	var data bytes.Buffer
	pw, err := writer.NewParquetWriterWithContext(context.Background(), writerfile.NewWriterFile(&data), new(row), writer.WithNP(1), writer.WithPageSize(1<<20))
	require.NoError(t, err)
	for i := int64(0); i < 4; i++ {
		values := make([]int64, 1301)
		for j := range values {
			values[j] = i
		}
		require.NoError(t, pw.WriteWithContext(context.Background(), row{Values: values}))
	}
	require.NoError(t, pw.WriteStopWithContext(context.Background()))
	pr, err := NewParquetColumnReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytesNoAlloc(data.Bytes()), WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStopWithContext(context.Background())) }()
	values, _, _, err := pr.ReadColumnByIndexWithContext(context.Background(), 0, 1)
	require.NoError(t, err)
	require.Len(t, values, 1301)
	require.NoError(t, pr.SkipRowsByIndexWithContext(context.Background(), 0, 1))
	values, _, _, err = pr.ReadColumnByIndexWithContext(context.Background(), 0, 1)
	require.NoError(t, err)
	require.Len(t, values, 1301)
	for _, v := range values {
		require.Equal(t, int64(2), v)
	}
}

func TestColumnBufferCancellationBetweenBatches(t *testing.T) {
	for _, skip := range []bool{false, true} {
		t.Run(fmt.Sprint(skip), func(t *testing.T) {
			pr, err := NewParquetColumnReaderWithContext(context.Background(), buffer.NewBufferReaderFromBytesNoAlloc(tinyRunParquet(t, 4096)), WithNP(1))
			require.NoError(t, err)
			defer func() { require.NoError(t, pr.ReadStopWithContext(context.Background())) }()
			_, _, _, err = pr.ReadColumnByIndexWithContext(context.Background(), 0, 1)
			require.NoError(t, err)
			for _, cb := range pr.ColumnBuffers {
				require.NotNil(t, cb.pendingPage)
				before := cb.ChunkReadValues
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				cb.PageReadOptions.Context = ctx
				if skip {
					_, err = cb.ReadPageForSkip()
				} else {
					err = cb.ReadPage()
				}
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, before, cb.ChunkReadValues)
			}
			values, _, _, err := pr.ReadColumnByIndexWithContext(context.Background(), 0, 1)
			require.NoError(t, err)
			require.Equal(t, []any{true}, values)
		})
	}
}
