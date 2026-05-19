package writer

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

func TestColumnOrders(t *testing.T) {
	t.Run("struct_writer", func(t *testing.T) {
		type Entry struct {
			Name  string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
			Age   int32  `parquet:"name=age, type=INT32"`
			Score int64  `parquet:"name=score, type=INT64"`
		}

		pw, buf, err := createTestParquetWriter(new(Entry), WithNP(1))
		require.NoError(t, err)

		require.NoError(t, pw.Write(Entry{Name: "alice", Age: 30, Score: 100}))
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(Entry), reader.WithNP(1))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, pf.Close())
		}()

		require.NotNil(t, pr.Footer.ColumnOrders, "ColumnOrders should be set in footer")
		require.Equal(t, 3, len(pr.Footer.ColumnOrders), "one ColumnOrder per leaf column")
		for i, co := range pr.Footer.ColumnOrders {
			require.NotNil(t, co.TYPE_ORDER, "ColumnOrder[%d] should have TYPE_ORDER set", i)
		}
	})

	t.Run("zero_rows", func(t *testing.T) {
		type Entry struct {
			X int32  `parquet:"name=x, type=INT32"`
			Y string `parquet:"name=y, type=BYTE_ARRAY, convertedtype=UTF8"`
		}

		pw, buf, err := createTestParquetWriter(new(Entry), WithNP(1))
		require.NoError(t, err)

		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(Entry), reader.WithNP(1))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, pf.Close())
		}()

		require.NotNil(t, pr.Footer.ColumnOrders)
		require.Equal(t, 2, len(pr.Footer.ColumnOrders))
	})

	t.Run("nested_schema", func(t *testing.T) {
		type Inner struct {
			A int32 `parquet:"name=a, type=INT32"`
			B int32 `parquet:"name=b, type=INT32"`
		}
		type Outer struct {
			Name  string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
			Inner Inner  `parquet:"name=inner"`
		}

		pw, buf, err := createTestParquetWriter(new(Outer), WithNP(1))
		require.NoError(t, err)

		require.NoError(t, pw.Write(Outer{Name: "test", Inner: Inner{A: 1, B: 2}}))
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(Outer), reader.WithNP(1))
		require.NoError(t, err)
		defer func() {
			require.NoError(t, pf.Close())
		}()

		// 3 leaf columns: name, inner.a, inner.b
		require.NotNil(t, pr.Footer.ColumnOrders)
		require.Equal(t, 3, len(pr.Footer.ColumnOrders))
	})

	t.Run("csv_writer", func(t *testing.T) {
		md := []string{
			"name=name, type=BYTE_ARRAY, convertedtype=UTF8",
			"name=age, type=INT32",
		}

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		cw, err := NewCSVWriter(md, fw, WithNP(1))
		require.NoError(t, err)

		name := "alice"
		age := "30"
		require.NoError(t, cw.WriteString([]*string{&name, &age}))
		require.NoError(t, cw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		defer func() {
			require.NoError(t, pf.Close())
		}()
		pr, err := reader.NewParquetReader(pf, nil, reader.WithNP(1))
		require.NoError(t, err)

		require.NotNil(t, pr.Footer.ColumnOrders)
		require.Equal(t, 2, len(pr.Footer.ColumnOrders))
	})

	t.Run("json_writer", func(t *testing.T) {
		jsonSchema := `{
			"Tag": "name=parquet-go-root",
			"Fields": [
				{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"},
				{"Tag": "name=age, type=INT32"}
			]
		}`

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		jw, err := NewJSONWriter(jsonSchema, fw, WithNP(1))
		require.NoError(t, err)

		require.NoError(t, jw.Write(`{"name":"alice","age":30}`))
		require.NoError(t, jw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		defer func() {
			require.NoError(t, pf.Close())
		}()
		pr, err := reader.NewParquetReader(pf, nil, reader.WithNP(1))
		require.NoError(t, err)

		require.NotNil(t, pr.Footer.ColumnOrders)
		require.Equal(t, 2, len(pr.Footer.ColumnOrders))
	})
}

func TestWrite_AfterStop(t *testing.T) {
	type SimpleStruct struct {
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8"`
	}

	pw, _, err := createTestParquetWriter(new(SimpleStruct), WithNP(1))
	require.NoError(t, err)

	require.NoError(t, pw.Write(SimpleStruct{Name: "test"}))
	require.NoError(t, pw.WriteStop())

	// Writing after stop should return error
	err = pw.Write(SimpleStruct{Name: "after_stop"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "stopped")
}

func TestWrite_NilSchemaHandler(t *testing.T) {
	var buf bytes.Buffer
	pw, err := NewParquetWriterFromWriter(&buf, nil, WithNP(1))
	require.NoError(t, err)
	require.NotNil(t, pw)

	// Write should return an error, not panic, when SchemaHandler is nil
	err = pw.Write("some data")
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema handler not initialized")
}

// TestNewParquetWriter_SchemaHandlerInput covers the *schema.SchemaHandler branch.

func TestWrite_PointerInput(t *testing.T) {
	type S struct {
		ID int32 `parquet:"name=id, type=INT32"`
	}
	pw, _, err := createTestParquetWriter(new(S), WithNP(1))
	require.NoError(t, err)

	val := S{ID: 99}
	// Pass a pointer — Write should dereference it.
	require.NoError(t, pw.Write(&val))
	require.NoError(t, pw.WriteStop())
}

// TestWriteStop_FooterWriteError covers the "write footer" error path in WriteStop.
// Uses a writer that succeeds only the first write (PAR1 magic) and fails all subsequent writes.

func TestWriteStop_FooterWriteError(t *testing.T) {
	type S struct {
		ID int32 `parquet:"name=id, type=INT32"`
	}
	fw := &firstSuccessThenFail{}
	pw, err := NewParquetWriter(fw, new(S), WithNP(1))
	require.NoError(t, err)

	err = pw.WriteStop()
	require.Error(t, err)
	require.Contains(t, err.Error(), "write footer")
}

// TestWriteStop_ColumnIndexWriteError covers the writeColumnIndexes write error path.
// Writes data to populate column indexes, then replaces PFile with a failing writer.

func TestWriteStop_ColumnIndexWriteError(t *testing.T) {
	type S struct {
		ID int32 `parquet:"name=id, type=INT32"`
	}
	pw, _, err := createTestParquetWriter(new(S), WithNP(1))
	require.NoError(t, err)

	require.NoError(t, pw.Write(S{ID: 1}))
	// Flush to populate column indexes, then replace PFile so WriteStop IO fails.
	require.NoError(t, pw.Flush(true))
	pw.PFile = &invalidFileWriter{}

	err = pw.WriteStop()
	require.Error(t, err)
}

type failOnWriteN struct {
	failOn int
	count  int
}

func (w *failOnWriteN) Write(data []byte) (int, error) {
	w.count++
	if w.count == w.failOn {
		return 0, errWrite
	}
	return len(data), nil
}

func (w *failOnWriteN) Close() error { return nil }

func (w *failOnWriteN) Create(string) (source.ParquetFileWriter, error) {
	return w, nil
}

func TestBuildChunkMapMissingDictionaryRecorder(t *testing.T) {
	tag, err := common.StringToTag("name=dict_col, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY")
	require.NoError(t, err)

	pw := &ParquetWriter{
		SchemaHandler: &schema.SchemaHandler{
			MapIndex: map[string]int32{},
		},
		pagesMapBuf: map[string][]*layout.Page{
			"dict_col": {
				{
					Info: tag,
				},
			},
		},
	}

	chunkMap, err := pw.buildChunkMap()
	require.Error(t, err)
	require.Nil(t, chunkMap)
	require.Contains(t, err.Error(), "missing dictionary recorder")
}

func TestBuildChunkMapPagesToChunkError(t *testing.T) {
	pw := &ParquetWriter{
		SchemaHandler: &schema.SchemaHandler{
			MapIndex: map[string]int32{},
		},
		pagesMapBuf: map[string][]*layout.Page{
			"empty_col": {},
		},
	}

	chunkMap, err := pw.buildChunkMap()
	require.Error(t, err)
	require.Nil(t, chunkMap)
	require.Contains(t, err.Error(), "convert pages to chunk")
}

func TestFlushBuildChunkMapError(t *testing.T) {
	tag, err := common.StringToTag("name=dict_col, type=BYTE_ARRAY, encoding=PLAIN_DICTIONARY")
	require.NoError(t, err)
	tag.Encoding = parquet.Encoding_RLE_DICTIONARY

	pw := &ParquetWriter{
		SchemaHandler: &schema.SchemaHandler{
			MapIndex: map[string]int32{},
		},
		pagesMapBuf: map[string][]*layout.Page{
			"dict_col": {
				{
					Info: tag,
				},
			},
		},
	}

	err = pw.Flush(true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing dictionary recorder")
}

func TestWriteStopOffsetIndexWriteError(t *testing.T) {
	pw := &ParquetWriter{
		PFile: &invalidFileWriter{},
		SchemaHandler: &schema.SchemaHandler{
			InPathToExPath: map[string]string{
				"id": "root" + common.ParGoPathDelimiter + "id",
			},
		},
		Footer: &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{
					Columns: []*parquet.ColumnChunk{
						{
							MetaData: &parquet.ColumnMetaData{
								PathInSchema: []string{"id"},
							},
						},
					},
				},
			},
		},
		offsetIndexes: []*parquet.OffsetIndex{
			parquet.NewOffsetIndex(),
		},
	}

	err := pw.WriteStop()
	require.ErrorIs(t, err, errWrite)
	require.Contains(t, err.Error(), "write offset index")
}

func TestWriteStopBloomFilterWriteError(t *testing.T) {
	pw := &ParquetWriter{
		PFile: &invalidFileWriter{},
		SchemaHandler: &schema.SchemaHandler{
			InPathToExPath: map[string]string{
				"id": "root" + common.ParGoPathDelimiter + "id",
			},
		},
		Footer: &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{
					Columns: []*parquet.ColumnChunk{
						{
							MetaData: &parquet.ColumnMetaData{
								PathInSchema: []string{"id"},
							},
						},
					},
				},
			},
		},
		bloomFilterData: [][]byte{[]byte("bloom-data")},
	}

	err := pw.WriteStop()
	require.ErrorIs(t, err, errWrite)
	require.Contains(t, err.Error(), "write bloom filter")
}

func TestWriteStopTailWriteErrors(t *testing.T) {
	tests := []struct {
		name        string
		failOnWrite int
		want        string
	}{
		{
			name:        "footer_size",
			failOnWrite: 3, // PAR1 during init, footer, then footer size
			want:        "write footer size",
		},
		{
			name:        "magic_tail",
			failOnWrite: 4, // PAR1 during init, footer, footer size, then PAR1 tail
			want:        "write magic tail",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			type Entry struct {
				ID int32 `parquet:"name=id, type=INT32"`
			}

			fw := &failOnWriteN{failOn: tt.failOnWrite}
			pw, err := NewParquetWriter(fw, new(Entry), WithNP(1))
			require.NoError(t, err)

			err = pw.WriteStop()
			require.Error(t, err)
			require.True(t, errors.Is(err, errWrite), "expected %v to wrap %v", err, errWrite)
			require.Contains(t, err.Error(), tt.want)
		})
	}
}
