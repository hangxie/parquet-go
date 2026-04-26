package writer

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
)

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

func TestExtractPageStats(t *testing.T) {
	nullCount := int64(3)

	tests := []struct {
		name string
		page *layout.Page
		want pageStats
	}{
		{
			name: "data_page_v1",
			page: &layout.Page{
				Header: &parquet.PageHeader{
					DataPageHeader: &parquet.DataPageHeader{
						Statistics: &parquet.Statistics{
							Min:       []byte("a"),
							Max:       []byte("z"),
							NullCount: &nullCount,
						},
					},
				},
			},
			want: pageStats{minVal: []byte("a"), maxVal: []byte("z"), nullCount: &nullCount},
		},
		{
			name: "data_page_v2",
			page: &layout.Page{
				Header: &parquet.PageHeader{
					DataPageHeaderV2: &parquet.DataPageHeaderV2{
						Statistics: &parquet.Statistics{
							Min:       []byte("b"),
							Max:       []byte("y"),
							NullCount: &nullCount,
						},
					},
				},
			},
			want: pageStats{minVal: []byte("b"), maxVal: []byte("y"), nullCount: &nullCount},
		},
		{
			name: "no_stats",
			page: &layout.Page{Header: &parquet.PageHeader{}},
			want: pageStats{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractPageStats(tt.page)
			require.Equal(t, tt.want.minVal, got.minVal)
			require.Equal(t, tt.want.maxVal, got.maxVal)
			if tt.want.nullCount == nil {
				require.Nil(t, got.nullCount)
			} else {
				require.NotNil(t, got.nullCount)
				require.Equal(t, *tt.want.nullCount, *got.nullCount)
			}
		})
	}
}

func TestDataPageNumValues(t *testing.T) {
	tests := []struct {
		name string
		page *layout.Page
		want int64
	}{
		{
			name: "data_page_v1",
			page: &layout.Page{Header: &parquet.PageHeader{
				DataPageHeader: &parquet.DataPageHeader{NumValues: 7},
			}},
			want: 7,
		},
		{
			name: "data_page_v2",
			page: &layout.Page{Header: &parquet.PageHeader{
				DataPageHeaderV2: &parquet.DataPageHeaderV2{NumValues: 11},
			}},
			want: 11,
		},
		{
			name: "not_data_page",
			page: &layout.Page{Header: &parquet.PageHeader{}},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, dataPageNumValues(tt.page))
		})
	}
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
