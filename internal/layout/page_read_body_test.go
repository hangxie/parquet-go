package layout

import (
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/compress"
	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

func TestReadPageV2Data_InvalidLevels(t *testing.T) {
	header := &parquet.PageHeader{
		DataPageHeaderV2: &parquet.DataPageHeaderV2{
			DefinitionLevelsByteLength: -1,
		},
	}
	_, err := readPageV2Data(nil, header, nil, nil, PageReadOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid level byte lengths")

	header.DataPageHeaderV2.DefinitionLevelsByteLength = 10
	header.DataPageHeaderV2.RepetitionLevelsByteLength = 10
	header.CompressedPageSize = 15
	_, err = readPageV2Data(nil, header, nil, nil, PageReadOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "level byte lengths exceed page size")
}

func TestReadPageV2Data_Compressed(t *testing.T) {
	data := []byte{0x01, 0x00, 0x00, 0x00}
	compressed, _ := compress.DefaultCompressor().Compress(data, parquet.CompressionCodec_SNAPPY)

	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE_V2,
		CompressedPageSize:   int32(len(compressed)),
		UncompressedPageSize: int32(len(data)),
		DataPageHeaderV2: &parquet.DataPageHeaderV2{
			NumValues:                  1,
			Encoding:                   parquet.Encoding_PLAIN,
			IsCompressed:               true,
			DefinitionLevelsByteLength: 0,
			RepetitionLevelsByteLength: 0,
		},
	}

	colMetaData := &parquet.ColumnMetaData{
		Codec: parquet.CompressionCodec_SNAPPY,
	}

	mem := thrift.NewTMemoryBuffer()
	mem.Write(compressed)
	thriftReader := thrift.NewTBufferedTransport(mem, 1024)

	res, err := readPageV2Data(thriftReader, header, colMetaData, nil, PageReadOptions{})
	require.NoError(t, err)
	// res should be prefixed by 0, 0 (rll, dll) as they are 0
	require.Equal(t, data, res)
}

func TestAssembleLevelPrefixedBuf_WithRll(t *testing.T) {
	repBuf := []byte{0x01}
	defBuf := []byte{0x02}
	dataBuf := []byte{0x03}
	res, err := assembleLevelPrefixedBuf(1, 1, repBuf, defBuf, dataBuf)
	require.NoError(t, err)
	require.NotEmpty(t, res)
}

func TestAssembleLevelPrefixedBuf_NoLevels(t *testing.T) {
	dataBuf := []byte{0x0A, 0x0B, 0x0C}
	res, err := assembleLevelPrefixedBuf(0, 0, nil, nil, dataBuf)
	require.NoError(t, err)
	// With no levels, only the data is returned without length prefixes.
	require.Equal(t, dataBuf, res)
}

func TestReadPageV1Data_Compressed(t *testing.T) {
	data := []byte("some data to be compressed")
	compressed, _ := compress.DefaultCompressor().Compress(data, parquet.CompressionCodec_SNAPPY)

	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE,
		CompressedPageSize:   int32(len(compressed)),
		UncompressedPageSize: int32(len(data)),
		DataPageHeader: &parquet.DataPageHeader{
			NumValues: 1,
			Encoding:  parquet.Encoding_PLAIN,
		},
	}

	colMetaData := &parquet.ColumnMetaData{
		Codec: parquet.CompressionCodec_SNAPPY,
	}

	mem := thrift.NewTMemoryBuffer()
	mem.Write(compressed)
	thriftReader := thrift.NewTBufferedTransport(mem, 1024)

	res, err := readPageV1Data(thriftReader, header, colMetaData, nil, PageReadOptions{})
	require.NoError(t, err)
	require.Equal(t, data, res)
}

func TestReadPageV1Data_WithCRC(t *testing.T) {
	data := []byte("some data")
	crc := common.ComputePageCRC(data)

	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE,
		CompressedPageSize:   int32(len(data)),
		UncompressedPageSize: int32(len(data)),
		Crc:                  common.ToPtr(int32(crc)),
		DataPageHeader: &parquet.DataPageHeader{
			NumValues: 1,
			Encoding:  parquet.Encoding_PLAIN,
		},
	}

	colMetaData := &parquet.ColumnMetaData{
		Codec: parquet.CompressionCodec_UNCOMPRESSED,
	}

	mem := thrift.NewTMemoryBuffer()
	mem.Write(data)
	thriftReader := thrift.NewTBufferedTransport(mem, 1024)

	res, err := readPageV1Data(thriftReader, header, colMetaData, nil, PageReadOptions{CRCMode: common.CRCAuto})
	require.NoError(t, err)
	require.Equal(t, data, res)
}

func TestReadPageV1Data_ReadError(t *testing.T) {
	header := &parquet.PageHeader{
		CompressedPageSize: 10,
	}
	mem := thrift.NewTMemoryBuffer()
	mem.Write([]byte{1, 2, 3}) // less than 10
	thriftReader := thrift.NewTBufferedTransport(mem, 1024)

	_, err := readPageV1Data(thriftReader, header, nil, nil, PageReadOptions{})
	require.Error(t, err)
}

// leafPathName builds the InName path and joined name for a leaf column the same
// way the page reader does (root InName + capitalized column InName).
func leafPathName(sh *schema.SchemaHandler, colInName string) ([]string, string) {
	path := []string{sh.GetRootInName(), colInName}
	return path, common.PathToStr(path)
}

func TestReadDictionaryPageBody(t *testing.T) {
	schemaHandler := &schema.SchemaHandler{
		SchemaElements: []*parquet.SchemaElement{
			{Name: "dict_col", Type: common.ToPtr(parquet.Type_INT32)},
		},
		MapIndex: map[string]int32{"dict_col": 0},
	}
	colMetaData := &parquet.ColumnMetaData{Type: parquet.Type_INT32}

	pageHeader := &parquet.PageHeader{
		Type: parquet.PageType_DICTIONARY_PAGE,
		DictionaryPageHeader: &parquet.DictionaryPageHeader{
			NumValues: 2,
			Encoding:  parquet.Encoding_PLAIN,
		},
	}
	// Two PLAIN INT32 values: 100, 200
	buf := []byte{0x64, 0x00, 0x00, 0x00, 0xC8, 0x00, 0x00, 0x00}

	page, err := readDictionaryPageBody(pageHeader, buf, []string{"dict_col"}, "dict_col", schemaHandler, colMetaData)
	require.NoError(t, err)
	require.NotNil(t, page)
	require.Equal(t, []any{int32(100), int32(200)}, page.DataTable.Values)
	require.Equal(t, []string{"dict_col"}, page.DataTable.Path)
}

func TestReadDictionaryPageBody_FixedLenByteArray(t *testing.T) {
	schemaHandler := &schema.SchemaHandler{
		SchemaElements: []*parquet.SchemaElement{
			{Name: "flba_col", Type: common.ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), TypeLength: common.ToPtr(int32(2))},
		},
		MapIndex: map[string]int32{"flba_col": 0},
	}
	colMetaData := &parquet.ColumnMetaData{Type: parquet.Type_FIXED_LEN_BYTE_ARRAY}

	pageHeader := &parquet.PageHeader{
		Type: parquet.PageType_DICTIONARY_PAGE,
		DictionaryPageHeader: &parquet.DictionaryPageHeader{
			NumValues: 2,
			Encoding:  parquet.Encoding_PLAIN,
		},
	}
	// Two 2-byte fixed values: "ab", "cd"
	buf := []byte{'a', 'b', 'c', 'd'}

	page, err := readDictionaryPageBody(pageHeader, buf, []string{"flba_col"}, "flba_col", schemaHandler, colMetaData)
	require.NoError(t, err)
	require.Equal(t, []any{"ab", "cd"}, page.DataTable.Values)
}

func TestReadDictionaryPageBody_InvalidData(t *testing.T) {
	schemaHandler := &schema.SchemaHandler{
		SchemaElements: []*parquet.SchemaElement{
			{Name: "dict_col", Type: common.ToPtr(parquet.Type_INT32)},
		},
		MapIndex: map[string]int32{"dict_col": 0},
	}
	colMetaData := &parquet.ColumnMetaData{Type: parquet.Type_INT32}

	pageHeader := &parquet.PageHeader{
		Type: parquet.PageType_DICTIONARY_PAGE,
		DictionaryPageHeader: &parquet.DictionaryPageHeader{
			NumValues: 2,
			Encoding:  parquet.Encoding_PLAIN,
		},
	}
	// Only one int32 worth of bytes, but two values requested.
	buf := []byte{0x64, 0x00}

	_, err := readDictionaryPageBody(pageHeader, buf, []string{"dict_col"}, "dict_col", schemaHandler, colMetaData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode dictionary values")
}

func TestReadDataPageBody_Required(t *testing.T) {
	schemaElements := []*parquet.SchemaElement{
		{
			Name:           "parquet_go_root",
			NumChildren:    common.ToPtr(int32(1)),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "test_col",
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
	}
	schemaHandler := schema.NewSchemaHandlerFromSchemaList(schemaElements)
	colMetaData := &parquet.ColumnMetaData{Type: parquet.Type_INT32}

	pageHeader := &parquet.PageHeader{
		Type: parquet.PageType_DATA_PAGE,
		DataPageHeader: &parquet.DataPageHeader{
			NumValues: 2,
			Encoding:  parquet.Encoding_PLAIN,
		},
	}
	// REQUIRED column has maxDef/maxRep == 0, so no level bytes; just two int32s: 42, 84.
	buf := []byte{0x2A, 0x00, 0x00, 0x00, 0x54, 0x00, 0x00, 0x00}
	path, name := leafPathName(schemaHandler, "Test_col")

	page, numValues, numRows, err := readDataPageBody(pageHeader, buf, path, name, schemaHandler, colMetaData)
	require.NoError(t, err)
	require.Equal(t, int64(2), numValues)
	require.Equal(t, int64(2), numRows)
	require.Equal(t, []any{int32(42), int32(84)}, page.DataTable.Values)
	require.Equal(t, []int32{0, 0}, page.DataTable.DefinitionLevels)
	require.Equal(t, []int32{0, 0}, page.DataTable.RepetitionLevels)
}

func TestReadDataPageBody_V2Header(t *testing.T) {
	schemaElements := []*parquet.SchemaElement{
		{
			Name:           "parquet_go_root",
			NumChildren:    common.ToPtr(int32(1)),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "v2_col",
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
	}
	schemaHandler := schema.NewSchemaHandlerFromSchemaList(schemaElements)
	colMetaData := &parquet.ColumnMetaData{Type: parquet.Type_INT32}

	pageHeader := &parquet.PageHeader{
		Type: parquet.PageType_DATA_PAGE_V2,
		DataPageHeaderV2: &parquet.DataPageHeaderV2{
			NumValues: 1,
			NumRows:   1,
			Encoding:  parquet.Encoding_PLAIN,
		},
	}
	buf := []byte{0x07, 0x00, 0x00, 0x00} // int32(7)
	path, name := leafPathName(schemaHandler, "V2_col")

	page, numValues, numRows, err := readDataPageBody(pageHeader, buf, path, name, schemaHandler, colMetaData)
	require.NoError(t, err)
	require.Equal(t, int64(1), numValues)
	require.Equal(t, int64(1), numRows)
	require.Equal(t, []any{int32(7)}, page.DataTable.Values)
}

func TestReadDataPageBody_NilDataPageHeader(t *testing.T) {
	schemaHandler := schema.NewSchemaHandlerFromSchemaList([]*parquet.SchemaElement{
		{Name: "parquet_go_root", NumChildren: common.ToPtr(int32(1)), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
		{Name: "test_col", Type: common.ToPtr(parquet.Type_INT32), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
	})
	// Type says DATA_PAGE but DataPageHeader is absent (malformed page).
	pageHeader := &parquet.PageHeader{Type: parquet.PageType_DATA_PAGE}
	path, name := leafPathName(schemaHandler, "Test_col")
	_, _, _, err := readDataPageBody(pageHeader, []byte{}, path, name, schemaHandler, &parquet.ColumnMetaData{Type: parquet.Type_INT32})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing DataPageHeader")
}

func TestReadDataPageBody_NilDataPageHeaderV2(t *testing.T) {
	schemaHandler := schema.NewSchemaHandlerFromSchemaList([]*parquet.SchemaElement{
		{Name: "parquet_go_root", NumChildren: common.ToPtr(int32(1)), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
		{Name: "test_col", Type: common.ToPtr(parquet.Type_INT32), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
	})
	pageHeader := &parquet.PageHeader{Type: parquet.PageType_DATA_PAGE_V2}
	path, name := leafPathName(schemaHandler, "Test_col")
	_, _, _, err := readDataPageBody(pageHeader, []byte{}, path, name, schemaHandler, &parquet.ColumnMetaData{Type: parquet.Type_INT32})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing DataPageHeaderV2")
}

func TestReadDictionaryPageBody_NilHeader(t *testing.T) {
	schemaHandler := &schema.SchemaHandler{
		SchemaElements: []*parquet.SchemaElement{{Name: "dict_col", Type: common.ToPtr(parquet.Type_INT32)}},
		MapIndex:       map[string]int32{"dict_col": 0},
	}
	// Type says DICTIONARY_PAGE but DictionaryPageHeader is absent.
	pageHeader := &parquet.PageHeader{Type: parquet.PageType_DICTIONARY_PAGE}
	_, err := readDictionaryPageBody(pageHeader, []byte{}, []string{"dict_col"}, "dict_col", schemaHandler, &parquet.ColumnMetaData{Type: parquet.Type_INT32})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing DictionaryPageHeader")
}

func TestReadPageV2Data_NilHeader(t *testing.T) {
	pageHeader := &parquet.PageHeader{Type: parquet.PageType_DATA_PAGE_V2}
	_, err := readPageV2Data(nil, pageHeader, &parquet.ColumnMetaData{}, nil, PageReadOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing DataPageHeaderV2")
}

func TestReadPageV2Data_CRCFailure(t *testing.T) {
	data := []byte{0x01, 0x00, 0x00, 0x00}
	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE_V2,
		CompressedPageSize:   int32(len(data)),
		UncompressedPageSize: int32(len(data)),
		Crc:                  common.ToPtr(int32(0x12345678)), // wrong CRC
		DataPageHeaderV2: &parquet.DataPageHeaderV2{
			NumValues:                  1,
			Encoding:                   parquet.Encoding_PLAIN,
			IsCompressed:               false,
			DefinitionLevelsByteLength: 0,
			RepetitionLevelsByteLength: 0,
		},
	}
	colMetaData := &parquet.ColumnMetaData{Codec: parquet.CompressionCodec_UNCOMPRESSED}

	mem := thrift.NewTMemoryBuffer()
	mem.Write(data)
	thriftReader := thrift.NewTBufferedTransport(mem, 1024)

	_, err := readPageV2Data(thriftReader, header, colMetaData, nil, PageReadOptions{CRCMode: common.CRCStrict})
	require.Error(t, err)
	require.Contains(t, err.Error(), "CRC validation failed")
}

func TestReadPageV2Data_TruncatedDefinitionLevels(t *testing.T) {
	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE_V2,
		CompressedPageSize:   8,
		UncompressedPageSize: 8,
		DataPageHeaderV2: &parquet.DataPageHeaderV2{
			NumValues:                  1,
			Encoding:                   parquet.Encoding_PLAIN,
			RepetitionLevelsByteLength: 2,
			DefinitionLevelsByteLength: 2,
		},
	}
	colMetaData := &parquet.ColumnMetaData{Codec: parquet.CompressionCodec_UNCOMPRESSED}

	mem := thrift.NewTMemoryBuffer()
	mem.Write([]byte{0x01, 0x02}) // only the 2 repetition-level bytes
	thriftReader := thrift.NewTBufferedTransport(mem, 1024)

	_, err := readPageV2Data(thriftReader, header, colMetaData, nil, PageReadOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "definition levels")
}

func TestReadPageV2Data_TruncatedData(t *testing.T) {
	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE_V2,
		CompressedPageSize:   8,
		UncompressedPageSize: 8,
		DataPageHeaderV2: &parquet.DataPageHeaderV2{
			NumValues:                  1,
			Encoding:                   parquet.Encoding_PLAIN,
			RepetitionLevelsByteLength: 2,
			DefinitionLevelsByteLength: 2,
		},
	}
	colMetaData := &parquet.ColumnMetaData{Codec: parquet.CompressionCodec_UNCOMPRESSED}

	mem := thrift.NewTMemoryBuffer()
	mem.Write([]byte{0x01, 0x02, 0x03, 0x04}) // rep+def levels but no data bytes
	thriftReader := thrift.NewTBufferedTransport(mem, 1024)

	_, err := readPageV2Data(thriftReader, header, colMetaData, nil, PageReadOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "v2 data")
}

func TestReadPageV2Data_DecompressError(t *testing.T) {
	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE_V2,
		CompressedPageSize:   4,
		UncompressedPageSize: 4,
		DataPageHeaderV2: &parquet.DataPageHeaderV2{
			NumValues:                  1,
			Encoding:                   parquet.Encoding_PLAIN,
			IsCompressed:               true,
			RepetitionLevelsByteLength: 0,
			DefinitionLevelsByteLength: 0,
		},
	}
	colMetaData := &parquet.ColumnMetaData{Codec: parquet.CompressionCodec_SNAPPY}

	mem := thrift.NewTMemoryBuffer()
	mem.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}) // not valid snappy data
	thriftReader := thrift.NewTBufferedTransport(mem, 1024)

	_, err := readPageV2Data(thriftReader, header, colMetaData, nil, PageReadOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "decompress v2 data")
}

func TestReadPageV1Data_CRCFailure(t *testing.T) {
	data := []byte("some data")
	header := &parquet.PageHeader{
		Type:                 parquet.PageType_DATA_PAGE,
		CompressedPageSize:   int32(len(data)),
		UncompressedPageSize: int32(len(data)),
		Crc:                  common.ToPtr(int32(0x12345678)), // wrong CRC
		DataPageHeader: &parquet.DataPageHeader{
			NumValues: 1,
			Encoding:  parquet.Encoding_PLAIN,
		},
	}
	colMetaData := &parquet.ColumnMetaData{Codec: parquet.CompressionCodec_UNCOMPRESSED}

	mem := thrift.NewTMemoryBuffer()
	mem.Write(data)
	thriftReader := thrift.NewTBufferedTransport(mem, 1024)

	_, err := readPageV1Data(thriftReader, header, colMetaData, nil, PageReadOptions{CRCMode: common.CRCStrict})
	require.Error(t, err)
	require.Contains(t, err.Error(), "CRC validation failed")
}

func TestReadDataPageBody_WithNulls(t *testing.T) {
	schemaElements := []*parquet.SchemaElement{
		{
			Name:           "parquet_go_root",
			NumChildren:    common.ToPtr(int32(1)),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "opt_col",
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
		},
	}
	schemaHandler := schema.NewSchemaHandlerFromSchemaList(schemaElements)
	colMetaData := &parquet.ColumnMetaData{Type: parquet.Type_INT32}

	pageHeader := &parquet.PageHeader{
		Type: parquet.PageType_DATA_PAGE,
		DataPageHeader: &parquet.DataPageHeader{
			NumValues: 3,
			Encoding:  parquet.Encoding_PLAIN,
		},
	}

	// OPTIONAL column: maxDef=1, maxRep=0. Definition levels [1,0,1] => middle is null.
	defLevels, err := encoding.WriteRLEBitPackedHybrid([]any{int64(1), int64(0), int64(1)}, 1, parquet.Type_INT64)
	require.NoError(t, err)
	values, err := encoding.WritePlainINT32([]any{int32(11), int32(33)})
	require.NoError(t, err)
	buf := append(append([]byte(nil), defLevels...), values...)
	path, name := leafPathName(schemaHandler, "Opt_col")

	page, numValues, numRows, err := readDataPageBody(pageHeader, buf, path, name, schemaHandler, colMetaData)
	require.NoError(t, err)
	require.Equal(t, int64(3), numValues)
	require.Equal(t, int64(3), numRows)
	require.Equal(t, []any{int32(11), nil, int32(33)}, page.DataTable.Values)
	require.Equal(t, []int32{1, 0, 1}, page.DataTable.DefinitionLevels)
}

func TestReadDataPageBody_ConvertedType(t *testing.T) {
	schemaElements := []*parquet.SchemaElement{
		{
			Name:           "parquet_go_root",
			NumChildren:    common.ToPtr(int32(1)),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "ts_col",
			Type:           common.ToPtr(parquet.Type_INT32),
			ConvertedType:  common.ToPtr(parquet.ConvertedType_TIME_MILLIS),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
	}
	schemaHandler := schema.NewSchemaHandlerFromSchemaList(schemaElements)
	colMetaData := &parquet.ColumnMetaData{Type: parquet.Type_INT32}

	pageHeader := &parquet.PageHeader{
		Type: parquet.PageType_DATA_PAGE,
		DataPageHeader: &parquet.DataPageHeader{
			NumValues: 2,
			Encoding:  parquet.Encoding_PLAIN,
		},
	}
	buf := []byte{0x10, 0x27, 0x00, 0x00, 0x20, 0x4E, 0x00, 0x00} // int32 10000, 20000
	path, name := leafPathName(schemaHandler, "Ts_col")

	page, numValues, _, err := readDataPageBody(pageHeader, buf, path, name, schemaHandler, colMetaData)
	require.NoError(t, err)
	require.Equal(t, int64(2), numValues)
	require.Equal(t, []any{int32(10000), int32(20000)}, page.DataTable.Values)
}

func TestReadDataPageBody_DefinitionLevelError(t *testing.T) {
	schemaElements := []*parquet.SchemaElement{
		{
			Name:           "parquet_go_root",
			NumChildren:    common.ToPtr(int32(1)),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "opt_col",
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_OPTIONAL),
		},
	}
	schemaHandler := schema.NewSchemaHandlerFromSchemaList(schemaElements)
	colMetaData := &parquet.ColumnMetaData{Type: parquet.Type_INT32}

	pageHeader := &parquet.PageHeader{
		Type: parquet.PageType_DATA_PAGE,
		DataPageHeader: &parquet.DataPageHeader{
			NumValues: 3,
			Encoding:  parquet.Encoding_PLAIN,
		},
	}
	// OPTIONAL column needs RLE definition levels, but buffer is empty.
	path, name := leafPathName(schemaHandler, "Opt_col")
	_, _, _, err := readDataPageBody(pageHeader, []byte{}, path, name, schemaHandler, colMetaData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "definition levels")
}

func TestReadDataPageBody_ValuesError(t *testing.T) {
	schemaElements := []*parquet.SchemaElement{
		{
			Name:           "parquet_go_root",
			NumChildren:    common.ToPtr(int32(1)),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
		{
			Name:           "test_col",
			Type:           common.ToPtr(parquet.Type_INT32),
			RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED),
		},
	}
	schemaHandler := schema.NewSchemaHandlerFromSchemaList(schemaElements)
	colMetaData := &parquet.ColumnMetaData{Type: parquet.Type_INT32}

	pageHeader := &parquet.PageHeader{
		Type: parquet.PageType_DATA_PAGE,
		DataPageHeader: &parquet.DataPageHeader{
			NumValues: 2,
			Encoding:  parquet.Encoding_PLAIN,
		},
	}
	// Two values requested but only one int32 worth of bytes.
	buf := []byte{0x2A, 0x00, 0x00, 0x00}
	path, name := leafPathName(schemaHandler, "Test_col")

	_, _, _, err := readDataPageBody(pageHeader, buf, path, name, schemaHandler, colMetaData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read data page values")
}
