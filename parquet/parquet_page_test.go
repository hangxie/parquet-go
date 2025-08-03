package parquet

import (
	"context"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"
)

func Test_DataPageHeader(t *testing.T) {
	dph := NewDataPageHeader()
	require.NotNil(t, dph)

	numValues := int32(1000)
	dph.NumValues = numValues
	require.Equal(t, numValues, dph.GetNumValues())

	encoding := Encoding_PLAIN
	dph.Encoding = encoding
	require.Equal(t, encoding, dph.GetEncoding())

	defLevelEncoding := Encoding_RLE
	dph.DefinitionLevelEncoding = defLevelEncoding
	require.Equal(t, defLevelEncoding, dph.GetDefinitionLevelEncoding())

	repLevelEncoding := Encoding_BIT_PACKED
	dph.RepetitionLevelEncoding = repLevelEncoding
	require.Equal(t, repLevelEncoding, dph.GetRepetitionLevelEncoding())

	stats := NewStatistics()
	stats.Max = []byte("max")
	dph.Statistics = stats
	require.Equal(t, stats, dph.GetStatistics())
	require.True(t, dph.IsSetStatistics())

	str := dph.String()
	require.NotEmpty(t, str)

	dph2 := NewDataPageHeader()
	dph2.NumValues = numValues
	dph2.Encoding = encoding
	dph2.DefinitionLevelEncoding = defLevelEncoding
	dph2.RepetitionLevelEncoding = repLevelEncoding
	dph2.Statistics = stats
	require.True(t, dph.Equals(dph2))
}

func Test_IndexPageHeader(t *testing.T) {
	iph := NewIndexPageHeader()
	require.NotNil(t, iph)

	str := iph.String()
	require.NotEmpty(t, str)

	iph2 := NewIndexPageHeader()
	require.True(t, iph.Equals(iph2))
}

func Test_DictionaryPageHeader(t *testing.T) {
	dph := NewDictionaryPageHeader()
	require.NotNil(t, dph)

	numValues := int32(500)
	dph.NumValues = numValues
	require.Equal(t, numValues, dph.GetNumValues())

	encoding := Encoding_PLAIN_DICTIONARY
	dph.Encoding = encoding
	require.Equal(t, encoding, dph.GetEncoding())

	isSorted := true
	dph.IsSorted = &isSorted
	require.Equal(t, isSorted, dph.GetIsSorted())
	require.True(t, dph.IsSetIsSorted())

	str := dph.String()
	require.NotEmpty(t, str)

	dph2 := NewDictionaryPageHeader()
	dph2.NumValues = numValues
	dph2.Encoding = encoding
	dph2.IsSorted = &isSorted
	require.True(t, dph.Equals(dph2))
}

func Test_DataPageHeaderV2(t *testing.T) {
	dph := NewDataPageHeaderV2()
	require.NotNil(t, dph)

	numValues := int32(2000)
	dph.NumValues = numValues
	require.Equal(t, numValues, dph.GetNumValues())

	numNulls := int32(10)
	dph.NumNulls = numNulls
	require.Equal(t, numNulls, dph.GetNumNulls())

	numRows := int32(100)
	dph.NumRows = numRows
	require.Equal(t, numRows, dph.GetNumRows())

	encoding := Encoding_DELTA_BINARY_PACKED
	dph.Encoding = encoding
	require.Equal(t, encoding, dph.GetEncoding())

	defLevelsLen := int32(50)
	dph.DefinitionLevelsByteLength = defLevelsLen
	require.Equal(t, defLevelsLen, dph.GetDefinitionLevelsByteLength())

	repLevelsLen := int32(30)
	dph.RepetitionLevelsByteLength = repLevelsLen
	require.Equal(t, repLevelsLen, dph.GetRepetitionLevelsByteLength())

	isCompressed := false // Set to false since default is true, so IsSetIsCompressed() checks != default
	dph.IsCompressed = isCompressed
	require.Equal(t, isCompressed, dph.GetIsCompressed())
	require.True(t, dph.IsSetIsCompressed())

	stats := NewStatistics()
	stats.Min = []byte("min")
	dph.Statistics = stats
	require.Equal(t, stats, dph.GetStatistics())
	require.True(t, dph.IsSetStatistics())

	str := dph.String()
	require.NotEmpty(t, str)

	dph2 := NewDataPageHeaderV2()
	dph2.NumValues = numValues
	dph2.NumNulls = numNulls
	dph2.NumRows = numRows
	dph2.Encoding = encoding
	dph2.DefinitionLevelsByteLength = defLevelsLen
	dph2.RepetitionLevelsByteLength = repLevelsLen
	dph2.IsCompressed = isCompressed
	dph2.Statistics = stats
	require.True(t, dph.Equals(dph2))
}

func Test_DataPageHeaderEdgeCases(t *testing.T) {
	dph := NewDataPageHeader()
	require.False(t, dph.IsSetStatistics())
}

func Test_DictionaryPageHeaderEdgeCases(t *testing.T) {
	dph := NewDictionaryPageHeader()
	require.False(t, dph.IsSetIsSorted())
}

func Test_DataPageHeaderV2IsCompressed(t *testing.T) {
	dph := NewDataPageHeaderV2()

	dph.IsCompressed = true
	require.False(t, dph.IsSetIsCompressed())

	dph.IsCompressed = false
	require.True(t, dph.IsSetIsCompressed())
}

func Test_DataPageHeaderV2GettersEdgeCases(t *testing.T) {
	dph := NewDataPageHeaderV2()

	t.Log("Statistics is nil as expected")
	require.Nil(t, dph.GetStatistics())
	require.False(t, dph.IsSetStatistics())
}

func Test_DataPageHeader_GetStatistics_EdgeCase(t *testing.T) {
	dph := NewDataPageHeader()
	dph.Statistics = nil

	result := dph.GetStatistics()
	require.Nil(t, result)
}

func Test_DictionaryPageHeader_GetIsSorted_EdgeCase(t *testing.T) {
	dph := NewDictionaryPageHeader()
	dph.IsSorted = nil

	result := dph.GetIsSorted()
	require.False(t, result)
}

func Test_PageHeader_GetType(t *testing.T) {
	ph := NewPageHeader()
	require.NotNil(t, ph)

	pageType := PageType_DATA_PAGE
	ph.Type = pageType
	require.Equal(t, pageType, ph.GetType())

	uncompressedSize := int32(2048)
	ph.UncompressedPageSize = uncompressedSize
	require.Equal(t, uncompressedSize, ph.GetUncompressedPageSize())

	compressedSize := int32(1024)
	ph.CompressedPageSize = compressedSize
	require.Equal(t, compressedSize, ph.GetCompressedPageSize())

	crc := int32(12345)
	ph.Crc = &crc
	require.Equal(t, crc, ph.GetCrc())
	require.True(t, ph.IsSetCrc())

	str := ph.String()
	require.NotEmpty(t, str)
	require.Contains(t, str, "PageHeader")
}

func Test_PageHeader_GetPageHeaders(t *testing.T) {
	ph := NewPageHeader()
	require.NotNil(t, ph)

	// Test DataPageHeader
	dataPageHeader := NewDataPageHeader()
	dataPageHeader.NumValues = 1000
	ph.DataPageHeader = dataPageHeader
	require.Equal(t, dataPageHeader, ph.GetDataPageHeader())
	require.True(t, ph.IsSetDataPageHeader())

	// Test IndexPageHeader
	ph2 := NewPageHeader()
	indexPageHeader := NewIndexPageHeader()
	ph2.IndexPageHeader = indexPageHeader
	require.Equal(t, indexPageHeader, ph2.GetIndexPageHeader())
	require.True(t, ph2.IsSetIndexPageHeader())

	// Test DictionaryPageHeader
	ph3 := NewPageHeader()
	dictPageHeader := NewDictionaryPageHeader()
	dictPageHeader.NumValues = 500
	ph3.DictionaryPageHeader = dictPageHeader
	require.Equal(t, dictPageHeader, ph3.GetDictionaryPageHeader())
	require.True(t, ph3.IsSetDictionaryPageHeader())

	// Test DataPageHeaderV2
	ph4 := NewPageHeader()
	dataPageHeaderV2 := NewDataPageHeaderV2()
	dataPageHeaderV2.NumValues = 2000
	ph4.DataPageHeaderV2 = dataPageHeaderV2
	require.Equal(t, dataPageHeaderV2, ph4.GetDataPageHeaderV2())
	require.True(t, ph4.IsSetDataPageHeaderV2())
}

func Test_PageHeader_Equals(t *testing.T) {
	ph1 := NewPageHeader()
	ph1.Type = PageType_DATA_PAGE
	ph1.UncompressedPageSize = 1024
	ph1.CompressedPageSize = 512

	ph2 := NewPageHeader()
	ph2.Type = PageType_DATA_PAGE
	ph2.UncompressedPageSize = 1024
	ph2.CompressedPageSize = 512

	require.True(t, ph1.Equals(ph2))

	ph3 := NewPageHeader()
	ph3.Type = PageType_INDEX_PAGE
	ph3.UncompressedPageSize = 1024
	ph3.CompressedPageSize = 512

	require.False(t, ph1.Equals(ph3))
}

func Test_PageHeaderThriftReadWrite(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		setupPage  func() *PageHeader
		verifyPage func(*testing.T, *PageHeader)
	}{
		{
			"DATA_PAGE with statistics",
			func() *PageHeader {
				pageHeader := NewPageHeader()
				pageHeader.Type = PageType_DATA_PAGE
				pageHeader.UncompressedPageSize = 1024
				pageHeader.CompressedPageSize = 512
				pageHeader.Crc = thrift.Int32Ptr(12345)

				dataPageHeader := NewDataPageHeader()
				dataPageHeader.NumValues = 100
				dataPageHeader.Encoding = Encoding_PLAIN
				dataPageHeader.DefinitionLevelEncoding = Encoding_RLE
				dataPageHeader.RepetitionLevelEncoding = Encoding_BIT_PACKED
				stats := NewStatistics()
				stats.Max = []byte("max")
				stats.Min = []byte("min")
				dataPageHeader.Statistics = stats
				pageHeader.DataPageHeader = dataPageHeader
				return pageHeader
			},
			func(t *testing.T, ph *PageHeader) {
				require.Equal(t, PageType_DATA_PAGE, ph.Type)
				require.NotNil(t, ph.DataPageHeader)
				require.Equal(t, int32(100), ph.DataPageHeader.NumValues)
				require.Equal(t, Encoding_PLAIN, ph.DataPageHeader.Encoding)
			},
		},
		{
			"INDEX_PAGE",
			func() *PageHeader {
				pageHeader := NewPageHeader()
				pageHeader.Type = PageType_INDEX_PAGE
				pageHeader.UncompressedPageSize = 2048
				pageHeader.CompressedPageSize = 1024
				indexPageHeader := NewIndexPageHeader()
				pageHeader.IndexPageHeader = indexPageHeader
				return pageHeader
			},
			func(t *testing.T, ph *PageHeader) {
				require.Equal(t, PageType_INDEX_PAGE, ph.Type)
				require.NotNil(t, ph.IndexPageHeader)
			},
		},
		{
			"DICTIONARY_PAGE",
			func() *PageHeader {
				pageHeader := NewPageHeader()
				pageHeader.Type = PageType_DICTIONARY_PAGE
				pageHeader.UncompressedPageSize = 4096
				pageHeader.CompressedPageSize = 2048
				dictPageHeader := NewDictionaryPageHeader()
				dictPageHeader.NumValues = 50
				dictPageHeader.Encoding = Encoding_RLE_DICTIONARY
				dictPageHeader.IsSorted = thrift.BoolPtr(true)
				pageHeader.DictionaryPageHeader = dictPageHeader
				return pageHeader
			},
			func(t *testing.T, ph *PageHeader) {
				require.Equal(t, PageType_DICTIONARY_PAGE, ph.Type)
				require.NotNil(t, ph.DictionaryPageHeader)
				require.Equal(t, int32(50), ph.DictionaryPageHeader.NumValues)
				require.Equal(t, Encoding_RLE_DICTIONARY, ph.DictionaryPageHeader.Encoding)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			transport := thrift.NewTMemoryBuffer()
			protocol := thrift.NewTBinaryProtocolConf(transport, nil)

			originalPage := tt.setupPage()
			require.NoError(t, originalPage.Write(ctx, protocol))

			readTransport := thrift.NewTMemoryBuffer()
			readTransport.Buffer = transport.Buffer
			readProtocol := thrift.NewTBinaryProtocolConf(readTransport, nil)

			newPageHeader := NewPageHeader()
			require.NoError(t, newPageHeader.Read(ctx, readProtocol))

			tt.verifyPage(t, newPageHeader)
		})
	}
}
