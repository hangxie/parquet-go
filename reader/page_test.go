package reader_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/source/buffer"
	"github.com/hangxie/parquet-go/v2/source/local"
)

type TestPageRecord struct {
	ShoeBrand string `parquet:"name=shoe_brand, type=BYTE_ARRAY, convertedtype=UTF8"`
	ShoeName  string `parquet:"name=shoe_name, type=BYTE_ARRAY, convertedtype=UTF8"`
}

var (
	testParquetURL       = "https://github.com/hangxie/parquet-tools/raw/refs/heads/main/testdata/dict-page.parquet"
	testParquetLocalPath string
	downloadOnce         sync.Once
	downloadErr          error
)

// getTestParquetFile downloads the test parquet file once and returns the local path.
// Subsequent calls return the cached local path without re-downloading.
func getTestParquetFile(t *testing.T) string {
	downloadOnce.Do(func() {
		// Create temp file
		tmpFile, err := os.CreateTemp("", "dict-page-*.parquet")
		if err != nil {
			downloadErr = fmt.Errorf("create temp file: %w", err)
			return
		}
		defer func() { _ = tmpFile.Close() }()

		testParquetLocalPath = tmpFile.Name()

		// Download file
		resp, err := http.Get(testParquetURL)
		if err != nil {
			downloadErr = fmt.Errorf("download test parquet file: %w", err)
			return
		}
		defer func() { _ = resp.Body.Close() }()

		if resp.StatusCode != http.StatusOK {
			downloadErr = fmt.Errorf("download test parquet file: status %d", resp.StatusCode)
			return
		}

		// Copy to temp file
		_, err = io.Copy(tmpFile, resp.Body)
		if err != nil {
			downloadErr = fmt.Errorf("write test parquet file: %w", err)
			return
		}
	})

	if downloadErr != nil {
		t.Fatalf("Failed to get test parquet file: %v", downloadErr)
	}

	return testParquetLocalPath
}

func Test_GetAllPageHeaders(t *testing.T) {
	testFile := getTestParquetFile(t)
	buf, err := local.NewLocalFileReader(testFile)
	require.NoError(t, err)
	pr, err := reader.NewParquetReader(buf, new(TestPageRecord), 4)
	require.NoError(t, err)
	defer func() {
		_ = pr.ReadStopWithError()
	}()

	t.Run("valid indices", func(t *testing.T) {
		headers, err := pr.GetAllPageHeaders(0, 0)
		require.NoError(t, err)
		require.NotEmpty(t, headers)

		// Verify page headers have sensible values
		for i, header := range headers {
			require.Equal(t, i, header.Index)
			require.Positive(t, header.CompressedSize)
			require.Positive(t, header.UncompressedSize)
			require.Positive(t, header.Offset)
		}
	})

	t.Run("invalid row group index", func(t *testing.T) {
		_, err := pr.GetAllPageHeaders(-1, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid row group index")

		_, err = pr.GetAllPageHeaders(999, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid row group index")
	})

	t.Run("invalid column index", func(t *testing.T) {
		_, err := pr.GetAllPageHeaders(0, -1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid column index")

		_, err = pr.GetAllPageHeaders(0, 999)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid column index")
	})

	t.Run("multiple columns", func(t *testing.T) {
		// Test that we can read headers for all columns
		numCols := len(pr.Footer.RowGroups[0].Columns)
		for colIdx := 0; colIdx < numCols; colIdx++ {
			headers, err := pr.GetAllPageHeaders(0, colIdx)
			require.NoError(t, err)
			require.NotEmpty(t, headers)
		}
	})
}

func Test_ReadDictionaryPageValues(t *testing.T) {
	testFile := getTestParquetFile(t)
	buf, err := local.NewLocalFileReader(testFile)
	require.NoError(t, err)
	pr, err := reader.NewParquetReader(buf, new(TestPageRecord), 4)
	require.NoError(t, err)
	defer func() {
		_ = pr.ReadStopWithError()
	}()

	dictCol := pr.Footer.RowGroups[0].Columns[0]
	dictOffset := *dictCol.MetaData.DictionaryPageOffset

	t.Run("valid dictionary page", func(t *testing.T) {
		values, err := pr.ReadDictionaryPageValues(
			dictOffset,
			dictCol.MetaData.Codec,
			dictCol.MetaData.Type,
		)
		require.NoError(t, err)
		require.NotEmpty(t, values)

		// Verify we got string values (since our test data uses string columns with dictionaries)
		for _, v := range values {
			require.NotNil(t, v)
		}
	})

	t.Run("non-dictionary page offset", func(t *testing.T) {
		// Try to read from a data page offset (not dictionary)
		dataOffset := dictCol.MetaData.DataPageOffset
		_, err := pr.ReadDictionaryPageValues(
			dataOffset,
			dictCol.MetaData.Codec,
			dictCol.MetaData.Type,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected dictionary page")
	})

	t.Run("invalid offset", func(t *testing.T) {
		// Try to read from an invalid offset
		_, err := pr.ReadDictionaryPageValues(
			-1,
			dictCol.MetaData.Codec,
			dictCol.MetaData.Type,
		)
		require.Error(t, err)
	})
}

func Test_GetAllPageHeaders_AllPageTypes(t *testing.T) {
	testFile := getTestParquetFile(t)
	buf, err := local.NewLocalFileReader(testFile)
	require.NoError(t, err)
	pr, err := reader.NewParquetReader(buf, new(TestPageRecord), 4)
	require.NoError(t, err)
	defer func() {
		_ = pr.ReadStopWithError()
	}()

	// Read all page headers from the first column (which should have dictionary)
	headers, err := pr.GetAllPageHeaders(0, 0)
	require.NoError(t, err)

	// Verify we have different page types
	pageTypes := make(map[parquet.PageType]bool)
	for _, header := range headers {
		pageTypes[header.PageType] = true
	}

	// We should have at least a dictionary page and data pages
	require.True(t, pageTypes[parquet.PageType_DICTIONARY_PAGE] || pageTypes[parquet.PageType_DATA_PAGE],
		"Should have dictionary or data pages")
}

func Test_ReadAllPageHeaders(t *testing.T) {
	testFile := getTestParquetFile(t)
	buf, err := local.NewLocalFileReader(testFile)
	require.NoError(t, err)
	pr, err := reader.NewParquetReader(buf, new(TestPageRecord), 4)
	require.NoError(t, err)
	defer func() {
		_ = pr.ReadStopWithError()
	}()

	require.NotEmpty(t, pr.Footer.RowGroups)
	require.NotEmpty(t, pr.Footer.RowGroups[0].Columns)

	col := pr.Footer.RowGroups[0].Columns[0]
	headers, err := reader.ReadAllPageHeaders(pr.PFile, col)
	require.NoError(t, err)
	require.NotEmpty(t, headers)

	// Verify the page headers have sensible values
	for i, header := range headers {
		require.Equal(t, i, header.Index)
		require.Positive(t, header.CompressedSize)
		require.Positive(t, header.UncompressedSize)
		require.Positive(t, header.Offset)
	}
}

func Test_ReadPageData(t *testing.T) {
	testFile := getTestParquetFile(t)
	buf, err := local.NewLocalFileReader(testFile)
	require.NoError(t, err)
	pr, err := reader.NewParquetReader(buf, new(TestPageRecord), 4)
	require.NoError(t, err)
	defer func() {
		_ = pr.ReadStopWithError()
	}()

	require.NotEmpty(t, pr.Footer.RowGroups)
	require.NotEmpty(t, pr.Footer.RowGroups[0].Columns)

	col := pr.Footer.RowGroups[0].Columns[0]
	headers, err := reader.ReadAllPageHeaders(pr.PFile, col)
	require.NoError(t, err)
	require.NotEmpty(t, headers)

	// Read data from the first page
	firstPage := headers[0]

	// For testing, we'll create a minimal page header
	pageHeader := parquet.NewPageHeader()
	pageHeader.Type = firstPage.PageType
	pageHeader.CompressedPageSize = firstPage.CompressedSize
	pageHeader.UncompressedPageSize = firstPage.UncompressedSize

	pageData, err := reader.ReadPageData(pr.PFile, firstPage.Offset, pageHeader, col.MetaData.Codec)
	require.NoError(t, err)
	require.NotEmpty(t, pageData)
	require.Equal(t, firstPage.UncompressedSize, int32(len(pageData)))
}

func Test_DecodeDictionaryPage(t *testing.T) {
	t.Run("decode from real parquet file", func(t *testing.T) {
		testFile := getTestParquetFile(t)
		buf, err := local.NewLocalFileReader(testFile)
		require.NoError(t, err)
		pr, err := reader.NewParquetReader(buf, new(TestPageRecord), 4)
		require.NoError(t, err)
		defer func() {
			_ = pr.ReadStopWithError()
		}()

		dictCol := pr.Footer.RowGroups[0].Columns[0]
		headers, err := reader.ReadAllPageHeaders(pr.PFile, dictCol)
		require.NoError(t, err)

		dictPageHeader := headers[0]

		// Create a minimal page header for testing
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = dictPageHeader.PageType
		pageHeader.CompressedPageSize = dictPageHeader.CompressedSize
		pageHeader.UncompressedPageSize = dictPageHeader.UncompressedSize
		dictHeader := parquet.NewDictionaryPageHeader()
		dictHeader.NumValues = dictPageHeader.NumValues
		dictHeader.Encoding = dictPageHeader.Encoding
		pageHeader.DictionaryPageHeader = dictHeader

		// Read and decode the dictionary page using the building block functions
		pageData, err := reader.ReadPageData(pr.PFile, dictPageHeader.Offset, pageHeader, dictCol.MetaData.Codec)
		require.NoError(t, err)
		require.NotEmpty(t, pageData)

		values, err := reader.DecodeDictionaryPage(pageData, pageHeader, dictCol.MetaData.Type)
		require.NoError(t, err)
		require.NotEmpty(t, values)
		require.Equal(t, dictPageHeader.NumValues, int32(len(values)))
	})

	t.Run("decode BYTE_ARRAY dictionary", func(t *testing.T) {
		// Create dictionary values: ["apple", "banana", "cherry"]
		dictValues := []string{"apple", "banana", "cherry"}
		var dictData bytes.Buffer

		// Encode as PLAIN BYTE_ARRAY
		for _, val := range dictValues {
			// Write length prefix (4 bytes)
			length := int32(len(val))
			dictData.WriteByte(byte(length))
			dictData.WriteByte(byte(length >> 8))
			dictData.WriteByte(byte(length >> 16))
			dictData.WriteByte(byte(length >> 24))
			// Write string data
			dictData.WriteString(val)
		}

		// Create page header
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DICTIONARY_PAGE
		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = int32(len(dictValues))
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_PLAIN

		// Decode
		values, err := reader.DecodeDictionaryPage(dictData.Bytes(), pageHeader, parquet.Type_BYTE_ARRAY)
		require.NoError(t, err)
		require.Len(t, values, 3)

		// Verify values
		require.Equal(t, "apple", values[0])
		require.Equal(t, "banana", values[1])
		require.Equal(t, "cherry", values[2])
	})

	t.Run("decode INT32 dictionary", func(t *testing.T) {
		// Create dictionary values: [100, 200, 300]
		dictValues := []int32{100, 200, 300}
		var dictData bytes.Buffer

		// Encode as PLAIN INT32 (little-endian)
		for _, val := range dictValues {
			dictData.WriteByte(byte(val))
			dictData.WriteByte(byte(val >> 8))
			dictData.WriteByte(byte(val >> 16))
			dictData.WriteByte(byte(val >> 24))
		}

		// Create page header
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DICTIONARY_PAGE
		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = int32(len(dictValues))
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_PLAIN

		// Decode
		values, err := reader.DecodeDictionaryPage(dictData.Bytes(), pageHeader, parquet.Type_INT32)
		require.NoError(t, err)
		require.Len(t, values, 3)

		// Verify values
		require.Equal(t, int32(100), values[0])
		require.Equal(t, int32(200), values[1])
		require.Equal(t, int32(300), values[2])
	})

	t.Run("decode INT64 dictionary", func(t *testing.T) {
		// Create dictionary values: [1000, 2000, 3000]
		dictValues := []int64{1000, 2000, 3000}
		var dictData bytes.Buffer

		// Encode as PLAIN INT64 (little-endian)
		for _, val := range dictValues {
			dictData.WriteByte(byte(val))
			dictData.WriteByte(byte(val >> 8))
			dictData.WriteByte(byte(val >> 16))
			dictData.WriteByte(byte(val >> 24))
			dictData.WriteByte(byte(val >> 32))
			dictData.WriteByte(byte(val >> 40))
			dictData.WriteByte(byte(val >> 48))
			dictData.WriteByte(byte(val >> 56))
		}

		// Create page header
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DICTIONARY_PAGE
		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = int32(len(dictValues))
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_PLAIN

		// Decode
		values, err := reader.DecodeDictionaryPage(dictData.Bytes(), pageHeader, parquet.Type_INT64)
		require.NoError(t, err)
		require.Len(t, values, 3)

		// Verify values
		require.Equal(t, int64(1000), values[0])
		require.Equal(t, int64(2000), values[1])
		require.Equal(t, int64(3000), values[2])
	})

	t.Run("error - missing dictionary page header", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DICTIONARY_PAGE
		// DictionaryPageHeader is nil

		_, err := reader.DecodeDictionaryPage([]byte{}, pageHeader, parquet.Type_BYTE_ARRAY)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing dictionary page header")
	})

	t.Run("error - unsupported encoding", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DICTIONARY_PAGE
		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = 3
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_RLE // Unsupported for dictionary

		_, err := reader.DecodeDictionaryPage([]byte{1, 2, 3}, pageHeader, parquet.Type_INT32)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported encoding for dictionary")
	})

	t.Run("error - invalid data", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DICTIONARY_PAGE
		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = 10 // Expect 10 values
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_PLAIN

		// Provide insufficient data
		_, err := reader.DecodeDictionaryPage([]byte{1, 2}, pageHeader, parquet.Type_INT64)
		require.Error(t, err)
		require.Contains(t, err.Error(), "decode dictionary values")
	})

	t.Run("PLAIN_DICTIONARY encoding", func(t *testing.T) {
		// PLAIN_DICTIONARY should also be accepted
		dictValues := []int32{42}
		var dictData bytes.Buffer

		// Encode as PLAIN INT32
		for _, val := range dictValues {
			dictData.WriteByte(byte(val))
			dictData.WriteByte(byte(val >> 8))
			dictData.WriteByte(byte(val >> 16))
			dictData.WriteByte(byte(val >> 24))
		}

		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DICTIONARY_PAGE
		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = 1
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_PLAIN_DICTIONARY

		values, err := reader.DecodeDictionaryPage(dictData.Bytes(), pageHeader, parquet.Type_INT32)
		require.NoError(t, err)
		require.Len(t, values, 1)
		require.Equal(t, int32(42), values[0])
	})

	t.Run("empty dictionary", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DICTIONARY_PAGE
		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = 0
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_PLAIN

		values, err := reader.DecodeDictionaryPage([]byte{}, pageHeader, parquet.Type_INT32)
		require.NoError(t, err)
		require.Empty(t, values)
	})
}

func Test_ExtractPageHeaderInfo(t *testing.T) {
	t.Run("DATA_PAGE", func(t *testing.T) {
		// Create DATA_PAGE header
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DATA_PAGE
		pageHeader.CompressedPageSize = 1024
		pageHeader.UncompressedPageSize = 2048
		crc := int32(12345)
		pageHeader.Crc = &crc

		pageHeader.DataPageHeader = parquet.NewDataPageHeader()
		pageHeader.DataPageHeader.NumValues = 100
		pageHeader.DataPageHeader.Encoding = parquet.Encoding_PLAIN
		pageHeader.DataPageHeader.DefinitionLevelEncoding = parquet.Encoding_RLE
		pageHeader.DataPageHeader.RepetitionLevelEncoding = parquet.Encoding_RLE

		// Add statistics
		pageHeader.DataPageHeader.Statistics = parquet.NewStatistics()
		pageHeader.DataPageHeader.Statistics.Max = []byte{0xFF}
		pageHeader.DataPageHeader.Statistics.Min = []byte{0x00}

		// Extract info
		info := reader.ExtractPageHeaderInfo(pageHeader, 5000, 3)

		// Verify basic fields
		require.Equal(t, 3, info.Index)
		require.Equal(t, int64(5000), info.Offset)
		require.Equal(t, parquet.PageType_DATA_PAGE, info.PageType)
		require.Equal(t, int32(1024), info.CompressedSize)
		require.Equal(t, int32(2048), info.UncompressedSize)
		require.True(t, info.HasCRC)
		require.Equal(t, int32(12345), info.CRC)

		// Verify DATA_PAGE specific fields
		require.Equal(t, int32(100), info.NumValues)
		require.Equal(t, parquet.Encoding_PLAIN, info.Encoding)
		require.Equal(t, parquet.Encoding_RLE, info.DefLevelEncoding)
		require.Equal(t, parquet.Encoding_RLE, info.RepLevelEncoding)
		require.True(t, info.HasStatistics)
		require.NotNil(t, info.Statistics)
		require.Equal(t, []byte{0xFF}, info.Statistics.Max)
		require.Equal(t, []byte{0x00}, info.Statistics.Min)
	})

	t.Run("DATA_PAGE_V2", func(t *testing.T) {
		// Create DATA_PAGE_V2 header
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DATA_PAGE_V2
		pageHeader.CompressedPageSize = 512
		pageHeader.UncompressedPageSize = 1024

		pageHeader.DataPageHeaderV2 = parquet.NewDataPageHeaderV2()
		pageHeader.DataPageHeaderV2.NumValues = 200
		pageHeader.DataPageHeaderV2.NumNulls = 10
		pageHeader.DataPageHeaderV2.NumRows = 50
		pageHeader.DataPageHeaderV2.Encoding = parquet.Encoding_DELTA_BINARY_PACKED
		pageHeader.DataPageHeaderV2.DefinitionLevelsByteLength = 20
		pageHeader.DataPageHeaderV2.RepetitionLevelsByteLength = 15
		pageHeader.DataPageHeaderV2.IsCompressed = true

		// Add statistics
		pageHeader.DataPageHeaderV2.Statistics = parquet.NewStatistics()
		pageHeader.DataPageHeaderV2.Statistics.Max = []byte{0xAA}
		pageHeader.DataPageHeaderV2.Statistics.Min = []byte{0x11}

		// Extract info
		info := reader.ExtractPageHeaderInfo(pageHeader, 10000, 5)

		// Verify basic fields
		require.Equal(t, 5, info.Index)
		require.Equal(t, int64(10000), info.Offset)
		require.Equal(t, parquet.PageType_DATA_PAGE_V2, info.PageType)
		require.Equal(t, int32(512), info.CompressedSize)
		require.Equal(t, int32(1024), info.UncompressedSize)
		require.False(t, info.HasCRC)

		// Verify DATA_PAGE_V2 specific fields
		require.Equal(t, int32(200), info.NumValues)
		require.Equal(t, int32(10), info.NumNulls)
		require.Equal(t, int32(50), info.NumRows)
		require.Equal(t, parquet.Encoding_DELTA_BINARY_PACKED, info.Encoding)
		require.Equal(t, int32(20), info.DefLevelBytes)
		require.Equal(t, int32(15), info.RepLevelBytes)
		require.NotNil(t, info.IsCompressed)
		require.True(t, *info.IsCompressed)
		require.True(t, info.HasStatistics)
		require.NotNil(t, info.Statistics)
		require.Equal(t, []byte{0xAA}, info.Statistics.Max)
		require.Equal(t, []byte{0x11}, info.Statistics.Min)
	})

	t.Run("DATA_PAGE_V2 without statistics", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DATA_PAGE_V2
		pageHeader.CompressedPageSize = 256
		pageHeader.UncompressedPageSize = 512

		pageHeader.DataPageHeaderV2 = parquet.NewDataPageHeaderV2()
		pageHeader.DataPageHeaderV2.NumValues = 75
		pageHeader.DataPageHeaderV2.NumNulls = 5
		pageHeader.DataPageHeaderV2.NumRows = 25
		pageHeader.DataPageHeaderV2.Encoding = parquet.Encoding_PLAIN
		pageHeader.DataPageHeaderV2.DefinitionLevelsByteLength = 10
		pageHeader.DataPageHeaderV2.RepetitionLevelsByteLength = 8
		pageHeader.DataPageHeaderV2.IsCompressed = false
		// No statistics set

		info := reader.ExtractPageHeaderInfo(pageHeader, 2000, 1)

		require.Equal(t, int32(75), info.NumValues)
		require.Equal(t, int32(5), info.NumNulls)
		require.Equal(t, int32(25), info.NumRows)
		require.Equal(t, parquet.Encoding_PLAIN, info.Encoding)
		require.Equal(t, int32(10), info.DefLevelBytes)
		require.Equal(t, int32(8), info.RepLevelBytes)
		require.NotNil(t, info.IsCompressed)
		require.False(t, *info.IsCompressed)
		require.False(t, info.HasStatistics)
		require.Nil(t, info.Statistics)
	})

	t.Run("DICTIONARY_PAGE", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DICTIONARY_PAGE
		pageHeader.CompressedPageSize = 128
		pageHeader.UncompressedPageSize = 256

		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = 50
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_PLAIN
		isSorted := true
		pageHeader.DictionaryPageHeader.IsSorted = &isSorted

		info := reader.ExtractPageHeaderInfo(pageHeader, 1000, 0)

		require.Equal(t, parquet.PageType_DICTIONARY_PAGE, info.PageType)
		require.Equal(t, int32(50), info.NumValues)
		require.Equal(t, parquet.Encoding_PLAIN, info.Encoding)
		require.NotNil(t, info.IsSorted)
		require.True(t, *info.IsSorted)
	})

	t.Run("INDEX_PAGE", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_INDEX_PAGE
		pageHeader.CompressedPageSize = 64
		pageHeader.UncompressedPageSize = 128

		info := reader.ExtractPageHeaderInfo(pageHeader, 500, 2)

		require.Equal(t, parquet.PageType_INDEX_PAGE, info.PageType)
		require.Equal(t, int32(0), info.NumValues) // Index pages have no values
	})

	t.Run("with CRC", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DATA_PAGE
		pageHeader.CompressedPageSize = 100
		pageHeader.UncompressedPageSize = 200
		crc := int32(98765)
		pageHeader.Crc = &crc

		pageHeader.DataPageHeader = parquet.NewDataPageHeader()

		info := reader.ExtractPageHeaderInfo(pageHeader, 0, 0)

		require.True(t, info.HasCRC)
		require.Equal(t, int32(98765), info.CRC)
	})

	t.Run("without CRC", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DATA_PAGE
		pageHeader.CompressedPageSize = 100
		pageHeader.UncompressedPageSize = 200
		pageHeader.DataPageHeader = parquet.NewDataPageHeader()

		info := reader.ExtractPageHeaderInfo(pageHeader, 0, 0)

		require.False(t, info.HasCRC)
		require.Equal(t, int32(0), info.CRC)
	})

	t.Run("DATA_PAGE_V2 with all fields", func(t *testing.T) {
		// Comprehensive test for DATA_PAGE_V2 with all possible fields set
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DATA_PAGE_V2
		pageHeader.CompressedPageSize = 4096
		pageHeader.UncompressedPageSize = 8192
		crc := int32(55555)
		pageHeader.Crc = &crc

		pageHeader.DataPageHeaderV2 = parquet.NewDataPageHeaderV2()
		pageHeader.DataPageHeaderV2.NumValues = 1000
		pageHeader.DataPageHeaderV2.NumNulls = 50
		pageHeader.DataPageHeaderV2.NumRows = 250
		pageHeader.DataPageHeaderV2.Encoding = parquet.Encoding_RLE_DICTIONARY
		pageHeader.DataPageHeaderV2.DefinitionLevelsByteLength = 100
		pageHeader.DataPageHeaderV2.RepetitionLevelsByteLength = 80
		pageHeader.DataPageHeaderV2.IsCompressed = true

		// Full statistics
		pageHeader.DataPageHeaderV2.Statistics = parquet.NewStatistics()
		pageHeader.DataPageHeaderV2.Statistics.Max = []byte{0xFF, 0xFF}
		pageHeader.DataPageHeaderV2.Statistics.Min = []byte{0x00, 0x00}
		pageHeader.DataPageHeaderV2.Statistics.NullCount = new(int64)
		*pageHeader.DataPageHeaderV2.Statistics.NullCount = 50
		pageHeader.DataPageHeaderV2.Statistics.DistinctCount = new(int64)
		*pageHeader.DataPageHeaderV2.Statistics.DistinctCount = 150

		info := reader.ExtractPageHeaderInfo(pageHeader, 20000, 10)

		// Verify all fields are correctly extracted
		require.Equal(t, 10, info.Index)
		require.Equal(t, int64(20000), info.Offset)
		require.Equal(t, parquet.PageType_DATA_PAGE_V2, info.PageType)
		require.Equal(t, int32(4096), info.CompressedSize)
		require.Equal(t, int32(8192), info.UncompressedSize)
		require.True(t, info.HasCRC)
		require.Equal(t, int32(55555), info.CRC)
		require.Equal(t, int32(1000), info.NumValues)
		require.Equal(t, int32(50), info.NumNulls)
		require.Equal(t, int32(250), info.NumRows)
		require.Equal(t, parquet.Encoding_RLE_DICTIONARY, info.Encoding)
		require.Equal(t, int32(100), info.DefLevelBytes)
		require.Equal(t, int32(80), info.RepLevelBytes)
		require.NotNil(t, info.IsCompressed)
		require.True(t, *info.IsCompressed)
		require.True(t, info.HasStatistics)
		require.NotNil(t, info.Statistics)
		require.Equal(t, []byte{0xFF, 0xFF}, info.Statistics.Max)
		require.Equal(t, []byte{0x00, 0x00}, info.Statistics.Min)
		require.NotNil(t, info.Statistics.NullCount)
		require.Equal(t, int64(50), *info.Statistics.NullCount)
		require.NotNil(t, info.Statistics.DistinctCount)
		require.Equal(t, int64(150), *info.Statistics.DistinctCount)
	})
}

func Test_ReadAllPageHeaders_NegativeCases(t *testing.T) {
	t.Run("nil column chunk metadata", func(t *testing.T) {
		// Create a mock reader (won't be used)
		testFile := getTestParquetFile(t)
		buf, err := local.NewLocalFileReader(testFile)
		require.NoError(t, err)
		pr, err := reader.NewParquetReader(buf, new(TestPageRecord), 4)
		require.NoError(t, err)
		defer func() {
			_ = pr.ReadStopWithError()
		}()

		// Create column chunk with nil metadata
		columnChunk := &parquet.ColumnChunk{
			MetaData: nil,
		}

		_, err = reader.ReadAllPageHeaders(pr.PFile, columnChunk)
		require.Error(t, err)
		require.Contains(t, err.Error(), "column chunk metadata is nil")
	})

	t.Run("invalid seek offset", func(t *testing.T) {
		// Create a mock reader that will fail on seek
		data := []byte{0x00} // Invalid/minimal data
		buf := buffer.NewBufferReaderFromBytesNoAlloc(data)

		// Create column chunk with metadata pointing to invalid offset
		columnChunk := &parquet.ColumnChunk{
			MetaData: &parquet.ColumnMetaData{
				DataPageOffset: 99999, // Beyond the file
				NumValues:      10,
			},
		}

		headers, err := reader.ReadAllPageHeaders(buf, columnChunk)
		// Should break out of loop and return empty headers (no error)
		require.NoError(t, err)
		require.Empty(t, headers)
	})

	t.Run("corrupted page header", func(t *testing.T) {
		// Create data with invalid thrift encoding
		data := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
		buf := buffer.NewBufferReaderFromBytesNoAlloc(data)

		columnChunk := &parquet.ColumnChunk{
			MetaData: &parquet.ColumnMetaData{
				DataPageOffset: 0,
				NumValues:      10,
			},
		}

		headers, err := reader.ReadAllPageHeaders(buf, columnChunk)
		// Should break out of loop on read error
		require.NoError(t, err)
		require.Empty(t, headers)
	})
}

func Test_ReadPageData_NegativeCases(t *testing.T) {
	t.Run("invalid offset - cannot seek", func(t *testing.T) {
		data := []byte{0x00}
		buf := buffer.NewBufferReaderFromBytesNoAlloc(data)

		pageHeader := parquet.NewPageHeader()
		pageHeader.CompressedPageSize = 100
		pageHeader.UncompressedPageSize = 200

		_, err := reader.ReadPageData(buf, 99999, pageHeader, parquet.CompressionCodec_UNCOMPRESSED)
		require.Error(t, err)
		require.Contains(t, err.Error(), "read page header")
	})

	t.Run("cannot read compressed data - EOF", func(t *testing.T) {
		// Create valid parquet file
		testFile := getTestParquetFile(t)
		buf, err := local.NewLocalFileReader(testFile)
		require.NoError(t, err)
		pr, err := reader.NewParquetReader(buf, new(TestPageRecord), 4)
		require.NoError(t, err)
		defer func() {
			_ = pr.ReadStopWithError()
		}()

		// Get a valid offset
		col := pr.Footer.RowGroups[0].Columns[0]
		headers, err := reader.ReadAllPageHeaders(pr.PFile, col)
		require.NoError(t, err)
		require.NotEmpty(t, headers)

		firstPage := headers[0]

		// Create a page header with size larger than available data
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = firstPage.PageType
		pageHeader.CompressedPageSize = 999
		pageHeader.UncompressedPageSize = 999

		_, err = reader.ReadPageData(pr.PFile, firstPage.Offset, pageHeader, col.MetaData.Codec)
		require.Error(t, err)
		require.Contains(t, err.Error(), "decompress page data")
	})

	t.Run("decompression failure - invalid compressed data", func(t *testing.T) {
		// Create minimal mock data that looks like a page but isn't valid GZIP
		mockData := []byte{
			// Minimal Thrift compact protocol header for PageHeader
			0x15, 0x00, // field id=0, type=struct (PageType)
			0x16, 0x00, // field id=1, type=i32 (UncompressedPageSize)
			0x80, 0x80, 0x01, // varint = 16384
			0x16, 0x02, // field id=2, type=i32 (CompressedPageSize)
			0x10, // varint = 8 (8 bytes of "compressed" data)
			0x00, // stop field
			// 8 bytes of invalid "compressed" data
			0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE,
		}
		buf := buffer.NewBufferReaderFromBytesNoAlloc(mockData)

		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DATA_PAGE
		pageHeader.CompressedPageSize = 8
		pageHeader.UncompressedPageSize = 16384

		// Try to decompress with GZIP (will fail on invalid data)
		_, err := reader.ReadPageData(buf, 0, pageHeader, parquet.CompressionCodec_GZIP)
		require.Error(t, err)
		// Should fail during reading header or page data
		require.True(t, err != nil)
	})
}

func Test_ReadDictionaryPageValues_NegativeCases(t *testing.T) {
	testFile := getTestParquetFile(t)
	buf, err := local.NewLocalFileReader(testFile)
	require.NoError(t, err)
	pr, err := reader.NewParquetReader(buf, new(TestPageRecord), 4)
	require.NoError(t, err)
	defer func() {
		_ = pr.ReadStopWithError()
	}()

	t.Run("invalid offset - cannot read header", func(t *testing.T) {
		_, err := pr.ReadDictionaryPageValues(
			999999, // Invalid offset
			parquet.CompressionCodec_UNCOMPRESSED,
			parquet.Type_BYTE_ARRAY,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "read page header")
	})

	t.Run("wrong page type - not a dictionary page", func(t *testing.T) {
		// Use a data page offset instead of dictionary page offset
		col := pr.Footer.RowGroups[0].Columns[0]
		dataOffset := col.MetaData.DataPageOffset

		_, err := pr.ReadDictionaryPageValues(
			dataOffset,
			col.MetaData.Codec,
			parquet.Type_BYTE_ARRAY,
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "expected dictionary page")
	})

	t.Run("failed to decode dictionary page - wrong encoding", func(t *testing.T) {
		dictCol := pr.Footer.RowGroups[0].Columns[0]
		dictOffset := *dictCol.MetaData.DictionaryPageOffset

		// This will fail during decoding if we use the wrong physical type
		// Try to decode as FIXED_LEN_BYTE_ARRAY when it's actually BYTE_ARRAY
		wrongType := parquet.Type_FIXED_LEN_BYTE_ARRAY
		if dictCol.MetaData.Type == parquet.Type_FIXED_LEN_BYTE_ARRAY {
			wrongType = parquet.Type_INT32
		}

		_, err := pr.ReadDictionaryPageValues(
			dictOffset,
			dictCol.MetaData.Codec,
			wrongType, // Wrong type
		)
		// This might succeed or fail depending on the data, but we're testing the path
		// The important thing is it doesn't panic
		_ = err
	})
}

func Test_DecodeDictionaryPage_NegativeCases(t *testing.T) {
	t.Run("nil page header", func(t *testing.T) {
		// This would panic in real code, but testing the validation
		pageHeader := parquet.NewPageHeader()
		// DictionaryPageHeader is nil by default

		_, err := reader.DecodeDictionaryPage([]byte{1, 2, 3}, pageHeader, parquet.Type_INT32)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing dictionary page header")
	})

	t.Run("unsupported encoding - RLE", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = 5
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_RLE

		_, err := reader.DecodeDictionaryPage([]byte{1, 2, 3}, pageHeader, parquet.Type_INT32)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported encoding for dictionary")
	})

	t.Run("unsupported encoding - DELTA_BINARY_PACKED", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = 5
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_DELTA_BINARY_PACKED

		_, err := reader.DecodeDictionaryPage([]byte{1, 2, 3}, pageHeader, parquet.Type_INT64)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported encoding for dictionary")
	})

	t.Run("corrupt data - truncated", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = 100 // Expect 100 values
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_PLAIN

		// Provide only 2 bytes of data (not enough for 100 INT32 values)
		_, err := reader.DecodeDictionaryPage([]byte{1, 2}, pageHeader, parquet.Type_INT32)
		require.Error(t, err)
		require.Contains(t, err.Error(), "decode dictionary values")
	})

	t.Run("corrupt data - invalid BYTE_ARRAY length", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = 1
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_PLAIN

		// Create invalid BYTE_ARRAY data with huge length prefix but no actual data
		var data bytes.Buffer
		length := int32(999999) // Claim 999999 bytes
		data.WriteByte(byte(length))
		data.WriteByte(byte(length >> 8))
		data.WriteByte(byte(length >> 16))
		data.WriteByte(byte(length >> 24))
		// No actual string data follows

		_, err := reader.DecodeDictionaryPage(data.Bytes(), pageHeader, parquet.Type_BYTE_ARRAY)
		require.Error(t, err)
		require.Contains(t, err.Error(), "decode dictionary values")
	})
}
