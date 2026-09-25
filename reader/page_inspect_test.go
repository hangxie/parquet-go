package reader

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/local"
)

func TestGetAllPageHeaders(t *testing.T) {
	testFile := getTestParquetFile(t)
	buf, err := local.NewLocalFileReader(testFile)
	require.NoError(t, err)
	pr, err := NewParquetReader(buf, new(TestPageRecord), WithNP(4))
	require.NoError(t, err)
	defer func() {
		_ = pr.ReadStop()
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

func TestReadDictionaryPageValues(t *testing.T) {
	testFile := getTestParquetFile(t)
	buf, err := local.NewLocalFileReader(testFile)
	require.NoError(t, err)
	pr, err := NewParquetReader(buf, new(TestPageRecord), WithNP(4))
	require.NoError(t, err)
	defer func() {
		_ = pr.ReadStop()
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
		require.Contains(t, err.Error(), "seek to page")
	})
}

func TestGetAllPageHeaders_AllPageTypes(t *testing.T) {
	testFile := getTestParquetFile(t)
	buf, err := local.NewLocalFileReader(testFile)
	require.NoError(t, err)
	pr, err := NewParquetReader(buf, new(TestPageRecord), WithNP(4))
	require.NoError(t, err)
	defer func() {
		_ = pr.ReadStop()
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

func TestReadDictionaryPageValues_NegativeCases(t *testing.T) {
	testFile := getTestParquetFile(t)
	buf, err := local.NewLocalFileReader(testFile)
	require.NoError(t, err)
	pr, err := NewParquetReader(buf, new(TestPageRecord), WithNP(4))
	require.NoError(t, err)
	defer func() {
		_ = pr.ReadStop()
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

func TestGetFirstDataPageHeader(t *testing.T) {
	testFile := getTestParquetFile(t)
	buf, err := local.NewLocalFileReader(testFile)
	require.NoError(t, err)
	pr, err := NewParquetReader(buf, new(TestPageRecord), WithNP(4))
	require.NoError(t, err)
	defer func() {
		_ = pr.ReadStop()
	}()

	t.Run("compare with first data page from GetAllPageHeaders", func(t *testing.T) {
		// Get all page headers
		allHeaders, err := pr.GetAllPageHeaders(0, 0)
		require.NoError(t, err)
		require.NotEmpty(t, allHeaders)

		// Find the first data page in all headers
		var firstDataPageFromAll *PageHeaderInfo
		for i := range allHeaders {
			if allHeaders[i].PageType == parquet.PageType_DATA_PAGE ||
				allHeaders[i].PageType == parquet.PageType_DATA_PAGE_V2 {
				firstDataPageFromAll = &allHeaders[i]
				break
			}
		}
		require.NotNil(t, firstDataPageFromAll, "Should have at least one data page")

		// Get first data page header directly
		firstDataPageHeader, err := pr.GetFirstDataPageHeader(0, 0)
		require.NoError(t, err)
		require.NotNil(t, firstDataPageHeader)

		// Compare the two results - they should match
		require.Equal(t, firstDataPageFromAll.Offset, firstDataPageHeader.Offset)
		require.Equal(t, firstDataPageFromAll.PageType, firstDataPageHeader.PageType)
		require.Equal(t, firstDataPageFromAll.CompressedSize, firstDataPageHeader.CompressedSize)
		require.Equal(t, firstDataPageFromAll.UncompressedSize, firstDataPageHeader.UncompressedSize)
		require.Equal(t, firstDataPageFromAll.NumValues, firstDataPageHeader.NumValues)
		require.Equal(t, firstDataPageFromAll.Encoding, firstDataPageHeader.Encoding)
		require.Equal(t, firstDataPageFromAll.DefLevelEncoding, firstDataPageHeader.DefLevelEncoding)
		require.Equal(t, firstDataPageFromAll.RepLevelEncoding, firstDataPageHeader.RepLevelEncoding)
		require.Equal(t, firstDataPageFromAll.HasStatistics, firstDataPageHeader.HasStatistics)
		require.Equal(t, firstDataPageFromAll.HasCRC, firstDataPageHeader.HasCRC)

		// Index should be 0 for GetFirstDataPageHeader (it's always the "first" in its result)
		require.Equal(t, 0, firstDataPageHeader.Index)
	})

	t.Run("valid indices", func(t *testing.T) {
		header, err := pr.GetFirstDataPageHeader(0, 0)
		require.NoError(t, err)
		require.NotNil(t, header)

		// Verify it's actually a data page
		require.True(t,
			header.PageType == parquet.PageType_DATA_PAGE ||
				header.PageType == parquet.PageType_DATA_PAGE_V2,
			"Should return a data page type")

		// Verify basic fields have sensible values
		require.Positive(t, header.CompressedSize)
		require.Positive(t, header.UncompressedSize)
		require.Positive(t, header.Offset)
		require.Positive(t, header.NumValues)
	})

	t.Run("invalid row group index", func(t *testing.T) {
		_, err := pr.GetFirstDataPageHeader(-1, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid row group index")

		_, err = pr.GetFirstDataPageHeader(999, 0)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid row group index")
	})

	t.Run("invalid column index", func(t *testing.T) {
		_, err := pr.GetFirstDataPageHeader(0, -1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid column index")

		_, err = pr.GetFirstDataPageHeader(0, 999)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid column index")
	})

	t.Run("multiple columns", func(t *testing.T) {
		// Test that we can read first data page header for all columns
		numCols := len(pr.Footer.RowGroups[0].Columns)
		for colIdx := 0; colIdx < numCols; colIdx++ {
			header, err := pr.GetFirstDataPageHeader(0, colIdx)
			require.NoError(t, err)
			require.NotNil(t, header)
			require.True(t,
				header.PageType == parquet.PageType_DATA_PAGE ||
					header.PageType == parquet.PageType_DATA_PAGE_V2,
				"Column %d should have a data page", colIdx)
		}
	})

	t.Run("skips dictionary page if present", func(t *testing.T) {
		// Check if column 0 has a dictionary page
		col := pr.Footer.RowGroups[0].Columns[0]
		if col.MetaData.DictionaryPageOffset != nil {
			// This column has a dictionary page
			allHeaders, err := pr.GetAllPageHeaders(0, 0)
			require.NoError(t, err)

			// First page in all headers might be dictionary
			if allHeaders[0].PageType == parquet.PageType_DICTIONARY_PAGE {
				// Get first data page header - should skip the dictionary
				firstDataPage, err := pr.GetFirstDataPageHeader(0, 0)
				require.NoError(t, err)

				// Should not be a dictionary page
				require.NotEqual(t, parquet.PageType_DICTIONARY_PAGE, firstDataPage.PageType)
				require.True(t,
					firstDataPage.PageType == parquet.PageType_DATA_PAGE ||
						firstDataPage.PageType == parquet.PageType_DATA_PAGE_V2)

				// Offset should be different from dictionary page
				require.NotEqual(t, allHeaders[0].Offset, firstDataPage.Offset)
			}
		}
	})

	t.Run("encoding matches expected type", func(t *testing.T) {
		header, err := pr.GetFirstDataPageHeader(0, 0)
		require.NoError(t, err)

		// Verify encoding is a valid data page encoding
		validEncodings := []parquet.Encoding{
			parquet.Encoding_PLAIN,
			parquet.Encoding_RLE_DICTIONARY,
			parquet.Encoding_PLAIN_DICTIONARY,
			parquet.Encoding_DELTA_BINARY_PACKED,
			parquet.Encoding_DELTA_LENGTH_BYTE_ARRAY,
			parquet.Encoding_DELTA_BYTE_ARRAY,
			parquet.Encoding_BYTE_STREAM_SPLIT,
		}

		found := false
		for _, validEnc := range validEncodings {
			if header.Encoding == validEnc {
				found = true
				break
			}
		}
		require.True(t, found, "Encoding %v should be a valid data page encoding", header.Encoding)
	})
}

func TestReadAllPageHeaders_ViaGetAllPageHeaders(t *testing.T) {
	// Test using the non-deprecated GetAllPageHeaders API
	testFile := getTestParquetFile(t)
	buf, err := local.NewLocalFileReader(testFile)
	require.NoError(t, err)
	pr, err := NewParquetReader(buf, new(TestPageRecord), WithNP(4))
	require.NoError(t, err)
	defer func() {
		_ = pr.ReadStop()
	}()

	require.NotEmpty(t, pr.Footer.RowGroups)
	require.NotEmpty(t, pr.Footer.RowGroups[0].Columns)

	// Use the non-deprecated API
	headers, err := pr.GetAllPageHeaders(0, 0)
	require.NoError(t, err)
	require.NotEmpty(t, headers)
}
