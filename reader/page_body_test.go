package reader

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

func TestReadPageData(t *testing.T) {
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

	// Get headers via public API
	headers, err := pr.GetAllPageHeaders(0, 0)
	require.NoError(t, err)
	require.NotEmpty(t, headers)

	col := pr.Footer.RowGroups[0].Columns[0]

	// Read data from the first page
	firstPage := headers[0]

	// For testing, we'll create a minimal page header
	pageHeader := parquet.NewPageHeader()
	pageHeader.Type = firstPage.PageType
	pageHeader.CompressedPageSize = firstPage.CompressedSize
	pageHeader.UncompressedPageSize = firstPage.UncompressedSize

	pageData, err := ReadPageData(pr.PFile, firstPage.Offset, pageHeader, col.MetaData.Codec, nil)
	require.NoError(t, err)
	require.NotEmpty(t, pageData)
	require.Equal(t, firstPage.UncompressedSize, int32(len(pageData)))
}

func TestReadPageData_V2(t *testing.T) {
	// Use an optional field so V2 pages have non-zero definition level bytes
	type Record struct {
		Name *string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
	}

	var buf bytes.Buffer
	pw, err := writer.NewParquetWriterFromWriterWithContext(
		context.Background(), &buf, new(Record),
		writer.WithNP(1),
		writer.WithDataPageVersion(2),
		writer.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)
	require.NoError(t, err)

	for i := range 100 {
		s := fmt.Sprintf("name_%d", i)
		require.NoError(t, pw.WriteWithContext(context.Background(), Record{Name: &s}))
	}
	require.NoError(t, pw.WriteStopWithContext(context.Background()))

	// Read back and verify ReadPageData handles V2 pages with level data
	data := buf.Bytes()
	pf := buffer.NewBufferReaderFromBytesNoAlloc(data)
	pr, err := NewParquetReader(pf, new(Record), WithNP(1))
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

	headers, err := pr.GetAllPageHeaders(0, 0)
	require.NoError(t, err)
	require.NotEmpty(t, headers)

	col := pr.Footer.RowGroups[0].Columns[0]

	// Verify we actually have V2 pages with non-zero level bytes
	hasV2WithLevels := false
	for _, h := range headers {
		if h.PageType == parquet.PageType_DATA_PAGE_V2 && h.DefLevelBytes > 0 {
			hasV2WithLevels = true
		}
	}
	require.True(t, hasV2WithLevels, "test must produce V2 pages with non-zero definition level bytes")

	for _, h := range headers {
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = h.PageType
		pageHeader.CompressedPageSize = h.CompressedSize
		pageHeader.UncompressedPageSize = h.UncompressedSize
		if h.PageType == parquet.PageType_DATA_PAGE_V2 {
			pageHeader.DataPageHeaderV2 = &parquet.DataPageHeaderV2{
				NumValues:                  h.NumValues,
				NumNulls:                   h.NumNulls,
				NumRows:                    h.NumRows,
				Encoding:                   h.Encoding,
				DefinitionLevelsByteLength: h.DefLevelBytes,
				RepetitionLevelsByteLength: h.RepLevelBytes,
				IsCompressed:               true,
			}
		}

		pageData, err := ReadPageData(pr.PFile, h.Offset, pageHeader, col.MetaData.Codec, nil)
		require.NoError(t, err)
		require.NotEmpty(t, pageData)
		require.Equal(t, h.UncompressedSize, int32(len(pageData)),
			"page %d (type %v): uncompressed size mismatch", h.Index, h.PageType)
	}
}

func TestDecodeDictionaryPage(t *testing.T) {
	t.Run("decode from real parquet file", func(t *testing.T) {
		testFile := getTestParquetFile(t)
		buf, err := local.NewLocalFileReader(testFile)
		require.NoError(t, err)
		pr, err := NewParquetReader(buf, new(TestPageRecord), WithNP(4))
		require.NoError(t, err)
		defer func() {
			_ = pr.ReadStop()
		}()

		dictCol := pr.Footer.RowGroups[0].Columns[0]
		headers, err := pr.GetAllPageHeaders(0, 0)
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
		pageData, err := ReadPageData(pr.PFile, dictPageHeader.Offset, pageHeader, dictCol.MetaData.Codec, nil)
		require.NoError(t, err)
		require.NotEmpty(t, pageData)

		values, err := DecodeDictionaryPage(pageData, pageHeader, dictCol.MetaData.Type)
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
		values, err := DecodeDictionaryPage(dictData.Bytes(), pageHeader, parquet.Type_BYTE_ARRAY)
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
		values, err := DecodeDictionaryPage(dictData.Bytes(), pageHeader, parquet.Type_INT32)
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
		values, err := DecodeDictionaryPage(dictData.Bytes(), pageHeader, parquet.Type_INT64)
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

		_, err := DecodeDictionaryPage([]byte{}, pageHeader, parquet.Type_BYTE_ARRAY)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing dictionary page header")
	})

	t.Run("error - unsupported encoding", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = parquet.PageType_DICTIONARY_PAGE
		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = 3
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_RLE // Unsupported for dictionary

		_, err := DecodeDictionaryPage([]byte{1, 2, 3}, pageHeader, parquet.Type_INT32)
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
		_, err := DecodeDictionaryPage([]byte{1, 2}, pageHeader, parquet.Type_INT64)
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

		values, err := DecodeDictionaryPage(dictData.Bytes(), pageHeader, parquet.Type_INT32)
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

		values, err := DecodeDictionaryPage([]byte{}, pageHeader, parquet.Type_INT32)
		require.NoError(t, err)
		require.Empty(t, values)
	})
}

func TestReadPageData_NegativeCases(t *testing.T) {
	t.Run("invalid offset - cannot seek", func(t *testing.T) {
		data := []byte{0x00}
		buf := buffer.NewBufferReaderFromBytesNoAlloc(data)

		pageHeader := parquet.NewPageHeader()
		pageHeader.CompressedPageSize = 100
		pageHeader.UncompressedPageSize = 200

		_, err := ReadPageData(buf, 99999, pageHeader, parquet.CompressionCodec_UNCOMPRESSED, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "read page header")
	})

	t.Run("cannot read compressed data - EOF", func(t *testing.T) {
		// Create valid parquet file
		testFile := getTestParquetFile(t)
		buf, err := local.NewLocalFileReader(testFile)
		require.NoError(t, err)
		pr, err := NewParquetReader(buf, new(TestPageRecord), WithNP(4))
		require.NoError(t, err)
		defer func() {
			_ = pr.ReadStop()
		}()

		// Get a valid offset
		col := pr.Footer.RowGroups[0].Columns[0]
		headers, err := pr.GetAllPageHeaders(0, 0)
		require.NoError(t, err)
		require.NotEmpty(t, headers)

		firstPage := headers[0]

		// Create a page header with size larger than available data
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = firstPage.PageType
		pageHeader.CompressedPageSize = 999
		pageHeader.UncompressedPageSize = 999

		_, err = ReadPageData(pr.PFile, firstPage.Offset, pageHeader, col.MetaData.Codec, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "decompress page data")
	})

	t.Run("decompressed size mismatch", func(t *testing.T) {
		testFile := getTestParquetFile(t)
		buf, err := local.NewLocalFileReader(testFile)
		require.NoError(t, err)
		pr, err := NewParquetReader(buf, new(TestPageRecord), WithNP(4))
		require.NoError(t, err)
		defer func() {
			_ = pr.ReadStop()
		}()

		col := pr.Footer.RowGroups[0].Columns[0]
		headers, err := pr.GetAllPageHeaders(0, 0)
		require.NoError(t, err)
		require.NotEmpty(t, headers)

		firstPage := headers[0]

		// Set a wrong uncompressed page size to trigger size validation
		pageHeader := parquet.NewPageHeader()
		pageHeader.Type = firstPage.PageType
		pageHeader.CompressedPageSize = firstPage.CompressedSize
		pageHeader.UncompressedPageSize = firstPage.UncompressedSize + 10

		_, err = ReadPageData(pr.PFile, firstPage.Offset, pageHeader, col.MetaData.Codec, nil)
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
		_, err := ReadPageData(buf, 0, pageHeader, parquet.CompressionCodec_GZIP, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Required field")
		// Should fail during reading header or page data
		require.True(t, err != nil)
	})
}

func TestDecodeDictionaryPage_NegativeCases(t *testing.T) {
	t.Run("nil page header", func(t *testing.T) {
		// This would panic in real code, but testing the validation
		pageHeader := parquet.NewPageHeader()
		// DictionaryPageHeader is nil by default

		_, err := DecodeDictionaryPage([]byte{1, 2, 3}, pageHeader, parquet.Type_INT32)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing dictionary page header")
	})

	t.Run("unsupported encoding - RLE", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = 5
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_RLE

		_, err := DecodeDictionaryPage([]byte{1, 2, 3}, pageHeader, parquet.Type_INT32)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported encoding for dictionary")
	})

	t.Run("unsupported encoding - DELTA_BINARY_PACKED", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = 5
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_DELTA_BINARY_PACKED

		_, err := DecodeDictionaryPage([]byte{1, 2, 3}, pageHeader, parquet.Type_INT64)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported encoding for dictionary")
	})

	t.Run("corrupt data - truncated", func(t *testing.T) {
		pageHeader := parquet.NewPageHeader()
		pageHeader.DictionaryPageHeader = parquet.NewDictionaryPageHeader()
		pageHeader.DictionaryPageHeader.NumValues = 100 // Expect 100 values
		pageHeader.DictionaryPageHeader.Encoding = parquet.Encoding_PLAIN

		// Provide only 2 bytes of data (not enough for 100 INT32 values)
		_, err := DecodeDictionaryPage([]byte{1, 2}, pageHeader, parquet.Type_INT32)
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

		_, err := DecodeDictionaryPage(data.Bytes(), pageHeader, parquet.Type_BYTE_ARRAY)
		require.Error(t, err)
		require.Contains(t, err.Error(), "decode dictionary values")
	})
}

func TestCRCValidation_ValidFile(t *testing.T) {
	testdataAvailable(t)

	testCases := map[string]struct {
		mode common.CRCMode
	}{
		"ignore": {common.CRCIgnore},
		"auto":   {common.CRCAuto},
		"strict": {common.CRCStrict},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			pf, err := local.NewLocalFileReader(validCRCFile)
			require.NoError(t, err)
			defer func() {
				_ = pf.Close()
			}()

			pr, err := NewParquetReader(pf, nil,
				WithNP(1), WithCRCMode(tc.mode))
			require.NoError(t, err)
			defer func() { _ = pr.ReadStop() }()

			values, _, _, err := pr.ReadColumnByIndex(0, pr.GetNumRows())
			require.NoError(t, err)
			require.NotEmpty(t, values)
		})
	}
}

func TestCRCValidation_CorruptFile(t *testing.T) {
	testdataAvailable(t)

	testCases := map[string]struct {
		mode   common.CRCMode
		errMsg string
	}{
		"ignore": {common.CRCIgnore, ""},
		"auto":   {common.CRCAuto, "CRC mismatch"},
		"strict": {common.CRCStrict, "CRC mismatch"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			pf, err := local.NewLocalFileReader(corruptCRCFile)
			require.NoError(t, err)
			defer func() {
				_ = pf.Close()
			}()

			pr, err := NewParquetReader(pf, nil,
				WithNP(1), WithCRCMode(tc.mode))
			require.NoError(t, err)
			defer func() { _ = pr.ReadStop() }()

			_, _, _, err = pr.ReadColumnByIndex(0, pr.GetNumRows())
			if tc.errMsg == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.errMsg)
			}
		})
	}
}
