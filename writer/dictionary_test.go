package writer

import (
	"context"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
)

func TestDictionaryEncodingBitWidthAndFallback(t *testing.T) {
	type Entry struct {
		Value string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	}

	t.Run("uses_completed_dictionary_width", func(t *testing.T) {
		pw, buf, err := createTestParquetWriter(
			new(Entry),
			WithNP(4),
			WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		)
		require.NoError(t, err)

		values := []string{"alpha", "beta", "gamma", "delta", "epsilon", "alpha"}
		for _, value := range values {
			require.NoError(t, pw.Write(Entry{Value: value}))
		}
		require.NoError(t, pw.WriteStop())

		column := pw.Footer.RowGroups[0].Columns[0]
		header, headerLen := rawPageHeaderAt(t, buf.Bytes(), column.MetaData.DataPageOffset)
		require.Equal(t, parquet.Encoding_RLE_DICTIONARY, header.DataPageHeader.Encoding)
		bodyOffset := column.MetaData.DataPageOffset + int64(headerLen)
		require.Equal(t, byte(bits.Len(uint(5-1))), buf.Bytes()[bodyOffset])
	})

	t.Run("falls_back_to_plain", func(t *testing.T) {
		pw, buf, err := createTestParquetWriter(
			new(Entry),
			WithNP(4),
			WithPageSize(5),
			WithMaxDictionarySize(10),
			WithDataPageVersion(2),
			WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		)
		require.NoError(t, err)

		values := []string{"alpha", "bravo", "charlie", "delta"}
		for _, value := range values {
			require.NoError(t, pw.Write(Entry{Value: value}))
		}
		require.NoError(t, pw.WriteStop())

		column := pw.Footer.RowGroups[0].Columns[0]
		require.Contains(t, column.MetaData.Encodings, parquet.Encoding_RLE_DICTIONARY)
		require.Contains(t, column.MetaData.Encodings, parquet.Encoding_PLAIN)

		dictHeader, _ := rawPageHeaderAt(t, buf.Bytes(), *column.MetaData.DictionaryPageOffset)
		require.Equal(t, int32(1), dictHeader.DictionaryPageHeader.NumValues)

		offset := column.MetaData.DataPageOffset
		chunkEnd := *column.MetaData.DictionaryPageOffset + column.MetaData.TotalCompressedSize
		for offset < chunkEnd {
			header, headerLen := rawPageHeaderAt(t, buf.Bytes(), offset)
			require.Equal(t, parquet.PageType_DATA_PAGE_V2, header.Type)
			offset += int64(headerLen) + int64(header.CompressedPageSize)
		}

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(Entry), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { require.NoError(t, pf.Close()) }()
		//nolint:staticcheck
		defer func() { require.NoError(t, pr.ReadStop()) }()

		got := make([]Entry, len(values))
		require.NoError(t, pr.ReadWithContext(context.Background(), &got))
		for i := range values {
			require.Equal(t, values[i], got[i].Value)
		}
	})
}
