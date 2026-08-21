package writer

import (
	"context"
	"encoding/binary"
	"math"
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

func TestDictionaryDistinctCountStatistics(t *testing.T) {
	type Entry struct {
		Value string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	}
	type PlainEntry struct {
		Value string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8"`
	}
	type OmitStatsEntry struct {
		Value string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY, omitstats=true"`
	}

	t.Run("dictionary_chunk_reports_exact_ndv", func(t *testing.T) {
		pw, buf, err := createTestParquetWriter(
			new(Entry),
			WithNP(1),
			WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		)
		require.NoError(t, err)

		values := []string{"alpha", "beta", "gamma", "alpha", "beta", "alpha"}
		for _, value := range values {
			require.NoError(t, pw.Write(Entry{Value: value}))
		}
		require.NoError(t, pw.WriteStop())

		pr, pf, err := createTestParquetReader(buf.Bytes(), new(Entry), reader.WithNP(1))
		require.NoError(t, err)
		defer func() { require.NoError(t, pf.Close()) }()
		//nolint:staticcheck
		defer func() { require.NoError(t, pr.ReadStop()) }()

		stats := pr.Footer.RowGroups[0].Columns[0].MetaData.Statistics
		require.NotNil(t, stats.DistinctCount)
		require.Equal(t, int64(3), *stats.DistinctCount)
	})

	t.Run("nulls_are_not_distinct_values", func(t *testing.T) {
		type OptionalEntry struct {
			Value *string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY, repetitiontype=OPTIONAL"`
		}
		pw, _, err := createTestParquetWriter(
			new(OptionalEntry),
			WithNP(1),
			WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		)
		require.NoError(t, err)

		alpha, beta := "alpha", "beta"
		for _, value := range []*string{&alpha, nil, &beta, nil, &alpha} {
			require.NoError(t, pw.Write(OptionalEntry{Value: value}))
		}
		require.NoError(t, pw.WriteStop())

		stats := pw.Footer.RowGroups[0].Columns[0].MetaData.Statistics
		require.NotNil(t, stats.DistinctCount)
		require.Equal(t, int64(2), *stats.DistinctCount)
		require.Equal(t, int64(2), stats.GetNullCount())
	})

	t.Run("plain_fallback_omits_distinct_count", func(t *testing.T) {
		pw, _, err := createTestParquetWriter(
			new(Entry),
			WithNP(1),
			WithPageSize(5),
			WithMaxDictionarySize(10),
			WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		)
		require.NoError(t, err)

		for _, value := range []string{"alpha", "bravo", "charlie", "delta"} {
			require.NoError(t, pw.Write(Entry{Value: value}))
		}
		require.NoError(t, pw.WriteStop())

		column := pw.Footer.RowGroups[0].Columns[0]
		require.Contains(t, column.MetaData.Encodings, parquet.Encoding_PLAIN)
		require.Nil(t, column.MetaData.Statistics.DistinctCount)
	})

	t.Run("plain_column_omits_distinct_count", func(t *testing.T) {
		pw, _, err := createTestParquetWriter(
			new(PlainEntry),
			WithNP(1),
			WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		)
		require.NoError(t, err)

		for _, value := range []string{"alpha", "beta", "alpha"} {
			require.NoError(t, pw.Write(PlainEntry{Value: value}))
		}
		require.NoError(t, pw.WriteStop())

		require.Nil(t, pw.Footer.RowGroups[0].Columns[0].MetaData.Statistics.DistinctCount)
	})

	t.Run("omitstats_omits_distinct_count", func(t *testing.T) {
		pw, _, err := createTestParquetWriter(
			new(OmitStatsEntry),
			WithNP(1),
			WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		)
		require.NoError(t, err)

		for _, value := range []string{"alpha", "beta", "alpha"} {
			require.NoError(t, pw.Write(OmitStatsEntry{Value: value}))
		}
		require.NoError(t, pw.WriteStop())

		require.Nil(t, pw.Footer.RowGroups[0].Columns[0].MetaData.Statistics.DistinctCount)
	})
}

func TestDictionaryDistinctCountFloatNaN(t *testing.T) {
	type Entry struct {
		Value float64 `parquet:"name=value, type=DOUBLE, encoding=RLE_DICTIONARY"`
	}

	write := func(t *testing.T, values []float64) *parquet.Statistics {
		t.Helper()
		pw, _, err := createTestParquetWriter(
			new(Entry),
			WithNP(1),
			WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		)
		require.NoError(t, err)
		for _, value := range values {
			require.NoError(t, pw.Write(Entry{Value: value}))
		}
		require.NoError(t, pw.WriteStop())
		return pw.Footer.RowGroups[0].Columns[0].MetaData.Statistics
	}

	t.Run("nan_omits_distinct_count", func(t *testing.T) {
		// Each NaN takes a dictionary entry of its own, overstating the distinct value count.
		stats := write(t, []float64{1.0, math.NaN(), 2.0, math.NaN(), math.NaN(), 1.0})
		require.Nil(t, stats.DistinctCount)
	})

	t.Run("without_nan_reports_exact_ndv", func(t *testing.T) {
		stats := write(t, []float64{1.0, 2.0, 1.0, math.Inf(1), math.Inf(-1)})
		require.NotNil(t, stats.DistinctCount)
		require.Equal(t, int64(4), *stats.DistinctCount)
	})
}

func TestDictionaryDistinctCountDecimal(t *testing.T) {
	t.Run("byte_array_decimal_omits_distinct_count", func(t *testing.T) {
		type Entry struct {
			Value string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=0, precision=10, encoding=RLE_DICTIONARY"`
		}
		pw, _, err := createTestParquetWriter(
			new(Entry),
			WithNP(1),
			WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		)
		require.NoError(t, err)

		// Sign-extended encodings of unscaled 1: three map keys, one decimal.
		for _, value := range []string{"\x01", "\x00\x01", "\x00\x00\x01"} {
			require.NoError(t, pw.Write(Entry{Value: value}))
		}
		require.NoError(t, pw.WriteStop())

		column := pw.Footer.RowGroups[0].Columns[0]
		require.Contains(t, column.MetaData.Encodings, parquet.Encoding_RLE_DICTIONARY)
		require.Nil(t, column.MetaData.Statistics.DistinctCount)
	})

	t.Run("fixed_len_byte_array_decimal_reports_exact_ndv", func(t *testing.T) {
		type Entry struct {
			Value string `parquet:"name=value, type=FIXED_LEN_BYTE_ARRAY, length=4, convertedtype=DECIMAL, scale=0, precision=9, encoding=RLE_DICTIONARY"`
		}
		pw, _, err := createTestParquetWriter(
			new(Entry),
			WithNP(1),
			WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		)
		require.NoError(t, err)

		// A fixed width gives each value one encoding.
		for _, value := range []string{"\x00\x00\x00\x01", "\x00\x00\x00\x01", "\x00\x00\x00\x02"} {
			require.NoError(t, pw.Write(Entry{Value: value}))
		}
		require.NoError(t, pw.WriteStop())

		stats := pw.Footer.RowGroups[0].Columns[0].MetaData.Statistics
		require.NotNil(t, stats.DistinctCount)
		require.Equal(t, int64(2), *stats.DistinctCount)
	})

	t.Run("float16_omits_distinct_count", func(t *testing.T) {
		type Entry struct {
			Value string `parquet:"name=value, type=FIXED_LEN_BYTE_ARRAY, length=2, logicaltype=FLOAT16, encoding=RLE_DICTIONARY"`
		}
		pw, _, err := createTestParquetWriter(
			new(Entry),
			WithNP(1),
			WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		)
		require.NoError(t, err)

		// +0.0 and -0.0 are distinct byte keys but compare equal as float16.
		for _, value := range []string{"\x00\x00", "\x00\x80"} {
			require.NoError(t, pw.Write(Entry{Value: value}))
		}
		require.NoError(t, pw.WriteStop())

		require.Nil(t, pw.Footer.RowGroups[0].Columns[0].MetaData.Statistics.DistinctCount)
	})
}

func TestDictionaryDistinctCountGeospatial(t *testing.T) {
	type GeometryEntry struct {
		Geom string `parquet:"name=geom, type=BYTE_ARRAY, logicaltype=GEOMETRY, encoding=RLE_DICTIONARY"`
	}
	type GeographyEntry struct {
		Geom string `parquet:"name=geom, type=BYTE_ARRAY, logicaltype=GEOGRAPHY, encoding=RLE_DICTIONARY"`
	}

	// The same POINT(1 2), in both WKB byte orders.
	wkbPoint := func(order binary.ByteOrder, flag byte, x, y float64) string {
		b := make([]byte, 21)
		b[0] = flag
		order.PutUint32(b[1:], 1)
		order.PutUint64(b[5:], math.Float64bits(x))
		order.PutUint64(b[13:], math.Float64bits(y))
		return string(b)
	}
	littleEndian := wkbPoint(binary.LittleEndian, 1, 1, 2)
	bigEndian := wkbPoint(binary.BigEndian, 0, 1, 2)

	tests := []struct {
		name    string
		obj     any
		entries []any
	}{
		{
			name:    "GEOMETRY",
			obj:     new(GeometryEntry),
			entries: []any{GeometryEntry{Geom: littleEndian}, GeometryEntry{Geom: bigEndian}},
		},
		{
			name:    "GEOGRAPHY",
			obj:     new(GeographyEntry),
			entries: []any{GeographyEntry{Geom: littleEndian}, GeographyEntry{Geom: bigEndian}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+"_omits_distinct_count", func(t *testing.T) {
			pw, _, err := createTestParquetWriter(
				tt.obj,
				WithNP(1),
				WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
			)
			require.NoError(t, err)

			for _, entry := range tt.entries {
				require.NoError(t, pw.Write(entry))
			}
			require.NoError(t, pw.WriteStop())

			column := pw.Footer.RowGroups[0].Columns[0]
			require.Contains(t, column.MetaData.Encodings, parquet.Encoding_RLE_DICTIONARY)
			require.Nil(t, column.MetaData.Statistics.DistinctCount)
		})
	}
}

func TestDictionaryDistinctCountAcrossRowGroups(t *testing.T) {
	type Entry struct {
		Value string `parquet:"name=value, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	}

	pw, _, err := createTestParquetWriter(
		new(Entry),
		WithNP(1),
		WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
	)
	require.NoError(t, err)

	write := func(values ...string) {
		t.Helper()
		for _, value := range values {
			require.NoError(t, pw.Write(Entry{Value: value}))
		}
	}

	write("alpha", "beta", "gamma", "alpha")
	require.NoError(t, pw.Flush(true))
	// "alpha" counts again: each row group has its own dictionary.
	write("alpha", "delta", "alpha")
	require.NoError(t, pw.WriteStop())

	require.Len(t, pw.Footer.RowGroups, 2)
	for i, expected := range []int64{3, 2} {
		stats := pw.Footer.RowGroups[i].Columns[0].MetaData.Statistics
		require.NotNil(t, stats.DistinctCount, "row group %d", i)
		require.Equal(t, expected, *stats.DistinctCount, "row group %d", i)
	}
}
