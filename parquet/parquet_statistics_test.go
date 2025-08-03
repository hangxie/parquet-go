package parquet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Statistics(t *testing.T) {
	stats := NewStatistics()
	require.NotNil(t, stats)

	maxVal := []byte("max")
	minVal := []byte("min")
	nullCount := int64(10)
	distinctCount := int64(100)
	maxValue := []byte("maxvalue")
	minValue := []byte("minvalue")

	stats.Max = maxVal
	stats.Min = minVal
	stats.NullCount = &nullCount
	stats.DistinctCount = &distinctCount
	stats.MaxValue = maxValue
	stats.MinValue = minValue
	require.Equal(t, maxVal, stats.GetMax())
	require.Equal(t, minVal, stats.GetMin())
	require.Equal(t, nullCount, stats.GetNullCount())
	require.Equal(t, distinctCount, stats.GetDistinctCount())
	require.Equal(t, maxValue, stats.GetMaxValue())
	require.Equal(t, minValue, stats.GetMinValue())
	require.True(t, stats.IsSetMax())
	require.True(t, stats.IsSetMin())
	require.True(t, stats.IsSetNullCount())
	require.True(t, stats.IsSetDistinctCount())
	require.True(t, stats.IsSetMaxValue())
	require.True(t, stats.IsSetMinValue())

	str := stats.String()
	require.NotEmpty(t, str)

	stats2 := NewStatistics()
	stats2.Max = maxVal
	stats2.Min = minVal
	stats2.NullCount = &nullCount
	stats2.DistinctCount = &distinctCount
	stats2.MaxValue = maxValue
	stats2.MinValue = minValue
	require.True(t, stats.Equals(stats2))

	differentNullCount := int64(20)
	stats2.NullCount = &differentNullCount
	require.False(t, stats.Equals(stats2))
}

func Test_StatisticsNilFields(t *testing.T) {
	stats := NewStatistics()
	require.Equal(t, int64(0), stats.GetDistinctCount())
}

func Test_Statistics_GetNullCount_EdgeCase(t *testing.T) {
	stats := NewStatistics()
	stats.NullCount = nil

	result := stats.GetNullCount()
	require.Equal(t, int64(0), result)
}

func Test_PageLocation(t *testing.T) {
	pl := NewPageLocation()
	require.NotNil(t, pl)

	offset := int64(4096)
	pl.Offset = offset
	require.Equal(t, offset, pl.GetOffset())

	compressedSize := int32(1024)
	pl.CompressedPageSize = compressedSize
	require.Equal(t, compressedSize, pl.GetCompressedPageSize())

	firstRowIndex := int64(500)
	pl.FirstRowIndex = firstRowIndex
	require.Equal(t, firstRowIndex, pl.GetFirstRowIndex())

	str := pl.String()
	require.NotEmpty(t, str)
	require.Contains(t, str, "PageLocation")

	pl2 := NewPageLocation()
	pl2.Offset = offset
	pl2.CompressedPageSize = compressedSize
	pl2.FirstRowIndex = firstRowIndex
	require.True(t, pl.Equals(pl2))

	pl3 := NewPageLocation()
	pl3.Offset = offset + 1
	pl3.CompressedPageSize = compressedSize
	pl3.FirstRowIndex = firstRowIndex
	require.False(t, pl.Equals(pl3))
}
