package layout

import (
	"math"
	"unicode/utf8"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func truncateBinaryBounds(minValue, maxValue []byte, length int) (min, max []byte, minExact, maxExact bool) {
	if length <= 0 {
		return minValue, maxValue, true, true
	}
	min, minExact = truncateBinaryMin(minValue, length)
	max, maxExact = truncateBinaryMax(maxValue, length)
	return min, max, minExact, maxExact
}

func truncateRawBinaryBounds(minValue, maxValue []byte, length int) (min, max []byte, minExact, maxExact bool) {
	if length <= 0 {
		return minValue, maxValue, true, true
	}
	min, minExact = minValue, true
	if len(minValue) > length {
		min = append([]byte(nil), minValue[:length]...)
		minExact = false
	}
	max, maxExact = maxValue, true
	if len(maxValue) > length {
		if incremented := incrementBinary(append([]byte(nil), maxValue[:length]...)); incremented != nil {
			max = incremented
			maxExact = false
		}
	}
	return min, max, minExact, maxExact
}

func truncateBinaryMin(value []byte, length int) ([]byte, bool) {
	if len(value) <= length {
		return value, true
	}
	if !utf8.Valid(value) {
		return value, true
	}
	truncated := append([]byte(nil), value[:length]...)
	for len(truncated) > 0 && !utf8.Valid(truncated) {
		truncated = truncated[:len(truncated)-1]
	}
	if len(truncated) == 0 {
		return value, true
	}
	return truncated, false
}

func truncateBinaryMax(value []byte, length int) ([]byte, bool) {
	if len(value) <= length {
		return value, true
	}
	if !utf8.Valid(value) {
		return value, true
	}
	truncated := append([]byte(nil), value[:length]...)
	for len(truncated) > 0 && !utf8.Valid(truncated) {
		truncated = truncated[:len(truncated)-1]
	}
	if len(truncated) == 0 {
		return value, true
	}
	if incremented := incrementUTF8(truncated); incremented != nil {
		return incremented, false
	}
	return value, true
}

func incrementBinary(value []byte) []byte {
	for i := len(value) - 1; i >= 0; i-- {
		value[i]++
		if value[i] != 0 {
			return value
		}
	}
	return nil
}

func incrementUTF8(value []byte) []byte {
	for i := len(value) - 1; i >= 0; i-- {
		original := value[i]
		for candidate := int(original) + 1; candidate <= 0xff; candidate++ {
			value[i] = byte(candidate)
			if utf8.Valid(value) {
				return value
			}
		}
		value[i] = original
	}
	return nil
}

type binaryTruncationMode uint8

const (
	binaryTruncationDisabled binaryTruncationMode = iota
	binaryTruncationRaw
	binaryTruncationUTF8
)

func binaryStatisticsTruncationMode(schema *parquet.SchemaElement) binaryTruncationMode {
	if schema == nil || schema.Type == nil ||
		(*schema.Type != parquet.Type_BYTE_ARRAY && *schema.Type != parquet.Type_FIXED_LEN_BYTE_ARRAY) {
		return binaryTruncationDisabled
	}
	if schema.LogicalType != nil {
		if *schema.Type == parquet.Type_BYTE_ARRAY && schema.LogicalType.STRING != nil &&
			(schema.ConvertedType == nil || *schema.ConvertedType == parquet.ConvertedType_UTF8) {
			return binaryTruncationUTF8
		}
		return binaryTruncationDisabled
	}
	if schema.ConvertedType != nil {
		if *schema.Type == parquet.Type_BYTE_ARRAY && *schema.ConvertedType == parquet.ConvertedType_UTF8 {
			return binaryTruncationUTF8
		}
		return binaryTruncationDisabled
	}
	return binaryTruncationRaw
}

// TruncateBinaryStatistics shortens eligible binary min/max values while
// preserving safe lower and upper bounds. Unsupported logical values and
// bounds without a valid shortened representation are left intact.
func TruncateBinaryStatistics(stats *parquet.Statistics, schema *parquet.SchemaElement, length int) {
	if stats == nil {
		return
	}
	mode := binaryStatisticsTruncationMode(schema)
	if mode == binaryTruncationDisabled {
		if stats.MinValue != nil {
			stats.IsMinValueExact = common.ToPtr(true)
		}
		if stats.MaxValue != nil {
			stats.IsMaxValueExact = common.ToPtr(true)
		}
		return
	}

	hasMin, hasMax := stats.MinValue != nil, stats.MaxValue != nil
	var min, max []byte
	var minExact, maxExact bool
	if mode == binaryTruncationUTF8 {
		min, max, minExact, maxExact = truncateBinaryBounds(stats.MinValue, stats.MaxValue, length)
	} else {
		min, max, minExact, maxExact = truncateRawBinaryBounds(stats.MinValue, stats.MaxValue, length)
	}
	stats.MinValue = min
	stats.MaxValue = max
	if hasMin {
		stats.IsMinValueExact = common.ToPtr(minExact)
	}
	if hasMax {
		stats.IsMaxValueExact = common.ToPtr(maxExact)
	}
	if stats.Min != nil {
		stats.Min = min
	}
	if stats.Max != nil {
		stats.Max = max
	}
}

// TruncateBinaryBounds shortens eligible binary bounds for a column. Bounds
// that cannot be safely shortened are returned unchanged.
func TruncateBinaryBounds(schema *parquet.SchemaElement, minValue, maxValue []byte, length int) (min, max []byte) {
	switch binaryStatisticsTruncationMode(schema) {
	case binaryTruncationRaw:
		min, max, _, _ = truncateRawBinaryBounds(minValue, maxValue, length)
		return min, max
	case binaryTruncationUTF8:
		min, max, _, _ = truncateBinaryBounds(minValue, maxValue, length)
		return min, max
	default:
		return minValue, maxValue
	}
}

// dictionaryDistinctCount returns the distinct non-null value count of a dictionary chunk, nil when inexact.
func dictionaryDistinctCount(pages []*Page, pT *parquet.Type) *int64 {
	if len(pages) == 0 || pages[0] == nil || pages[0].Header == nil || pages[0].Header.DictionaryPageHeader == nil {
		return nil
	}
	// Values of a page that fell back to PLAIN, as happens once the dictionary hits its size limit,
	// never reach the dictionary, leaving its size a lower bound instead of the exact count.
	for _, page := range pages[1:] {
		if page == nil || page.Header == nil {
			return nil
		}
		switch {
		case page.Header.DataPageHeader != nil:
			if page.Header.DataPageHeader.Encoding != parquet.Encoding_RLE_DICTIONARY {
				return nil
			}
		case page.Header.DataPageHeaderV2 != nil:
			if page.Header.DataPageHeaderV2.Encoding != parquet.Encoding_RLE_DICTIONARY {
				return nil
			}
		default:
			return nil
		}
	}
	if pT != nil && (*pT == parquet.Type_FLOAT || *pT == parquet.Type_DOUBLE) && !nanFreeFloatDictionary(pages[0]) {
		return nil
	}
	return common.ToPtr(int64(pages[0].Header.DictionaryPageHeader.NumValues))
}

// nanFreeFloatDictionary reports whether a FLOAT/DOUBLE dictionary page is known to hold no NaN.
func nanFreeFloatDictionary(dictPage *Page) bool {
	// Dedup goes through Go map equality, which never matches NaN, so each NaN takes an entry of its
	// own; a page carrying no values cannot be checked.
	if dictPage.DataTable == nil {
		return false
	}
	for _, value := range dictPage.DataTable.Values {
		switch typed := value.(type) {
		case float32:
			if math.IsNaN(float64(typed)) {
				return false
			}
		case float64:
			if math.IsNaN(typed) {
				return false
			}
		}
	}
	return true
}
