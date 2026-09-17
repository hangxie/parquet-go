package types

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestDECIMAL(t *testing.T) {
	a1, _ := StrToParquetType("1.23", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 2)
	sa1 := DECIMAL_INT_ToString(int64(a1.(int32)), 9, 2)
	require.Equal(t, "1.23", sa1)

	a2, _ := StrToParquetType("1.230", parquet.TypePtr(parquet.Type_INT64), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 3)
	sa2 := DECIMAL_INT_ToString(int64(a2.(int64)), 9, 3)
	require.Equal(t, "1.230", sa2)

	a3, _ := StrToParquetType("11.230", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 3)
	sa3 := DECIMAL_BYTE_ARRAY_ToString([]byte(a3.(string)), 9, 3)
	require.Equal(t, "11.230", sa3)

	a4, _ := StrToParquetType("-123.456", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 3)
	sa4 := DECIMAL_BYTE_ARRAY_ToString([]byte(a4.(string)), 9, 3)
	require.Equal(t, "-123.456", sa4)

	a5, _ := StrToParquetType("0.000", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 3)
	sa5 := DECIMAL_BYTE_ARRAY_ToString([]byte(a5.(string)), 9, 3)
	require.Equal(t, "0.000", sa5)

	a6, _ := StrToParquetType("-0.01", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 6, 2)
	sa6 := DECIMAL_BYTE_ARRAY_ToString([]byte(a6.(string)), 6, 2)
	require.Equal(t, "-0.01", sa6)

	a7, _ := StrToParquetType("0.1234", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa7 := DECIMAL_BYTE_ARRAY_ToString([]byte(a7.(string)), 8, 4)
	require.Equal(t, "0.1234", sa7)

	a8, _ := StrToParquetType("-12.345", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 0, 3)
	sa8 := DECIMAL_INT_ToString(int64(a8.(int32)), 0, 3)
	require.Equal(t, "-12.345", sa8)

	a9, _ := StrToParquetType("-0.001", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 0, 3)
	sa9 := DECIMAL_INT_ToString(int64(a9.(int32)), 0, 3)
	require.Equal(t, "-0.001", sa9)

	a10, _ := StrToParquetType("0.0001", parquet.TypePtr(parquet.Type_INT32), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 0, 4)
	sa10 := DECIMAL_INT_ToString(int64(a10.(int32)), 0, 4)
	require.Equal(t, "0.0001", sa10)

	a11, _ := StrToParquetType("-100000", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa11 := DECIMAL_BYTE_ARRAY_ToString([]byte(a11.(string)), 8, 4)
	require.Equal(t, "-100000.0000", sa11)

	a12, _ := StrToParquetType("100000", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa12 := DECIMAL_BYTE_ARRAY_ToString([]byte(a12.(string)), 8, 4)
	require.Equal(t, "100000.0000", sa12)

	a13, _ := StrToParquetType("-100", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa13 := DECIMAL_BYTE_ARRAY_ToString([]byte(a13.(string)), 8, 4)
	require.Equal(t, "-100.0000", sa13)

	a14, _ := StrToParquetType("100", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa14 := DECIMAL_BYTE_ARRAY_ToString([]byte(a14.(string)), 8, 4)
	require.Equal(t, "100.0000", sa14)

	a15, _ := StrToParquetType("-431", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa15 := DECIMAL_BYTE_ARRAY_ToString([]byte(a15.(string)), 8, 4)
	require.Equal(t, "-431.0000", sa15)

	a16, _ := StrToParquetType("431", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa16 := DECIMAL_BYTE_ARRAY_ToString([]byte(a16.(string)), 8, 4)
	require.Equal(t, "431.0000", sa16)

	a17, _ := StrToParquetType("-432", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa17 := DECIMAL_BYTE_ARRAY_ToString([]byte(a17.(string)), 8, 4)
	require.Equal(t, "-432.0000", sa17)

	a18, _ := StrToParquetType("432", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa18 := DECIMAL_BYTE_ARRAY_ToString([]byte(a18.(string)), 8, 4)
	require.Equal(t, "432.0000", sa18)

	a19, _ := StrToParquetType("-433", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa19 := DECIMAL_BYTE_ARRAY_ToString([]byte(a19.(string)), 8, 4)
	require.Equal(t, "-433.0000", sa19)

	a20, _ := StrToParquetType("433", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa20 := DECIMAL_BYTE_ARRAY_ToString([]byte(a20.(string)), 8, 4)
	require.Equal(t, "433.0000", sa20)

	a21, _ := StrToParquetType("-65535", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa21 := DECIMAL_BYTE_ARRAY_ToString([]byte(a21.(string)), 8, 4)
	require.Equal(t, "-65535.0000", sa21)

	a22, _ := StrToParquetType("65535", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 8, 4)
	sa22 := DECIMAL_BYTE_ARRAY_ToString([]byte(a22.(string)), 8, 4)
	require.Equal(t, "65535.0000", sa22)
}

func TestDECIMAL_BYTE_ARRAY_ToString_EmptyInput(t *testing.T) {
	// Test that DECIMAL_BYTE_ARRAY_ToString handles empty input without panicking
	result := DECIMAL_BYTE_ARRAY_ToString([]byte{}, 9, 3)
	require.Equal(t, "0.000", result)

	result = DECIMAL_BYTE_ARRAY_ToString(nil, 9, 3)
	require.Equal(t, "0.000", result)
}

func TestDECIMAL_BYTE_ARRAY_ToString_DoesNotMutateInput(t *testing.T) {
	// Test that DECIMAL_BYTE_ARRAY_ToString does not mutate the input slice
	// This is important because callers may reuse the slice

	// Create a negative decimal value that will trigger the XOR operation
	// -123.456 in decimal with scale 3
	original, _ := StrToParquetType("-123.456", parquet.TypePtr(parquet.Type_BYTE_ARRAY), parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL), 9, 3)
	input := []byte(original.(string))

	// Make a copy to compare against later
	inputCopy := make([]byte, len(input))
	copy(inputCopy, input)

	// Call the function
	result := DECIMAL_BYTE_ARRAY_ToString(input, 9, 3)

	// Verify the result is correct
	require.Equal(t, "-123.456", result)

	// Verify the input slice was NOT mutated
	require.Equal(t, inputCopy, input, "DECIMAL_BYTE_ARRAY_ToString should not mutate input slice")
}

func TestDecimalExactWrite(t *testing.T) {
	tests := []struct {
		name      string
		s         string
		pT        *parquet.Type
		precision int32
		scale     int32
		length    int
		expected  any
		errMsg    string
	}{
		{
			name:      "int64_full_18_digits",
			s:         "999999999999999999",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     0,
			expected:  int64(999999999999999999),
		},
		{
			name:      "int64_full_18_digits_scaled",
			s:         "9999999999999999.99",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			expected:  int64(999999999999999999),
		},
		{
			name:      "int32_full_9_digits",
			s:         "9999999.99",
			pT:        parquet.TypePtr(parquet.Type_INT32),
			precision: 9,
			scale:     2,
			expected:  int32(999999999),
		},
		{
			name:      "flba_38_digits",
			s:         "123456789012345678901234.56",
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			precision: 38,
			scale:     2,
			length:    16,
			expected:  StrIntToBinary("12345678901234567890123456", "BigEndian", 16, true),
		},
		{
			name:      "byte_array_38_digits_negative",
			s:         "-123456789012345678901234.56",
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			precision: 38,
			scale:     2,
			expected:  StrIntToBinary("-12345678901234567890123456", "BigEndian", 0, true),
		},
		{
			name:      "exponent_form",
			s:         "1.2345e3",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			expected:  int64(123450),
		},
		{
			name:      "signed_exponent",
			s:         "1.2345e+3",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			expected:  int64(123450),
		},
		{
			name:      "negative_exponent",
			s:         "1234500E-3",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			expected:  int64(123450),
		},
		{
			name:      "extra_digits_round_half_away_from_zero",
			s:         "1.235",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			expected:  int64(124),
		},
		{
			name:      "extra_digits_round_negative",
			s:         "-1.235",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			expected:  int64(-124),
		},
		{
			name:      "leading_radix_point",
			s:         ".5",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			expected:  int64(50),
		},
		{
			name:      "trailing_radix_point",
			s:         "12.",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			expected:  int64(1200),
		},
		{
			name:      "surrounding_space",
			s:         "  12.34  ",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			expected:  int64(1234),
		},
		{
			name:      "malformed",
			s:         "not-a-number",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			errMsg:    "parse DECIMAL",
		},
		{
			name:      "fraction",
			s:         "1/2",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			errMsg:    "parse DECIMAL",
		},
		{
			name:      "hexadecimal",
			s:         "0x10",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			errMsg:    "parse DECIMAL",
		},
		{
			name:      "binary",
			s:         "0b101",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			errMsg:    "parse DECIMAL",
		},
		{
			name:      "digit_separators",
			s:         "1_000",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			errMsg:    "parse DECIMAL",
		},
		{
			name:      "empty",
			s:         "",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			errMsg:    "parse DECIMAL",
		},
		{
			name:      "radix_point_only",
			s:         ".",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			errMsg:    "parse DECIMAL",
		},
		{
			name:      "exponent_without_digits",
			s:         "1.5e",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			errMsg:    "parse DECIMAL",
		},
		{
			name:      "trailing_garbage",
			s:         "1.5x",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			errMsg:    "parse DECIMAL",
		},
		{
			name:      "exponent_out_of_range",
			s:         "1e999999999999999999999999999",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			errMsg:    "exponent out of range",
		},
		{
			name:      "not_a_number_literal",
			s:         "NaN",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			errMsg:    "parse DECIMAL",
		},
		{
			name:      "exceeds_declared_precision",
			s:         "99.99",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 3,
			scale:     2,
			errMsg:    `DECIMAL "99.99" exceeds precision 3`,
		},
		{
			name:      "fills_declared_precision",
			s:         "9.99",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 3,
			scale:     2,
			expected:  int64(999),
		},
		{
			name:      "exceeds_declared_precision_negative",
			s:         "-99.99",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 3,
			scale:     2,
			errMsg:    "exceeds precision 3",
		},
		{
			name:      "exceeds_declared_precision_byte_array",
			s:         "123456789012345678901234.56",
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			precision: 20,
			scale:     2,
			errMsg:    "exceeds precision 20",
		},
		{
			name:      "int64_overflow",
			s:         "99999999999999999999999",
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 23,
			scale:     0,
			errMsg:    "does not fit in INT64",
		},
		{
			name:      "int32_overflow",
			s:         "99999999999",
			pT:        parquet.TypePtr(parquet.Type_INT32),
			precision: 11,
			scale:     0,
			errMsg:    "does not fit in INT32",
		},
		{
			name:      "flba_too_narrow",
			s:         "123456789012345678901234.56",
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			precision: 38,
			scale:     2,
			length:    8,
			errMsg:    "does not fit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lT := createDecimalLogicalType(tt.precision, tt.scale)
			got, err := StrToParquetTypeWithLogical(tt.s, tt.pT, nil, lT, tt.length, int(tt.scale))
			if tt.errMsg != "" {
				require.ErrorContains(t, err, tt.errMsg)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, got)

			// the ConvertedType path must agree with the LogicalType path
			cT := parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)
			gotCT, err := StrToParquetType(tt.s, tt.pT, cT, tt.length, int(tt.scale))
			require.NoError(t, err)
			require.Equal(t, tt.expected, gotCT)
		})
	}
}

func TestDecimalConvertedTypePrecisionUnchecked(t *testing.T) {
	// A ConvertedType DECIMAL carries no precision, so the digit count cannot be
	// checked on that path; schemas built from a tag always get the LogicalType too.
	pT := parquet.TypePtr(parquet.Type_INT64)
	cT := parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)

	got, err := StrToParquetType("99.99", pT, cT, 0, 2)
	require.NoError(t, err)
	require.Equal(t, int64(9999), got)

	_, err = StrToParquetTypeWithLogical("99.99", pT, cT, createDecimalLogicalType(3, 2), 0, 2)
	require.ErrorContains(t, err, "exceeds precision 3")
}

func TestDecimalExactRead(t *testing.T) {
	tests := []struct {
		name      string
		val       any
		pT        *parquet.Type
		precision int
		scale     int
		expected  any
	}{
		{
			name:      "int32",
			val:       int32(12345),
			pT:        parquet.TypePtr(parquet.Type_INT32),
			precision: 9,
			scale:     2,
			expected:  json.Number("123.45"),
		},
		{
			name:      "int64_beyond_float64",
			val:       int64(999999999999999999),
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     0,
			expected:  json.Number("999999999999999999"),
		},
		{
			name:      "int64_scaled_beyond_float64",
			val:       int64(999999999999999999),
			pT:        parquet.TypePtr(parquet.Type_INT64),
			precision: 18,
			scale:     2,
			expected:  json.Number("9999999999999999.99"),
		},
		{
			name:      "flba_38_digits",
			val:       StrIntToBinary("12345678901234567890123456", "BigEndian", 16, true),
			pT:        parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY),
			precision: 38,
			scale:     2,
			expected:  json.Number("123456789012345678901234.56"),
		},
		{
			name:      "byte_array_bytes_input",
			val:       []byte(StrIntToBinary("12345", "BigEndian", 0, true)),
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			precision: 10,
			scale:     2,
			expected:  json.Number("123.45"),
		},
		{
			name:      "byte_array_unpadded",
			val:       string([]byte{0x01, 0x23, 0x45}),
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			precision: 5,
			scale:     2,
			expected:  json.Number("745.65"),
		},
		{
			name:      "byte_array_negative_one_unscaled",
			val:       string([]byte{0xFF, 0xFF, 0xFF, 0xFF}),
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			precision: 10,
			scale:     5,
			expected:  json.Number("-0.00001"),
		},
		{
			name:      "byte_array_negative",
			val:       StrIntToBinary("-12345678901234567890123456", "BigEndian", 0, true),
			pT:        parquet.TypePtr(parquet.Type_BYTE_ARRAY),
			precision: 38,
			scale:     2,
			expected:  json.Number("-123456789012345678901234.56"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ConvertDecimalValue(tt.val, tt.pT, tt.precision, tt.scale)
			require.Equal(t, tt.expected, got)

			// the JSON form must be a bare number carrying every digit
			buf, err := json.Marshal(got)
			require.NoError(t, err)
			require.Equal(t, string(tt.expected.(json.Number)), string(buf))
		})
	}
}

func TestDecimalUnsupportedValue(t *testing.T) {
	// a value whose Go type does not back the physical type passes through untouched
	pT := parquet.TypePtr(parquet.Type_BYTE_ARRAY)
	require.Equal(t, float32(123.45), ConvertDecimalValue(float32(123.45), pT, 10, 2))
}

func TestDecimalRoundTrip(t *testing.T) {
	values := []struct {
		s         string
		pT        *parquet.Type
		precision int32
		scale     int32
		length    int
	}{
		{"999999999999999999", parquet.TypePtr(parquet.Type_INT64), 18, 0, 0},
		{"-9999999999999999.99", parquet.TypePtr(parquet.Type_INT64), 18, 2, 0},
		{"9999999.99", parquet.TypePtr(parquet.Type_INT32), 9, 2, 0},
		{"123456789012345678901234.56", parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), 38, 2, 16},
		{"-99999999999999999999999999999999999.999", parquet.TypePtr(parquet.Type_BYTE_ARRAY), 38, 3, 0},
		{"0.00", parquet.TypePtr(parquet.Type_BYTE_ARRAY), 38, 2, 0},
	}

	for _, v := range values {
		t.Run(v.s, func(t *testing.T) {
			lT := createDecimalLogicalType(v.precision, v.scale)
			stored, err := StrToParquetTypeWithLogical(v.s, v.pT, nil, lT, v.length, int(v.scale))
			require.NoError(t, err)
			got := ConvertDecimalValue(stored, v.pT, int(v.precision), int(v.scale))
			require.Equal(t, json.Number(v.s), got)
		})
	}
}

// TestDecimalByteArrayPaddingCanonicalizes pins the one documented exception to the exact
// physical round trip in TestPhysicalRoundTrip: a BYTE_ARRAY DECIMAL may carry redundant
// sign-extension bytes, and the value comes back in its minimal encoding instead.
func TestDecimalByteArrayPaddingCanonicalizes(t *testing.T) {
	se := parquet.NewSchemaElement()
	se.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
	se.LogicalType = createDecimalLogicalType(38, 3)

	minimal := StrIntToBinary("-99999999999999999999999999999999999999", "BigEndian", 16, true)
	padded := "\xff" + minimal
	require.Equal(t, ConvertToJSONType(minimal, se), ConvertToJSONType(padded, se))

	got, err := StrToParquetTypeWithLogical(fmt.Sprintf("%v", ConvertToJSONType(padded, se)),
		se.Type, nil, se.LogicalType, 0, 3)
	require.NoError(t, err)
	require.Equal(t, minimal, got)
}

// TestDecimalDigitsEqualScale covers an unscaled value with exactly as many digits as the
// scale, where the radix point lands at position zero: the leading zero is part of the
// number, and a json.Number without it is one json.Marshal refuses.
func TestDecimalDigitsEqualScale(t *testing.T) {
	tests := []struct {
		name     string
		unscaled int64
		scale    int
		expected string
	}{
		{"digits equal scale", 92, 2, "0.92"},
		{"digits equal scale, negative", -92, 2, "-0.92"},
		{"one digit, scale one", 5, 1, "0.5"},
		{"digits below scale", 5, 2, "0.05"},
		{"digits above scale", 123, 2, "1.23"},
		{"widest", 999999999999999999, 18, "0.999999999999999999"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expected, DECIMAL_INT_ToString(tt.unscaled, 18, tt.scale))
			require.Equal(t, tt.expected,
				DECIMAL_BYTE_ARRAY_ToString([]byte(StrIntToBinary(strconv.FormatInt(tt.unscaled, 10), "BigEndian", 16, true)), 18, tt.scale))

			rendered, err := ConvertValue(tt.unscaled, &parquet.SchemaElement{
				Type: parquet.TypePtr(parquet.Type_INT64),
				LogicalType: &parquet.LogicalType{
					DECIMAL: &parquet.DecimalType{Precision: 18, Scale: int32(tt.scale)},
				},
			})
			require.NoError(t, err)
			require.Equal(t, json.Number(tt.expected), rendered)
			// A rendering the JSON encoder refuses is one no caller can use.
			marshalled, err := json.Marshal(rendered)
			require.NoError(t, err)
			require.Equal(t, tt.expected, string(marshalled))
		})
	}
}
