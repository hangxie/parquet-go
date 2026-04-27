package types

import (
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
