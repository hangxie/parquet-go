package parquet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Type_String(t *testing.T) {
	tests := []struct {
		typ      Type
		expected string
	}{
		{Type_BOOLEAN, "BOOLEAN"},
		{Type_INT32, "INT32"},
		{Type_INT64, "INT64"},
		{Type_INT96, "INT96"},
		{Type_FLOAT, "FLOAT"},
		{Type_DOUBLE, "DOUBLE"},
		{Type_BYTE_ARRAY, "BYTE_ARRAY"},
		{Type_FIXED_LEN_BYTE_ARRAY, "FIXED_LEN_BYTE_ARRAY"},
		{Type(999), "<UNSET>"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			got := test.typ.String()
			require.Equal(t, test.expected, got)
		})
	}
}

func Test_Type_FromString(t *testing.T) {
	tests := []struct {
		input    string
		expected Type
		hasError bool
	}{
		{"BOOLEAN", Type_BOOLEAN, false},
		{"INT32", Type_INT32, false},
		{"INT64", Type_INT64, false},
		{"INT96", Type_INT96, false},
		{"FLOAT", Type_FLOAT, false},
		{"DOUBLE", Type_DOUBLE, false},
		{"BYTE_ARRAY", Type_BYTE_ARRAY, false},
		{"FIXED_LEN_BYTE_ARRAY", Type_FIXED_LEN_BYTE_ARRAY, false},
		{"INVALID", Type(0), true},
		{"", Type(0), true},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := TypeFromString(test.input)
			if test.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, got)
			}
		})
	}
}

func Test_Type_Ptr(t *testing.T) {
	typ := Type_INT32
	ptr := TypePtr(typ)
	require.Equal(t, typ, *ptr)
}

func Test_Type_MarshalText(t *testing.T) {
	typ := Type_BOOLEAN
	bytes, err := typ.MarshalText()
	require.NoError(t, err)
	require.Equal(t, "BOOLEAN", string(bytes))
}

func Test_Type_UnmarshalText(t *testing.T) {
	var typ Type
	err := typ.UnmarshalText([]byte("INT32"))
	require.NoError(t, err)
	require.Equal(t, Type_INT32, typ)

	err = typ.UnmarshalText([]byte("INVALID"))
	require.Error(t, err)
}

func Test_Type_Scan(t *testing.T) {
	var typ Type

	err := typ.Scan(int64(Type_INT64))
	require.NoError(t, err)
	require.Equal(t, Type_INT64, typ)

	err = typ.Scan(int64(Type_FLOAT))
	require.NoError(t, err)
	require.Equal(t, Type_FLOAT, typ)

	err = typ.Scan("invalid")
	require.Error(t, err)
}

func Test_Type_Value(t *testing.T) {
	typ := Type_DOUBLE
	val, err := typ.Value()
	require.NoError(t, err)
	require.Equal(t, int64(Type_DOUBLE), val)
}

func Test_ConvertedType_String(t *testing.T) {
	tests := []struct {
		typ      ConvertedType
		expected string
	}{
		{ConvertedType_UTF8, "UTF8"},
		{ConvertedType_MAP, "MAP"},
		{ConvertedType_MAP_KEY_VALUE, "MAP_KEY_VALUE"},
		{ConvertedType_LIST, "LIST"},
		{ConvertedType_ENUM, "ENUM"},
		{ConvertedType_DECIMAL, "DECIMAL"},
		{ConvertedType_DATE, "DATE"},
		{ConvertedType_TIME_MILLIS, "TIME_MILLIS"},
		{ConvertedType_TIME_MICROS, "TIME_MICROS"},
		{ConvertedType_TIMESTAMP_MILLIS, "TIMESTAMP_MILLIS"},
		{ConvertedType_TIMESTAMP_MICROS, "TIMESTAMP_MICROS"},
		{ConvertedType_UINT_8, "UINT_8"},
		{ConvertedType_UINT_16, "UINT_16"},
		{ConvertedType_UINT_32, "UINT_32"},
		{ConvertedType_UINT_64, "UINT_64"},
		{ConvertedType_INT_8, "INT_8"},
		{ConvertedType_INT_16, "INT_16"},
		{ConvertedType_INT_32, "INT_32"},
		{ConvertedType_INT_64, "INT_64"},
		{ConvertedType_JSON, "JSON"},
		{ConvertedType_BSON, "BSON"},
		{ConvertedType_INTERVAL, "INTERVAL"},
		{ConvertedType(999), "<UNSET>"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			got := test.typ.String()
			require.Equal(t, test.expected, got)
		})
	}
}

func Test_ConvertedType_FromString(t *testing.T) {
	tests := []struct {
		input    string
		expected ConvertedType
		hasError bool
	}{
		{"UTF8", ConvertedType_UTF8, false},
		{"MAP", ConvertedType_MAP, false},
		{"MAP_KEY_VALUE", ConvertedType_MAP_KEY_VALUE, false},
		{"LIST", ConvertedType_LIST, false},
		{"ENUM", ConvertedType_ENUM, false},
		{"DECIMAL", ConvertedType_DECIMAL, false},
		{"DATE", ConvertedType_DATE, false},
		{"TIME_MILLIS", ConvertedType_TIME_MILLIS, false},
		{"TIME_MICROS", ConvertedType_TIME_MICROS, false},
		{"TIMESTAMP_MILLIS", ConvertedType_TIMESTAMP_MILLIS, false},
		{"TIMESTAMP_MICROS", ConvertedType_TIMESTAMP_MICROS, false},
		{"UINT_8", ConvertedType_UINT_8, false},
		{"UINT_16", ConvertedType_UINT_16, false},
		{"UINT_32", ConvertedType_UINT_32, false},
		{"UINT_64", ConvertedType_UINT_64, false},
		{"INT_8", ConvertedType_INT_8, false},
		{"INT_16", ConvertedType_INT_16, false},
		{"INT_32", ConvertedType_INT_32, false},
		{"INT_64", ConvertedType_INT_64, false},
		{"JSON", ConvertedType_JSON, false},
		{"BSON", ConvertedType_BSON, false},
		{"INTERVAL", ConvertedType_INTERVAL, false},
		{"INVALID", ConvertedType(0), true},
		{"", ConvertedType(0), true},
	}

	for _, test := range tests {
		testName := test.input
		if testName == "" {
			testName = "empty_string"
		}
		t.Run(testName, func(t *testing.T) {
			got, err := ConvertedTypeFromString(test.input)
			if test.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, got)
			}
		})
	}
}

func Test_ConvertedType_Ptr(t *testing.T) {
	ct := ConvertedType_UTF8
	ptr := ConvertedTypePtr(ct)
	require.Equal(t, ct, *ptr)
}

func Test_ConvertedType_MarshalText(t *testing.T) {
	ct := ConvertedType_JSON
	bytes, err := ct.MarshalText()
	require.NoError(t, err)
	require.Equal(t, "JSON", string(bytes))
}

func Test_ConvertedType_UnmarshalText(t *testing.T) {
	var ct ConvertedType
	err := ct.UnmarshalText([]byte("DECIMAL"))
	require.NoError(t, err)
	require.Equal(t, ConvertedType_DECIMAL, ct)

	err = ct.UnmarshalText([]byte("INVALID"))
	require.Error(t, err)
}

func Test_ConvertedType_Scan(t *testing.T) {
	var ct ConvertedType
	err := ct.Scan(int64(ConvertedType_ENUM))
	require.NoError(t, err)
	require.Equal(t, ConvertedType_ENUM, ct)

	err = ct.Scan("invalid")
	require.Error(t, err)
}

func Test_ConvertedType_Value(t *testing.T) {
	ct := ConvertedType_LIST
	val, err := ct.Value()
	require.NoError(t, err)
	require.Equal(t, int64(ConvertedType_LIST), val)
}

func Test_FieldRepetitionType_String(t *testing.T) {
	tests := []struct {
		typ      FieldRepetitionType
		expected string
	}{
		{FieldRepetitionType_REQUIRED, "REQUIRED"},
		{FieldRepetitionType_OPTIONAL, "OPTIONAL"},
		{FieldRepetitionType_REPEATED, "REPEATED"},
		{FieldRepetitionType(999), "<UNSET>"},
	}

	for _, test := range tests {
		t.Run(test.expected, func(t *testing.T) {
			got := test.typ.String()
			require.Equal(t, test.expected, got)
		})
	}
}

func Test_FieldRepetitionType_FromString(t *testing.T) {
	tests := []struct {
		input    string
		expected FieldRepetitionType
		hasError bool
	}{
		{"REQUIRED", FieldRepetitionType_REQUIRED, false},
		{"OPTIONAL", FieldRepetitionType_OPTIONAL, false},
		{"REPEATED", FieldRepetitionType_REPEATED, false},
		{"INVALID", FieldRepetitionType(0), true},
	}

	for _, test := range tests {
		testName := test.input
		if testName == "" {
			testName = "empty_string"
		}
		t.Run(testName, func(t *testing.T) {
			got, err := FieldRepetitionTypeFromString(test.input)
			if test.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expected, got)
			}
		})
	}
}

func Test_FieldRepetitionType_Ptr(t *testing.T) {
	frt := FieldRepetitionType_REQUIRED
	ptr := FieldRepetitionTypePtr(frt)
	require.Equal(t, frt, *ptr)
}

func Test_FieldRepetitionType_MarshalText(t *testing.T) {
	frt := FieldRepetitionType_OPTIONAL
	bytes, err := frt.MarshalText()
	require.NoError(t, err)
	require.Equal(t, "OPTIONAL", string(bytes))
}

func Test_FieldRepetitionType_UnmarshalText(t *testing.T) {
	var frt FieldRepetitionType
	err := frt.UnmarshalText([]byte("REPEATED"))
	require.NoError(t, err)
	require.Equal(t, FieldRepetitionType_REPEATED, frt)

	err = frt.UnmarshalText([]byte("INVALID"))
	require.Error(t, err)
}

func Test_FieldRepetitionType_Scan(t *testing.T) {
	var frt FieldRepetitionType
	err := frt.Scan(int64(FieldRepetitionType_REQUIRED))
	require.NoError(t, err)
	require.Equal(t, FieldRepetitionType_REQUIRED, frt)

	err = frt.Scan("invalid")
	require.Error(t, err)
}

func Test_FieldRepetitionType_Value(t *testing.T) {
	frt := FieldRepetitionType_OPTIONAL
	val, err := frt.Value()
	require.NoError(t, err)
	require.Equal(t, int64(FieldRepetitionType_OPTIONAL), val)
}

func Test_TypeNilPointerValue(t *testing.T) {
	tests := []struct {
		name  string
		getFn func() (interface{}, error)
	}{
		{"Type nil", func() (interface{}, error) { var t *Type; return t.Value() }},
		{"ConvertedType nil", func() (interface{}, error) { var ct *ConvertedType; return ct.Value() }},
		{"FieldRepetitionType nil", func() (interface{}, error) { var frt *FieldRepetitionType; return frt.Value() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := tt.getFn()
			require.NoError(t, err)
			require.Nil(t, val)
		})
	}
}

func Test_TypeScanEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		input       interface{}
		expectError bool
		expected    Type
	}{
		{"string input", "not_an_int", true, Type(0)},
		{"float input", 3.14, true, Type(0)},
		{"nil input", nil, true, Type(0)},
		{"valid int64", int64(Type_BOOLEAN), false, Type_BOOLEAN},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var typ Type
			err := typ.Scan(tt.input)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, typ)
			}
		})
	}
}

func Test_TypeScanErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		getFn func() interface{ Scan(interface{}) error }
		input interface{}
	}{
		{"ConvertedType string", func() interface{ Scan(interface{}) error } { var c ConvertedType; return &c }, "invalid"},
		{"ConvertedType float", func() interface{ Scan(interface{}) error } { var c ConvertedType; return &c }, 3.14},
		{"FieldRepetitionType string", func() interface{ Scan(interface{}) error } { var f FieldRepetitionType; return &f }, "invalid"},
		{"FieldRepetitionType bytes", func() interface{ Scan(interface{}) error } { var f FieldRepetitionType; return &f }, []byte("bytes")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := tt.getFn()
			err := target.Scan(tt.input)
			require.Error(t, err)
		})
	}
}

func Test_TypeUnmarshalTextErrors(t *testing.T) {
	invalidInputs := [][]byte{
		[]byte(""),
		[]byte("COMPLETELY_INVALID"),
		[]byte("123"),
		[]byte("special!@#$%"),
		[]byte(" LEADING_SPACE"),
		[]byte("TRAILING_SPACE "),
		[]byte("lower_case"),
		[]byte("Mixed_Case"),
	}

	for _, input := range invalidInputs {
		t.Run("Type_"+string(input), func(t *testing.T) {
			var typ Type
			err := typ.UnmarshalText(input)
			require.Error(t, err)
		})
	}

	for _, input := range invalidInputs {
		t.Run("ConvertedType_"+string(input), func(t *testing.T) {
			var ct ConvertedType
			err := ct.UnmarshalText(input)
			require.Error(t, err)
		})
	}

	for _, input := range invalidInputs {
		t.Run("FieldRepetitionType_"+string(input), func(t *testing.T) {
			var frt FieldRepetitionType
			err := frt.UnmarshalText(input)
			require.Error(t, err)
		})
	}
}
