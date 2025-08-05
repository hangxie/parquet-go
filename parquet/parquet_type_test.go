package parquet

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Individual Type and ConvertedType method tests removed - replaced by consolidated versions below

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

func Test_TypeMethods(t *testing.T) {
	t.Run("String", func(t *testing.T) {
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
	})

	t.Run("FromString", func(t *testing.T) {
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
	})

	t.Run("Ptr", func(t *testing.T) {
		tests := []Type{
			Type_BOOLEAN,
			Type_INT32,
			Type_INT64,
			Type_FLOAT,
			Type_DOUBLE,
		}

		for _, typ := range tests {
			t.Run(typ.String(), func(t *testing.T) {
				ptr := TypePtr(typ)
				require.Equal(t, typ, *ptr)
			})
		}
	})

	t.Run("MarshalText", func(t *testing.T) {
		tests := []struct {
			typ      Type
			expected string
		}{
			{Type_BOOLEAN, "BOOLEAN"},
			{Type_INT32, "INT32"},
			{Type_DOUBLE, "DOUBLE"},
		}

		for _, test := range tests {
			t.Run(test.expected, func(t *testing.T) {
				bytes, err := test.typ.MarshalText()
				require.NoError(t, err)
				require.Equal(t, test.expected, string(bytes))
			})
		}
	})

	t.Run("UnmarshalText", func(t *testing.T) {
		tests := []struct {
			input    string
			expected Type
			hasError bool
		}{
			{"INT32", Type_INT32, false},
			{"BOOLEAN", Type_BOOLEAN, false},
			{"INVALID", Type(0), true},
		}

		for _, test := range tests {
			t.Run(test.input, func(t *testing.T) {
				var typ Type
				err := typ.UnmarshalText([]byte(test.input))
				if test.hasError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, test.expected, typ)
				}
			})
		}
	})

	t.Run("Scan", func(t *testing.T) {
		tests := []struct {
			name     string
			input    interface{}
			expected Type
			hasError bool
		}{
			{"valid int64", int64(Type_INT64), Type_INT64, false},
			{"valid int64 float", int64(Type_FLOAT), Type_FLOAT, false},
			{"invalid string", "invalid", Type(0), true},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				var typ Type
				err := typ.Scan(test.input)
				if test.hasError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, test.expected, typ)
				}
			})
		}
	})

	t.Run("Value", func(t *testing.T) {
		tests := []Type{
			Type_DOUBLE,
			Type_INT32,
			Type_BOOLEAN,
		}

		for _, typ := range tests {
			t.Run(typ.String(), func(t *testing.T) {
				val, err := typ.Value()
				require.NoError(t, err)
				require.Equal(t, int64(typ), val)
			})
		}
	})
}

func Test_ConvertedTypeMethods(t *testing.T) {
	t.Run("String", func(t *testing.T) {
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
			{ConvertedType_JSON, "JSON"},
			{ConvertedType(999), "<UNSET>"},
		}

		for _, test := range tests {
			t.Run(test.expected, func(t *testing.T) {
				got := test.typ.String()
				require.Equal(t, test.expected, got)
			})
		}
	})

	t.Run("FromString", func(t *testing.T) {
		tests := []struct {
			input    string
			expected ConvertedType
			hasError bool
		}{
			{"UTF8", ConvertedType_UTF8, false},
			{"MAP", ConvertedType_MAP, false},
			{"LIST", ConvertedType_LIST, false},
			{"ENUM", ConvertedType_ENUM, false},
			{"DECIMAL", ConvertedType_DECIMAL, false},
			{"INVALID", ConvertedType(0), true},
			{"", ConvertedType(0), true},
		}

		for _, test := range tests {
			t.Run(test.input, func(t *testing.T) {
				got, err := ConvertedTypeFromString(test.input)
				if test.hasError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, test.expected, got)
				}
			})
		}
	})

	t.Run("Ptr", func(t *testing.T) {
		tests := []ConvertedType{
			ConvertedType_UTF8,
			ConvertedType_MAP,
			ConvertedType_ENUM,
		}

		for _, ct := range tests {
			t.Run(ct.String(), func(t *testing.T) {
				ptr := ConvertedTypePtr(ct)
				require.Equal(t, ct, *ptr)
			})
		}
	})

	t.Run("MarshalText", func(t *testing.T) {
		tests := []struct {
			typ      ConvertedType
			expected string
		}{
			{ConvertedType_JSON, "JSON"},
			{ConvertedType_DECIMAL, "DECIMAL"},
			{ConvertedType_UTF8, "UTF8"},
		}

		for _, test := range tests {
			t.Run(test.expected, func(t *testing.T) {
				bytes, err := test.typ.MarshalText()
				require.NoError(t, err)
				require.Equal(t, test.expected, string(bytes))
			})
		}
	})

	t.Run("UnmarshalText", func(t *testing.T) {
		tests := []struct {
			input    string
			expected ConvertedType
			hasError bool
		}{
			{"DECIMAL", ConvertedType_DECIMAL, false},
			{"JSON", ConvertedType_JSON, false},
			{"INVALID", ConvertedType(0), true},
		}

		for _, test := range tests {
			t.Run(test.input, func(t *testing.T) {
				var ct ConvertedType
				err := ct.UnmarshalText([]byte(test.input))
				if test.hasError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, test.expected, ct)
				}
			})
		}
	})

	t.Run("Scan", func(t *testing.T) {
		tests := []struct {
			name     string
			input    interface{}
			expected ConvertedType
			hasError bool
		}{
			{"valid int64", int64(ConvertedType_ENUM), ConvertedType_ENUM, false},
			{"invalid string", "invalid", ConvertedType(0), true},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				var ct ConvertedType
				err := ct.Scan(test.input)
				if test.hasError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, test.expected, ct)
				}
			})
		}
	})

	t.Run("Value", func(t *testing.T) {
		tests := []ConvertedType{
			ConvertedType_UTF8,
			ConvertedType_DECIMAL,
			ConvertedType_JSON,
		}

		for _, ct := range tests {
			t.Run(ct.String(), func(t *testing.T) {
				val, err := ct.Value()
				require.NoError(t, err)
				require.Equal(t, int64(ct), val)
			})
		}
	})
}
