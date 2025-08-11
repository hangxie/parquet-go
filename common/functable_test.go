package common

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_FindFuncTable(t *testing.T) {
	testCases := map[string]struct {
		pT       *parquet.Type
		cT       *parquet.ConvertedType
		lT       *parquet.LogicalType
		expected FuncTable
	}{
		"BOOLEAN-nil-nil":                   {ToPtr(parquet.Type_BOOLEAN), nil, nil, boolFuncTable{}},
		"INT32-nil-nil":                     {ToPtr(parquet.Type_INT32), nil, nil, int32FuncTable{}},
		"INT64-nil-nil":                     {ToPtr(parquet.Type_INT64), nil, nil, int64FuncTable{}},
		"INT96-nil-nil":                     {ToPtr(parquet.Type_INT96), nil, nil, int96FuncTable{}},
		"FLOAT-nil-nil":                     {ToPtr(parquet.Type_FLOAT), nil, nil, float32FuncTable{}},
		"DOUBLE-nil-nil":                    {ToPtr(parquet.Type_DOUBLE), nil, nil, float64FuncTable{}},
		"BYTE_ARRAY-nil-nil":                {ToPtr(parquet.Type_BYTE_ARRAY), nil, nil, byteArrayFuncTable{}},
		"FIXED_LEN_BYTE_ARRAY-nil-nil":      {ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil, nil, byteArrayFuncTable{}},
		"BYTE_ARRAY-UTF8-nil":               {ToPtr(parquet.Type_BYTE_ARRAY), ToPtr(parquet.ConvertedType_UTF8), nil, stringFuncTable{}},
		"BYTE_ARRAY-BSON-nil":               {ToPtr(parquet.Type_BYTE_ARRAY), ToPtr(parquet.ConvertedType_BSON), nil, stringFuncTable{}},
		"BYTE_ARRAY-JSON-nil":               {ToPtr(parquet.Type_BYTE_ARRAY), ToPtr(parquet.ConvertedType_JSON), nil, stringFuncTable{}},
		"BYTE_ARRAY-ENUM-nil":               {ToPtr(parquet.Type_BYTE_ARRAY), ToPtr(parquet.ConvertedType_ENUM), nil, stringFuncTable{}},
		"INT32-INT_8-nil":                   {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_INT_8), nil, int32FuncTable{}},
		"INT32-INT_16-nil":                  {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_INT_16), nil, int32FuncTable{}},
		"INT32-INT_32-nil":                  {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_INT_32), nil, int32FuncTable{}},
		"INT64-INT_64-nil":                  {ToPtr(parquet.Type_INT64), ToPtr(parquet.ConvertedType_INT_64), nil, int64FuncTable{}},
		"INT32-UINT_8-nil":                  {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_UINT_8), nil, uint32FuncTable{}},
		"INT32-UINT_16-nil":                 {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_UINT_16), nil, uint32FuncTable{}},
		"INT32-UINT_32-nil":                 {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_UINT_32), nil, uint32FuncTable{}},
		"INT64-UINT_64-nil":                 {ToPtr(parquet.Type_INT64), ToPtr(parquet.ConvertedType_UINT_64), nil, uint64FuncTable{}},
		"INT32-DATE-nil":                    {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_DATE), nil, int32FuncTable{}},
		"INT64-TIME_MILLIS-nil":             {ToPtr(parquet.Type_INT64), ToPtr(parquet.ConvertedType_TIME_MILLIS), nil, int32FuncTable{}},
		"INT64-TIME_MICROS-nil":             {ToPtr(parquet.Type_INT64), ToPtr(parquet.ConvertedType_TIME_MICROS), nil, int64FuncTable{}},
		"FIXED_LEN_BYTE_ARRAY-INTERVAL-nil": {ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), ToPtr(parquet.ConvertedType_INTERVAL), nil, intervalFuncTable{}},
		"BYTE_ARRAY-DECIMAL-nil":            {ToPtr(parquet.Type_BYTE_ARRAY), ToPtr(parquet.ConvertedType_DECIMAL), nil, decimalStringFuncTable{}},
		"FIXED_LEN_BYTE_ARRAY-DECIMAL-nil":  {ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), ToPtr(parquet.ConvertedType_DECIMAL), nil, decimalStringFuncTable{}},
		"INT32-DECIMAL-nil":                 {ToPtr(parquet.Type_INT32), ToPtr(parquet.ConvertedType_DECIMAL), nil, int32FuncTable{}},
		"INT64-DECIMAL-nil":                 {ToPtr(parquet.Type_INT64), ToPtr(parquet.ConvertedType_DECIMAL), nil, int64FuncTable{}},
		"INT32-nil-TIME":                    {ToPtr(parquet.Type_INT32), nil, &parquet.LogicalType{TIME: &parquet.TimeType{}}, int32FuncTable{}},
		"INT64-nil-TIMESTAMP":               {ToPtr(parquet.Type_INT64), nil, &parquet.LogicalType{TIME: &parquet.TimeType{}}, int64FuncTable{}},
		"INT64-nil-DATE":                    {ToPtr(parquet.Type_INT64), nil, &parquet.LogicalType{DATE: &parquet.DateType{}}, int32FuncTable{}},
		"INT32-nil-INTEGER-signed":          {ToPtr(parquet.Type_INT32), nil, &parquet.LogicalType{INTEGER: &parquet.IntType{IsSigned: true}}, int32FuncTable{}},
		"INT64-nil-INTEGER-signed":          {ToPtr(parquet.Type_INT64), nil, &parquet.LogicalType{INTEGER: &parquet.IntType{IsSigned: true}}, int64FuncTable{}},
		"INT32-nil-INTEGER-unsigned":        {ToPtr(parquet.Type_INT32), nil, &parquet.LogicalType{INTEGER: &parquet.IntType{}}, uint32FuncTable{}},
		"INT64-nil-INTEGER-unsigned":        {ToPtr(parquet.Type_INT64), nil, &parquet.LogicalType{INTEGER: &parquet.IntType{}}, uint64FuncTable{}},
		"BYTE_ARRAY-nil-DECIMAL":            {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}}, decimalStringFuncTable{}},
		"FIXED_LEN_BYTE_ARRAY-nil-DECIMAL":  {ToPtr(parquet.Type_FIXED_LEN_BYTE_ARRAY), nil, &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}}, decimalStringFuncTable{}},
		"INT32-nil-DECIMAL":                 {ToPtr(parquet.Type_INT32), nil, &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}}, int32FuncTable{}},
		"INT64-nil-DECIMAL":                 {ToPtr(parquet.Type_INT64), nil, &parquet.LogicalType{DECIMAL: &parquet.DecimalType{}}, int64FuncTable{}},
		"BYTE_ARRAY-nil-BSON":               {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{BSON: &parquet.BsonType{}}, stringFuncTable{}},
		"BYTE_ARRAY-nil-JSON":               {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{JSON: &parquet.JsonType{}}, stringFuncTable{}},
		"BYTE_ARRAY-nil-STRING":             {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{STRING: &parquet.StringType{}}, stringFuncTable{}},
		"BYTE_ARRAY-nil-UUID":               {ToPtr(parquet.Type_BYTE_ARRAY), nil, &parquet.LogicalType{UUID: &parquet.UUIDType{}}, stringFuncTable{}},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := FindFuncTable(tc.pT, tc.cT, tc.lT)
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}

	t.Run("bad", func(t *testing.T) {
		_, err := FindFuncTable(nil, nil, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "all types are nil")
		_, err = FindFuncTable(nil, nil, &parquet.LogicalType{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot find func table for given types")
	})
}

func Test_LessThan(t *testing.T) {
	toHex := func(input string) string {
		ret, _ := hex.DecodeString(input)
		return string(ret)
	}
	testCases := map[string]struct {
		f        FuncTable
		a        any
		b        any
		expected bool
	}{
		"bool":       {boolFuncTable{}, true, false, false},
		"int32":      {int32FuncTable{}, int32(1), int32(2), true},
		"uint32":     {uint32FuncTable{}, int32(1), int32(2), true},
		"int64":      {int64FuncTable{}, int64(1), int64(2), true},
		"uint64":     {uint64FuncTable{}, int64(1), int64(2), true},
		"int96-1":    {int96FuncTable{}, toHex("000000000000000000000001"), toHex("010000000000000000000000"), false},
		"int96-2":    {int96FuncTable{}, toHex("000000000000000000010000"), toHex("000000000000000000020000"), true},
		"int96-3":    {int96FuncTable{}, toHex("0000000000000000000000ff"), toHex("010000000000000000000000"), true},
		"int96-4":    {int96FuncTable{}, toHex("000000000000000000000000"), toHex("0100000000000000000000ff"), false},
		"int96-5":    {int96FuncTable{}, toHex("000000000000000000000000"), toHex("000000000000000000000000"), false},
		"float32":    {float32FuncTable{}, float32(1), float32(2), true},
		"float64":    {float64FuncTable{}, float64(1), float64(2), true},
		"string":     {stringFuncTable{}, "a", "b", true},
		"interval-1": {intervalFuncTable{}, toHex("000000000000000000000001"), toHex("010000000000000000000000"), false},
		"interval-2": {intervalFuncTable{}, toHex("000000000000000000000001"), toHex("000000000000000000000002"), true},
		"interval-3": {intervalFuncTable{}, toHex("000000000000000000000000"), toHex("000000000000000000000000"), false},
		"decimal":    {decimalStringFuncTable{}, "\x00\x02", "\x00\x01", false},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.f.lessThan(tc.a, tc.b))
		})
	}
}

func Test_Max(t *testing.T) {
	testCases := map[string]struct {
		Num1, Num2 any
		PT         *parquet.Type
		CT         *parquet.ConvertedType
		Expected   any
	}{
		"nil-int32":     {nil, int32(1), parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
		"nil-nil":       {nil, nil, parquet.TypePtr(parquet.Type_INT32), nil, nil},
		"int32-nil":     {int32(1), nil, parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
		"int32-int32-1": {int32(1), int32(2), parquet.TypePtr(parquet.Type_INT32), nil, int32(2)},
		"int32-int32-2": {int32(2), int32(1), parquet.TypePtr(parquet.Type_INT32), nil, int32(2)},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			funcTable, err := FindFuncTable(tc.PT, tc.CT, nil)
			require.NoError(t, err)
			res := max(funcTable, tc.Num1, tc.Num2)
			require.Equal(t, tc.Expected, res)
		})
	}
}

func Test_Min(t *testing.T) {
	testCases := map[string]struct {
		Num1, Num2 any
		PT         *parquet.Type
		CT         *parquet.ConvertedType
		Expected   any
	}{
		"nil-int32":     {nil, int32(1), parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
		"nil-nil":       {nil, nil, parquet.TypePtr(parquet.Type_INT32), nil, nil},
		"int32-nil":     {int32(1), nil, parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
		"int32-int32-1": {int32(1), int32(2), parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
		"int32-int32-2": {int32(2), int32(1), parquet.TypePtr(parquet.Type_INT32), nil, int32(1)},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			funcTable, err := FindFuncTable(tc.PT, tc.CT, nil)
			require.NoError(t, err)
			res := min(funcTable, tc.Num1, tc.Num2)
			require.Equal(t, tc.Expected, res)
		})
	}
}

func Test_MinMaxSize(t *testing.T) {
	toHex := func(input string) string {
		ret, _ := hex.DecodeString(input)
		return string(ret)
	}
	testCases := map[string]struct {
		f            FuncTable
		minVal       any
		maxVal       any
		val          any
		expectedMin  any
		expectedMax  any
		expectedSize int32
	}{
		"bool-1":   {boolFuncTable{}, false, true, false, false, true, 1},
		"bool-2":   {boolFuncTable{}, false, true, true, false, true, 1},
		"bool-3":   {boolFuncTable{}, false, false, true, false, true, 1},
		"bool-4":   {boolFuncTable{}, true, true, false, false, true, 1},
		"int32-1":  {int32FuncTable{}, int32(2), int32(4), int32(1), int32(1), int32(4), 4},
		"int32-2":  {int32FuncTable{}, int32(2), int32(4), int32(3), int32(2), int32(4), 4},
		"int32-3":  {int32FuncTable{}, int32(2), int32(4), int32(5), int32(2), int32(5), 4},
		"uint32-1": {uint32FuncTable{}, int32(2), int32(4), int32(1), int32(1), int32(4), 4},
		"uint32-2": {uint32FuncTable{}, int32(2), int32(4), int32(3), int32(2), int32(4), 4},
		"uint32-3": {uint32FuncTable{}, int32(2), int32(4), int32(5), int32(2), int32(5), 4},
		"int64-1":  {int64FuncTable{}, int64(2), int64(4), int64(1), int64(1), int64(4), 8},
		"int64-2":  {int64FuncTable{}, int64(2), int64(4), int64(3), int64(2), int64(4), 8},
		"int64-3":  {int64FuncTable{}, int64(2), int64(4), int64(5), int64(2), int64(5), 8},
		"uint64-1": {uint64FuncTable{}, int64(2), int64(4), int64(1), int64(1), int64(4), 8},
		"uint64-2": {uint64FuncTable{}, int64(2), int64(4), int64(3), int64(2), int64(4), 8},
		"uint64-3": {uint64FuncTable{}, int64(2), int64(4), int64(5), int64(2), int64(5), 8},
		"int96-1": {
			int96FuncTable{},
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			toHex("000000000000000000000001"),
			toHex("000000000000000000000001"),
			toHex("000000000000000000000004"),
			12,
		},
		"int96-2": {
			int96FuncTable{},
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			toHex("000000000000000000000003"),
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			12,
		},
		"int96-3": {
			int96FuncTable{},
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			toHex("000000000000000000000005"),
			toHex("000000000000000000000002"),
			toHex("000000000000000000000005"),
			12,
		},
		"float32-1": {float32FuncTable{}, float32(2), float32(4), float32(1), float32(1), float32(4), 4},
		"float32-2": {float32FuncTable{}, float32(2), float32(4), float32(3), float32(2), float32(4), 4},
		"float32-3": {float32FuncTable{}, float32(2), float32(4), float32(5), float32(2), float32(5), 4},
		"float64-1": {float64FuncTable{}, float64(2), float64(4), float64(1), float64(1), float64(4), 8},
		"float64-2": {float64FuncTable{}, float64(2), float64(4), float64(3), float64(2), float64(4), 8},
		"float64-3": {float64FuncTable{}, float64(2), float64(4), float64(5), float64(2), float64(5), 8},
		"string-1":  {stringFuncTable{}, "2", "4", "11", "11", "4", 2},
		"string-2":  {stringFuncTable{}, "2", "4", "33", "2", "4", 2},
		"string-3":  {stringFuncTable{}, "2", "4", "55", "2", "55", 2},
		"interval-1": {
			intervalFuncTable{},
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			toHex("000000000000000000000001"),
			toHex("000000000000000000000001"),
			toHex("000000000000000000000004"),
			12,
		},
		"interval-2": {
			intervalFuncTable{},
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			toHex("000000000000000000000003"),
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			12,
		},
		"interval-3": {
			intervalFuncTable{},
			toHex("000000000000000000000002"),
			toHex("000000000000000000000004"),
			toHex("000000000000000000000005"),
			toHex("000000000000000000000002"),
			toHex("000000000000000000000005"),
			12,
		},
		"decimal-1": {decimalStringFuncTable{}, "\x02", "\x04", "\x00\x01", "\x00\x01", "\x04", 2},
		"decimal-2": {decimalStringFuncTable{}, "\x02", "\x04", "\x00\x03", "\x02", "\x04", 2},
		"decimal-3": {decimalStringFuncTable{}, "\x02", "\x04", "\x00\x05", "\x02", "\x00\x05", 2},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			newMin, newMax, size := tc.f.MinMaxSize(tc.minVal, tc.maxVal, tc.val)
			require.Equal(t, tc.expectedMin, newMin)
			require.Equal(t, tc.expectedMax, newMax)
			require.Equal(t, tc.expectedSize, int32(size))
		})
	}
}

func Test_SizeOf(t *testing.T) {
	testCases := map[string]struct {
		val      reflect.Value
		expected int64
	}{
		"bool":            {reflect.ValueOf(true), 1},
		"bool-pointer":    {reflect.ValueOf(ToPtr(true)), 1},
		"int32":           {reflect.ValueOf(int32(1)), 4},
		"int32-pointer":   {reflect.ValueOf(ToPtr(int32(1))), 4},
		"int64":           {reflect.ValueOf(int64(1)), 8},
		"int64-pointer":   {reflect.ValueOf(ToPtr(int64(1))), 8},
		"string":          {reflect.ValueOf("012345678901"), 12},
		"string-empty":    {reflect.ValueOf(""), 0},
		"string-pointer":  {reflect.ValueOf(ToPtr("012345678901")), 12},
		"float32":         {reflect.ValueOf(float32(0.1)), 4},
		"float32-pointer": {reflect.ValueOf(ToPtr(float32(0.1))), 4},
		"float64":         {reflect.ValueOf(float64(0.1)), 8},
		"float64-pointer": {reflect.ValueOf(ToPtr(float64(0.1))), 8},
		"pointer-nil":     {reflect.ValueOf((*string)(nil)), 0},
		"slice":           {reflect.ValueOf([]int32{1, 2, 3}), 12},
		"map": {reflect.ValueOf(map[string]int32{
			string("1"):   1,
			string("11"):  11,
			string("111"): 111,
		}), 18},
		"struct": {reflect.ValueOf(struct {
			A int32
			B int64
			C []string
			D map[string]string
		}{
			1, 2,
			[]string{"hello", "world", "", "good"},
			map[string]string{
				string("hello"): string("012345678901"),
				string("world"): string("012345678901"),
			},
		}), 60},
		"channel":       {reflect.ValueOf(make(chan int)), 4},
		"invalid-value": {reflect.ValueOf(nil), 0},
	}

	for name, data := range testCases {
		t.Run(name, func(t *testing.T) {
			res := SizeOf(data.val)
			require.Equal(t, data.expected, res)
		})
	}
}

func Test_cmpIntBinary(t *testing.T) {
	testCases := map[string]struct {
		a        []byte
		b        []byte
		endian   string
		signed   bool
		expected bool
	}{
		"8-bits: 0 < 0":        {[]byte{0}, []byte{0}, "LittleEndian", true, false},
		"8-bits: 0 < -1":       {[]byte{0}, []byte{255}, "LittleEndian", true, false},
		"8-bits: -1 < 0":       {[]byte{255}, []byte{0}, "LittleEndian", true, true},
		"8-bits: 255 < 0":      {[]byte{255}, []byte{0}, "LittleEndian", false, false},
		"16-bits: -1 < 0":      {[]byte{255, 255}, []byte{0, 0}, "LittleEndian", true, true},
		"16-bits: 65535 < 0":   {[]byte{255, 255}, []byte{0, 0}, "LittleEndian", false, false},
		"16-bits: -256 < 0":    {[]byte{255, 0}, []byte{0, 0}, "BigEndian", true, true},
		"16-bits: 65280 < 0":   {[]byte{0, 255}, []byte{0, 0}, "LittleEndian", false, false},
		"8/16-bits: -1 < -2":   {[]byte{255}, []byte{255, 254}, "BigEndian", true, false},
		"16/8-bits: -2 < -1":   {[]byte{254, 255}, []byte{255}, "LittleEndian", true, true},
		"8/16-bits: 255 < 254": {[]byte{255}, []byte{0, 254}, "BigEndian", false, false},
		"16/8-bits: 254 < 255": {[]byte{254, 0}, []byte{255}, "LittleEndian", false, true},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, cmpIntBinary(string(tc.a), string(tc.b), tc.endian, tc.signed), tc.expected)
		})
	}
}

func Test_Int96FuncTable_LessThan_NilChecks(t *testing.T) {
	table := int96FuncTable{}

	tests := []struct {
		name   string
		a      any
		b      any
		expect bool
	}{
		{
			name:   "non_string_first_param",
			a:      42,
			b:      "123456789012",
			expect: false,
		},
		{
			name:   "non_string_second_param",
			a:      "123456789012",
			b:      42,
			expect: false,
		},
		{
			name:   "both_non_string",
			a:      42,
			b:      43,
			expect: false,
		},
		{
			name:   "first_string_too_short",
			a:      "short",
			b:      "123456789012",
			expect: false,
		},
		{
			name:   "second_string_too_short",
			a:      "123456789012",
			b:      "short",
			expect: false,
		},
		{
			name:   "both_strings_too_short",
			a:      "short1",
			b:      "short2",
			expect: false,
		},
		{
			name:   "nil_first_param",
			a:      nil,
			b:      "123456789012",
			expect: false,
		},
		{
			name:   "nil_second_param",
			a:      "123456789012",
			b:      nil,
			expect: false,
		},
		{
			name:   "both_nil",
			a:      nil,
			b:      nil,
			expect: false,
		},
		{
			name:   "empty_strings",
			a:      "",
			b:      "",
			expect: false,
		},
		{
			name:   "valid_12_byte_strings",
			a:      "123456789012",
			b:      "123456789013",
			expect: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := table.lessThan(tt.a, tt.b)
			require.Equal(t, tt.expect, result)
		})
	}
}

func Test_IntervalFuncTable_LessThan_NilChecks(t *testing.T) {
	table := intervalFuncTable{}

	tests := []struct {
		name   string
		a      any
		b      any
		expect bool
	}{
		{
			name:   "non_string_first_param",
			a:      42,
			b:      "123456789012",
			expect: false,
		},
		{
			name:   "non_string_second_param",
			a:      "123456789012",
			b:      42,
			expect: false,
		},
		{
			name:   "both_non_string",
			a:      42,
			b:      43,
			expect: false,
		},
		{
			name:   "first_string_too_short",
			a:      "short",
			b:      "123456789012",
			expect: false,
		},
		{
			name:   "second_string_too_short",
			a:      "123456789012",
			b:      "short",
			expect: false,
		},
		{
			name:   "both_strings_too_short",
			a:      "short1",
			b:      "short2",
			expect: false,
		},
		{
			name:   "nil_first_param",
			a:      nil,
			b:      "123456789012",
			expect: false,
		},
		{
			name:   "nil_second_param",
			a:      "123456789012",
			b:      nil,
			expect: false,
		},
		{
			name:   "both_nil",
			a:      nil,
			b:      nil,
			expect: false,
		},
		{
			name:   "empty_strings",
			a:      "",
			b:      "",
			expect: false,
		},
		{
			name:   "valid_12_byte_strings_equal",
			a:      "123456789012",
			b:      "123456789012",
			expect: false,
		},
		{
			name:   "valid_12_byte_strings_different",
			a:      "123456789012",
			b:      "123456789013",
			expect: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := table.lessThan(tt.a, tt.b)
			require.Equal(t, tt.expect, result)
		})
	}
}

// Test the bounds checking with malformed data that could cause index out of bounds
func Test_Int96FuncTable_BoundsChecking(t *testing.T) {
	table := int96FuncTable{}

	// Test with exactly 11 bytes (one less than required)
	shortString := "12345678901"  // 11 bytes
	validString := "123456789012" // 12 bytes

	result := table.lessThan(shortString, validString)
	require.False(t, result)

	result = table.lessThan(validString, shortString)
	require.False(t, result)
}

func Test_IntervalFuncTable_BoundsChecking(t *testing.T) {
	table := intervalFuncTable{}

	// Test with exactly 11 bytes (one less than required)
	shortString := "12345678901"  // 11 bytes
	validString := "123456789012" // 12 bytes

	result := table.lessThan(shortString, validString)
	require.False(t, result)

	result = table.lessThan(validString, shortString)
	require.False(t, result)
}
