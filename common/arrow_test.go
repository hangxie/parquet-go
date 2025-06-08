package common

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
)

func Test_newTable(t *testing.T) {
	actual := newTable(5, 6)
	require.Equal(t, 5, len(actual))
	require.Equal(t, 6, len(actual[0]))
}

func Test_TransposeTable(t *testing.T) {
	testCases := map[string]struct {
		table    [][]any
		expected [][]any
	}{
		"test-case-1": {[][]any{{1, 2, 3}}, [][]any{{1}, {2}, {3}}},
		"test-case-2": {[][]any{{1, 2, 3}, {4, 5, 6}}, [][]any{{1, 4}, {2, 5}, {3, 6}}},
		"test-case-3": {[][]any{{1}, {2}, {3}}, [][]any{{1, 2, 3}}},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, TransposeTable(tc.table))
		})
	}
}

func Test_ArrowColToParquetCol(t *testing.T) {
	testCases := map[string]struct {
		field    arrow.Field
		col      arrow.Array
		expected []any
		errMsg   string
	}{
		// TODO test cases
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual, err := ArrowColToParquetCol(tc.field, tc.col)
			if err == nil && tc.errMsg == "" {
				require.Equal(t, tc.expected, actual)
			} else if err == nil || tc.errMsg == "" {
				t.Errorf("expected [%s], got [%v]", tc.errMsg, err)
			} else {
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}
}

func Test_nonNullableFieldContainsNullError(t *testing.T) {
	err := nonNullableFieldContainsNullError(arrow.Field{Name: "unit-test"}, 3)
	require.Equal(t, "field with name 'unit-test' is marked non-nullable but its column array contains Null value at index 3", err.Error())
}
