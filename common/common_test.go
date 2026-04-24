package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsChildPath(t *testing.T) {
	testCases := map[string]struct {
		parent   string
		child    string
		expected bool
	}{
		"test-case-1": {"a\x01b\x01c", "a\x01b\x01c", true},
		"test-case-2": {"a\x01b", "a\x01b\x01c", true},
		"test-case-3": {"a\x01b\x01", "a\x01b\x01c", false},
		"test-case-4": {"x\x01b\x01c", "a\x01b\x01c", false},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, IsChildPath(tc.parent, tc.child))
		})
	}
}

func TestPathStrIndex(t *testing.T) {
	testCases := map[string]struct {
		path     string
		expected int
	}{
		"test-case-1": {"a\x01b\x01c", 3},
		"test-case-2": {"a\x01\x01c", 3},
		"test-case-3": {"", 1},
		"test-case-4": {"abc", 1},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, PathStrIndex(tc.path))
		})
	}
}

func TestPathToStr(t *testing.T) {
	testCases := map[string]struct {
		path     []string
		expected string
	}{
		"test-case-1": {[]string{"a", "b", "c"}, "a\x01b\x01c"},
		"test-case-2": {[]string{"a", "", "c"}, "a\x01\x01c"},
		"test-case-3": {[]string{}, ""},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, PathToStr(tc.path))
		})
	}
}

func TestReformPathStr(t *testing.T) {
	testCases := map[string]struct {
		path     string
		expected string
	}{
		"test-case-1": {"a.b.c", "a\x01b\x01c"},
		"test-case-2": {"a..c", "a\x01\x01c"},
		"test-case-3": {"", ""},
		"test-case-4": {"abc", "abc"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, ReformPathStr(tc.path))
		})
	}
}

func TestStrToPath(t *testing.T) {
	testCases := map[string]struct {
		str      string
		expected []string
	}{
		"test-case-1": {"a\x01b\x01c", []string{"a", "b", "c"}},
		"test-case-2": {"a\x01\x01c", []string{"a", "", "c"}},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expected, StrToPath(tc.str))
		})
	}
}

func TestToPtr(t *testing.T) {
	testCases := map[string]struct {
		val any
	}{
		"bool":    {true},
		"int32":   {int32(1)},
		"int64":   {int64(1)},
		"string":  {"012345678901"},
		"float32": {float32(0.1)},
		"float64": {float64(0.1)},
		"slice":   {[]int32{1, 2, 3}},
		"map":     {map[string]int32{"a": 1, "b": 2, "c": 3}},
		"struct": {
			struct {
				id   uint64
				name string
			}{123, "abc"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ptr := ToPtr(tc.val)
			require.NotNil(t, ptr)
			require.Equal(t, tc.val, *ptr)
		})
	}
}
