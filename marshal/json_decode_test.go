package marshal

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeJSONRow(t *testing.T) {
	tests := []struct {
		name, input string
		want        any
		strictError bool
	}{
		{"unicode", `{"text":"你好"}`, map[string]any{"text": "你好"}, false},
		{"invalid byte", "{\"text\":\"A\xffB\"}", map[string]any{"text": "A�B"}, true},
		{"invalid key", "{\"A\xffB\":1}", map[string]any{"A�B": json.Number("1")}, true},
		{"unpaired surrogate", `"\ud800"`, "�", true},
		{"surrogate pair", `"\ud83c\udf0d"`, "🌍", false},
		{"numbers", `[18446744073709551615,1.234567890123456789,1e1000]`, []any{json.Number("18446744073709551615"), json.Number("1.234567890123456789"), json.Number("1e1000")}, false},
		{"nested numbers", `{"a":[{"n":9007199254740993}]}`, map[string]any{"a": []any{map[string]any{"n": json.Number("9007199254740993")}}}, false},
		{"duplicate names", `{"n":1,"n":2}`, map[string]any{"n": json.Number("2")}, false},
		{"first row only", `{"n":1} {"n":2}`, map[string]any{"n": json.Number("1")}, false},
		{"null", `null`, nil, false},
		{"boolean", `true`, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, row := range []any{tc.input, []byte(tc.input)} {
				for _, enabled := range []bool{false, true} {
					decoder := jsonRowDecoder{enforceUTF8: enabled}
					got, err := decoder.decode(row)
					if enabled && tc.strictError {
						require.Error(t, err)
						require.Nil(t, got)
					} else {
						require.NoError(t, err)
						require.Equal(t, tc.want, got)
					}
				}
			}
		})
	}
	for _, row := range []any{42, `{"n":`, `1e`, "", "\ufeff{\"n\":1}"} {
		for _, enabled := range []bool{false, true} {
			decoder := jsonRowDecoder{enforceUTF8: enabled}
			got, err := decoder.decode(row)
			require.Error(t, err)
			require.Nil(t, got)
		}
	}
}

func TestJSONRowDecoderReuse(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		decoder := jsonRowDecoder{enforceUTF8: enabled}
		first, err := decoder.decode(`{"n":9007199254740993,"text":"first"} {"ignored":true}`)
		require.NoError(t, err)
		raw := []byte(`{"n":2,"n":3,"text":"second"}`)
		second, err := decoder.decode(raw)
		require.NoError(t, err)
		for i := range raw {
			raw[i] = 'x'
		}
		got, err := decoder.decode(`{"broken":`)
		require.Error(t, err)
		require.Nil(t, got)
		long := strings.Repeat("你好", 1024)
		got, err = decoder.decode(`{"text":"` + long + `"}`)
		require.NoError(t, err)
		require.Equal(t, map[string]any{"text": long}, got)
		got, err = decoder.decode(`{"text":"last"}`)
		require.NoError(t, err)
		require.Equal(t, map[string]any{"text": "last"}, got)
		require.Equal(t, map[string]any{"n": json.Number("9007199254740993"), "text": "first"}, first)
		require.Equal(t, map[string]any{"n": json.Number("3"), "text": "second"}, second)
		got, err = decoder.decode("\"\xff\"")
		if enabled {
			require.Error(t, err)
			require.Nil(t, got)
		} else {
			require.NoError(t, err)
			require.Equal(t, "�", got)
		}
		got, err = decoder.decode(`"valid after error"`)
		require.NoError(t, err)
		require.Equal(t, "valid after error", got)
	}
}

func TestStrictJSONDecoderBatchAllocations(t *testing.T) {
	const rows = 128
	decoder := jsonRowDecoder{enforceUTF8: true}
	row := `{"text":"abcdefghijklmnopqrstuvwxyz012345","n":9007199254740993}`
	allocations := testing.AllocsPerRun(10, func() {
		for range rows {
			if _, err := decoder.decode(row); err != nil {
				t.Fatal(err)
			}
		}
	})
	require.LessOrEqual(t, allocations, float64(11*rows), "strict decoding should retain per-batch state")
}

func BenchmarkJSONRowDecoder(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "default"
		if enabled {
			name = "enforced"
		}
		b.Run(name, func(b *testing.B) {
			decoder := jsonRowDecoder{enforceUTF8: enabled}
			row := `{"text":"abcdefghijklmnopqrstuvwxyz012345","n":9007199254740993}`
			b.ReportAllocs()
			b.SetBytes(int64(len(row)))
			for b.Loop() {
				if _, err := decoder.decode(row); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
