package types

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestConvertBSONLogicalValue(t *testing.T) {
	tests := []struct {
		name     string
		val      any
		expected any
	}{
		{
			name:     "nil_value",
			val:      nil,
			expected: nil,
		},
		{
			name:     "byte_slice_input",
			val:      []byte{0x0c, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x01, 0x00, 0x00, 0x00, 0x00},
			expected: `{"i":{"$numberInt":"1"}}`,
		},
		{
			name:     "string_input",
			val:      string([]byte{0x0c, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x02, 0x00, 0x00, 0x00, 0x00}),
			expected: `{"i":{"$numberInt":"2"}}`,
		},
		{
			name:     "empty_byte_slice",
			val:      []byte{},
			expected: `{}`,
		},
		{
			name:     "empty_string",
			val:      "",
			expected: `{}`,
		},
		{
			name:     "empty_document",
			val:      []byte{0x05, 0x00, 0x00, 0x00, 0x00},
			expected: `{}`,
		},
		{
			name:     "non_binary_value",
			val:      123,
			expected: 123,
		},
		{
			name:     "complex_bson_document",
			val:      []byte{29, 0, 0, 0, 2, 107, 101, 121, 0, 6, 0, 0, 0, 118, 97, 108, 117, 101, 0, 16, 110, 117, 109, 0, 42, 0, 0, 0, 0},
			expected: `{"key":"value","num":{"$numberInt":"42"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertBSONLogicalValue(tt.val)
			require.Equal(t, tt.expected, result)
		})
	}
}

// TestConvertBSONValueKeepsCause pins the driver's own reason inside the chain.
func TestConvertBSONValueKeepsCause(t *testing.T) {
	// A document declaring five bytes and carrying two.
	rendered, err := convertBSONValue("\x05\x00")
	require.ErrorIs(t, err, ErrUnrenderable)
	require.ErrorContains(t, err, "too few bytes")
	// The message alone would pass a reformat, so the error value itself has to be
	// there. Two %w verbs, so the chain is the slice form and errors.Unwrap is nil.
	var chain interface{ Unwrap() []error }
	require.ErrorAs(t, err, &chain)
	require.Equal(t, bson.Raw("\x05\x00").Validate(), chain.Unwrap()[1])
	require.Equal(t, "BQA=", rendered)
}

// TestConvertBSONValueBadLength covers length prefixes that lie. MarshalExtJSON trusts
// them: it panics on the first case here and drops the tail of the last.
func TestConvertBSONValueBadLength(t *testing.T) {
	nested := func(inner []byte) []byte {
		buf := []byte{0, 0, 0, 0, 0x03, 'd', 0x00}
		buf = append(buf, inner...)
		buf = append(buf, 0x00)
		binary.LittleEndian.PutUint32(buf[:4], uint32(len(buf)))
		return buf
	}
	tests := []struct {
		name   string
		raw    []byte
		errMsg string
	}{
		{
			// The prefix reads 0, four less than its own header; MarshalExtJSON slices
			// to length-4 and panics. This is the input that put Validate in front.
			name:   "length shorter than the header",
			raw:    make([]byte, 16),
			errMsg: "length is invalid",
		},
		{
			// An embedded document lying while the outer prefix is consistent. Validate
			// recurses, which is the property this pins: without it the panic returns.
			name:   "embedded document overstates its length",
			raw:    nested([]byte{0xff, 0x00, 0x00, 0x00, 0x00}),
			errMsg: "too few bytes",
		},
		{
			name:   "embedded document understates its length",
			raw:    nested([]byte{0x03, 0x00, 0x00, 0x00, 0x00}),
			errMsg: "too few bytes",
		},
		{
			// Validate stops at the first document, so without the length check the
			// tail vanishes into a clean `{}`.
			name:   "trailing bytes after the document",
			raw:    []byte{0x05, 0x00, 0x00, 0x00, 0x00, 0xde, 0xad, 0xbe, 0xef},
			errMsg: "declares 5 bytes, value carries 9",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rendered, err := convertBSONValue(tt.raw)
			require.ErrorIs(t, err, ErrUnrenderable)
			require.ErrorContains(t, err, tt.errMsg)
			require.Equal(t, base64.StdEncoding.EncodeToString(tt.raw), rendered)
		})
	}
}

func TestConvertBSONValueEmpty(t *testing.T) {
	rendered, err := convertBSONValue([]byte{})
	require.ErrorIs(t, err, ErrUnrenderable)
	require.ErrorContains(t, err, "document is empty")
	require.Equal(t, "{}", rendered)
}

// mustRichBSON is the shared document: every BSON type a plain JSON object cannot
// express, plus two fields out of order. Not a *testing.T helper, so seeds can use it.
func mustRichBSON() []byte {
	must := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	dec, err := bson.ParseDecimal128("123.456")
	must(err)
	decNaN, err := bson.ParseDecimal128("NaN")
	must(err)
	decInf, err := bson.ParseDecimal128("Infinity")
	must(err)
	oid, err := bson.ObjectIDFromHex("6112233445566778899aabbc")
	must(err)
	raw, err := bson.Marshal(bson.D{
		{Key: "z", Value: int32(1)},
		{Key: "a", Value: 2.5},
		{Key: "oid", Value: oid},
		{Key: "bin", Value: bson.Binary{Subtype: 0x80, Data: []byte{1, 2, 3}}},
		{Key: "long", Value: int64(1 << 40)},
		{Key: "dec", Value: dec},
		{Key: "decNaN", Value: decNaN},
		{Key: "decInf", Value: decInf},
		{Key: "dt", Value: bson.DateTime(1700000000123)},
		{Key: "ts", Value: bson.Timestamp{T: 1, I: 2}},
		{Key: "yes", Value: true},
		{Key: "no", Value: false},
		{Key: "nul", Value: nil},
		{Key: "undef", Value: bson.Undefined{}},
		{Key: "min", Value: bson.MinKey{}},
		{Key: "max", Value: bson.MaxKey{}},
		{Key: "re", Value: bson.Regex{Pattern: "ab", Options: "ix"}},
		{Key: "nested", Value: bson.D{{Key: "y", Value: "last"}, {Key: "b", Value: int32(7)}}},
		{Key: "arr", Value: bson.A{int32(1), "two", nil}},
	})
	must(err)
	return raw
}

// byteArrayType is the physical type every BSON column has; the scan checks for it.
var byteArrayType = parquet.TypePtr(parquet.Type_BYTE_ARRAY)

// TestConvertBSONValueCanonicalExtJSON pins the rendering: the wrappers tell an int32
// from an int64, which the map could not, and the field order is the document's own.
func TestConvertBSONValueCanonicalExtJSON(t *testing.T) {
	rendered, err := convertBSONValue(mustRichBSON())
	require.NoError(t, err)
	require.Equal(t,
		`{"z":{"$numberInt":"1"},"a":{"$numberDouble":"2.5"},"oid":{"$oid":"6112233445566778899aabbc"`+
			`},"bin":{"$binary":{"base64":"AQID","subType":"80"}},"long":{"$numberLong":"1099511627776"`+
			`},"dec":{"$numberDecimal":"123.456"},"decNaN":{"$numberDecimal":"NaN"`+
			`},"decInf":{"$numberDecimal":"Infinity"},"dt":{"$date":{"$numberLong":"1700000000123"}`+
			`},"ts":{"$timestamp":{"t":1,"i":2}},"yes":true,"no":false,"nul":null,"undef":{"$undefined":true`+
			`},"min":{"$minKey":1},"max":{"$maxKey":1`+
			`},"re":{"$regularExpression":{"pattern":"ab","options":"ix"}`+
			`},"nested":{"y":"last","b":{"$numberInt":"7"}},"arr":[{"$numberInt":"1"},"two",null]}`,
		rendered)
}

// TestBSONExtJSONRoundTrip: a document the format can hold comes back unchanged, field
// order included. TestBSONFormatLimits has the ones it cannot.
func TestBSONExtJSONRoundTrip(t *testing.T) {
	raw := mustRichBSON()
	rendered, err := convertBSONValue(raw)
	require.NoError(t, err)

	back, err := strToBSON(rendered.(string), byteArrayType)
	require.NoError(t, err)
	require.Equal(t, string(raw), back)
}

// TestBSONFormatLimits pins what Extended JSON does to the documents it cannot express.
// None is an error; raw mode is the lossless reading. The want strings are the contract.
func TestBSONFormatLimits(t *testing.T) {
	badUTF8 := "a\xffb"
	tests := []struct {
		name string
		doc  bson.D
		// raw replaces doc where bson.Marshal would sanitize the bytes on the way in.
		raw  []byte
		want string
		// unparseable marks a rendering the scanner will not read back at all, as
		// opposed to one that reads back as a different document.
		unparseable bool
	}{
		{
			// The spec requires a wrapper-shaped subdocument to be read as the type it
			// names, with no escape, so this one is the int32 1.
			name: "nested document spells a type wrapper",
			doc:  bson.D{{Key: "x", Value: bson.D{{Key: "$numberInt", Value: "1"}}}},
			want: `{"x":{"$numberInt":"1"}}`,
		},
		{
			// The same collision where the value does not fit the type its key names.
			// Nothing can read this back, so writing the reader's output fails outright.
			name:        "wrapper-shaped document whose value does not fit the type",
			doc:         bson.D{{Key: "x", Value: bson.D{{Key: "$numberInt", Value: "A"}}}},
			want:        `{"x":{"$numberInt":"A"}}`,
			unparseable: true,
		},
		{
			name:        "wrapper-shaped document holding a bad ObjectID",
			doc:         bson.D{{Key: "x", Value: bson.D{{Key: "$oid", Value: "nothex"}}}},
			want:        `{"x":{"$oid":"nothex"}}`,
			unparseable: true,
		},
		{
			// Extended JSON spells every NaN "NaN", so sign and payload are both gone.
			name: "negative NaN",
			doc:  bson.D{{Key: "x", Value: math.Float64frombits(0xfff8000000000000)}},
			want: `{"x":{"$numberDouble":"NaN"}}`,
		},
		{
			name: "NaN with a payload of its own",
			doc:  bson.D{{Key: "x", Value: math.Float64frombits(0x7ff8000000000002)}},
			want: `{"x":{"$numberDouble":"NaN"}}`,
		},
		{
			// BSON requires a key to be valid UTF-8. This one is not, and every JSON
			// reader substitutes U+FFFD for it.
			name: "key is not valid UTF-8",
			doc:  bson.D{{Key: "\x82", Value: int32(1)}},
			want: `{"\ufffd":{"$numberInt":"1"}}`,
		},
		{
			name: "string is not valid UTF-8",
			doc:  bson.D{{Key: "s", Value: badUTF8}},
			want: `{"s":"a\ufffdb"}`,
		},
		{
			name: "string in an array is not valid UTF-8",
			doc:  bson.D{{Key: "a", Value: bson.A{"ok", badUTF8}}},
			want: `{"a":["ok","a\ufffdb"]}`,
		},
		{
			name: "string in a nested document is not valid UTF-8",
			doc:  bson.D{{Key: "d", Value: bson.D{{Key: "s", Value: badUTF8}}}},
			want: `{"d":{"s":"a\ufffdb"}}`,
		},
		{
			// Scope is typed any, so it is the nesting a rendering is most likely to
			// treat differently; it has to be reached as surely as a top-level string.
			name: "string in a code scope is not valid UTF-8",
			doc: bson.D{{Key: "c", Value: bson.CodeWithScope{
				Code:  "f",
				Scope: bson.D{{Key: "s", Value: badUTF8}},
			}}},
			want: `{"c":{"$code":"f","$scope":{"s":"a\ufffdb"}}}`,
		},
		{
			// One regex element: 0x0b, "r", pattern "a", options "\x82".
			name: "regex options are not valid UTF-8",
			raw:  []byte{0x0c, 0x00, 0x00, 0x00, 0x0b, 'r', 0x00, 'a', 0x00, 0x82, 0x00, 0x00},
			want: "{\"r\":{\"$regularExpression\":{\"pattern\":\"a\",\"options\":\"\ufffd\"}}}",
		},
		{
			// BSON requires a regex's option letters in alphabetical order; the driver
			// sorts them on the way through. One regex element: 0x0b, "r", "a", "xi".
			name: "regex options out of order",
			raw:  []byte{0x0d, 0x00, 0x00, 0x00, 0x0b, 'r', 0x00, 'a', 0x00, 'x', 'i', 0x00, 0x00},
			want: `{"r":{"$regularExpression":{"pattern":"a","options":"ix"}}}`,
		},
		{
			// The deprecated 0x02 payload carries its own length prefix. Missing here,
			// so the value gains four bytes on every pass rather than settling.
			name: "malformed binary of the deprecated 0x02 subtype",
			raw:  []byte{0x0c, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00},
			want: `{"":{"$binary":{"base64":"","subType":"02"}}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := tt.raw
			if raw == nil {
				var err error
				raw, err = bson.Marshal(tt.doc)
				require.NoError(t, err)
			}

			rendered, err := convertBSONValue(raw)
			require.NoError(t, err)
			require.Equal(t, tt.want, rendered)

			back, err := strToBSON(rendered.(string), byteArrayType)
			if tt.unparseable {
				require.ErrorContains(t, err, "parse BSON")
			} else {
				require.NoError(t, err)
				require.NotEqual(t, string(raw), back, "listed as a limit but round trips")
			}

			// Raw mode is the lossless reading of every one of them.
			se := readSE(parquet.Type_BYTE_ARRAY, parquet.ConvertedTypePtr(parquet.ConvertedType_BSON), nil, 0)
			rawMode, err := ConvertValue(string(raw), se, WithValueMode(ValueModeRaw))
			require.NoError(t, err)
			scanned, err := StrToParquetTypeWithLogical(rawMode.(string), byteArrayType,
				parquet.ConvertedTypePtr(parquet.ConvertedType_BSON), nil, 0, 0,
				WithValueMode(ValueModeRaw))
			require.NoError(t, err)
			require.Equal(t, string(raw), scanned)
		})
	}
}

// TestBSONColumnRejectsNonString pins the error a BSON column gives a non-string, which
// names the grammar. Unreachable from JSONWriter, whose node walker refuses one first.
func TestBSONColumnRejectsNonString(t *testing.T) {
	cT := parquet.ConvertedTypePtr(parquet.ConvertedType_BSON)

	for _, val := range []any{map[string]any{"i": 1}, json.Number("1"), true} {
		_, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf(val), byteArrayType, cT, nil, 0, 0)
		require.ErrorContains(t, err, "BSON column takes Extended JSON in a JSON string")
	}

	// Raw mode takes base64, also a string, and says so in the column's own terms.
	_, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf(json.Number("1")), byteArrayType,
		cT, nil, 0, 0, WithValueMode(ValueModeRaw))
	require.ErrorContains(t, err, "BYTE_ARRAY column takes a JSON string, got number")
}

// TestBSONNestingTooDeep pins the driver's own limit: its Extended JSON parser stops at
// 200 levels where its BSON decoder does not, so it will not read back its own output.
func TestBSONNestingTooDeep(t *testing.T) {
	doc := bson.D{{Key: "a", Value: int32(1)}}
	for range 201 {
		doc = bson.D{{Key: "n", Value: doc}}
	}
	raw, err := bson.Marshal(doc)
	require.NoError(t, err)

	rendered, err := convertBSONValue(raw)
	require.NoError(t, err)

	_, err = strToBSON(rendered.(string), byteArrayType)
	require.ErrorContains(t, err, "nesting too deep")
}

// TestJSONTypeToParquetTypeForwardsOptions pins that the JSON entry point hands its
// options to the scanner it delegates to; it dropped them until v3.9.0. Counted rather
// than observed, since every option this package defines is read before the delegation.
func TestJSONTypeToParquetTypeForwardsOptions(t *testing.T) {
	applied := 0
	count := ValueOption(func(*ValueConfig) { applied++ })
	cT := parquet.ConvertedTypePtr(parquet.ConvertedType_BSON)

	_, err := JSONTypeToParquetTypeWithLogical(reflect.ValueOf(`{"i":1}`),
		byteArrayType, cT, nil, 0, 0, count)
	require.NoError(t, err)
	// At least, not exactly: the count is how many times the config is resolved on this
	// path, which is not what the test is about.
	require.GreaterOrEqual(t, applied, 2, "applied here and again by the scanner it delegates to")
}

// TestStrToBSONAcceptsTextTheRenderingCannotCarry is the write path's half of the
// format's limits: the parser takes a raw byte in a key that the reader renders as
// U+FFFD, so text in and text out disagree and neither side reports it.
func TestStrToBSONAcceptsTextTheRenderingCannotCarry(t *testing.T) {
	raw, err := strToBSON("{\"\xb1\":0}", byteArrayType)
	require.NoError(t, err)

	rendered, err := convertBSONValue(raw)
	require.NoError(t, err)
	require.Equal(t, `{"\ufffd":{"$numberInt":"0"}}`, rendered)

	back, err := strToBSON(rendered.(string), byteArrayType)
	require.NoError(t, err)
	require.NotEqual(t, raw, back)
}

// TestBSONDuplicateKeys pins a shape that looks lossy and is not: the key is emitted
// twice and both come back, in order.
func TestBSONDuplicateKeys(t *testing.T) {
	raw, err := bson.Marshal(bson.D{{Key: "a", Value: int32(1)}, {Key: "a", Value: int32(2)}})
	require.NoError(t, err)

	rendered, err := convertBSONValue(raw)
	require.NoError(t, err)
	require.Equal(t, `{"a":{"$numberInt":"1"},"a":{"$numberInt":"2"}}`, rendered)

	back, err := strToBSON(rendered.(string), byteArrayType)
	require.NoError(t, err)
	require.Equal(t, string(raw), back)
}

// TestBSONTextBearingTypes covers the remaining BSON types that carry text, each of which
// the rendering has to reach as surely as a top-level string.
func TestBSONTextBearingTypes(t *testing.T) {
	oid, err := bson.ObjectIDFromHex("6112233445566778899aabbc")
	require.NoError(t, err)
	raw, err := bson.Marshal(bson.D{
		{Key: "js", Value: bson.JavaScript("function () {}")},
		{Key: "sym", Value: bson.Symbol("s")},
		{Key: "ptr", Value: bson.DBPointer{DB: "db", Pointer: oid}},
		{Key: "cws", Value: bson.CodeWithScope{
			Code:  "function () {}",
			Scope: bson.D{{Key: "v", Value: int32(1)}},
		}},
	})
	require.NoError(t, err)

	rendered, err := convertBSONValue(raw)
	require.NoError(t, err)
	back, err := strToBSON(rendered.(string), byteArrayType)
	require.NoError(t, err)
	require.Equal(t, string(raw), back)
}

func TestStrToBSON(t *testing.T) {
	tests := []struct {
		name  string
		input string
		// pT overrides the BYTE_ARRAY every BSON column has, for the cases that check it;
		// noPT is the separate case of a schema element carrying no physical type at all.
		pT     *parquet.Type
		noPT   bool
		want   string
		errMsg string
	}{
		{
			name:  "empty document",
			input: `{}`,
			want:  string([]byte{0x05, 0x00, 0x00, 0x00, 0x00}),
		},
		{
			name:  "canonical",
			input: `{"i":{"$numberInt":"1"}}`,
			want:  string([]byte{0x0c, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}),
		},
		{
			// Relaxed Extended JSON is plain JSON for the types JSON can express, so a
			// hand-written document is accepted as well as the canonical rendering.
			name:  "relaxed",
			input: `{"i":1}`,
			want:  string([]byte{0x0c, 0x00, 0x00, 0x00, 0x10, 'i', 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}),
		},
		{
			name:   "no physical type",
			input:  `{"i":1}`,
			noPT:   true,
			errMsg: "without a physical type",
		},
		{
			name:   "not a BYTE_ARRAY column",
			input:  `{"i":1}`,
			pT:     parquet.TypePtr(parquet.Type_INT32),
			errMsg: "BSON requires a BYTE_ARRAY column, not INT32",
		},
		{name: "not JSON", input: `notjson`, errMsg: `parse BSON "notjson"`},
		{name: "empty string", input: ``, errMsg: `parse BSON ""`},
		{name: "JSON array", input: `[1,2]`, errMsg: `parse BSON "[1,2]"`},
		{name: "JSON string", input: `"x"`, errMsg: `parse BSON "\"x\""`},
		// Without the json.Valid guard each of these would store just {"a":1}.
		{name: "trailing text", input: `{"a":1} oops`, errMsg: "not a single JSON value"},
		{name: "a second document", input: `{"a":1}{"b":2}`, errMsg: "not a single JSON value"},
		{name: "trailing NUL", input: "{\"a\":1}\x00junk", errMsg: "not a single JSON value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pT := byteArrayType
			switch {
			case tt.noPT:
				pT = nil
			case tt.pT != nil:
				pT = tt.pT
			}
			got, err := strToBSON(tt.input, pT)
			if tt.errMsg != "" {
				require.ErrorContains(t, err, tt.errMsg)
				require.Nil(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// bsonBenchCase is one measurement: a name and the work to repeat.
type bsonBenchCase struct {
	name string
	size int
	run  func() error
}

// bsonBenchCases builds the cases BenchmarkBSON runs, over two document shapes.
func bsonBenchCases(t require.TestingT) []bsonBenchCase {
	wide := bson.D{}
	for i := range 100 {
		wide = append(wide,
			bson.E{Key: fmt.Sprintf("s%d", i), Value: fmt.Sprintf("value-%d", i)},
			bson.E{Key: fmt.Sprintf("i%d", i), Value: int32(i)},
			bson.E{Key: fmt.Sprintf("f%d", i), Value: float64(i) + 0.5},
		)
	}
	wideRaw, err := bson.Marshal(wide)
	require.NoError(t, err)
	se := readSE(parquet.Type_BYTE_ARRAY, parquet.ConvertedTypePtr(parquet.ConvertedType_BSON), nil, 0)

	var cases []bsonBenchCase
	// A slice, not a map: subtest order has to be stable for benchstat to compare runs.
	for _, shape := range []struct {
		name string
		raw  []byte
	}{{"rich", mustRichBSON()}, {"wide", wideRaw}} {
		doc, size := string(shape.raw), len(shape.raw)
		ext, err := convertBSONValue(doc)
		require.NoError(t, err)
		text := ext.(string)

		cases = append(cases,
			bsonBenchCase{shape.name + "/read", size, func() error {
				_, err := convertBSONValue(doc)
				return err
			}},
			bsonBenchCase{shape.name + "/write", size, func() error {
				_, err := strToBSON(text, byteArrayType)
				return err
			}},
			// The v3.8.3 rendering, which README's BSON Values quotes the rest against.
			bsonBenchCase{shape.name + "/read_v3_8_3_map", size, func() error {
				var out map[string]any
				return bson.Unmarshal(shape.raw, &out)
			}},
			bsonBenchCase{shape.name + "/read_raw", size, func() error {
				_, err := ConvertValue(doc, se, WithValueMode(ValueModeRaw))
				return err
			}},
		)
	}
	return cases
}

// BenchmarkBSON measures the interpreted form against the raw one and against the map the
// read path returned up to v3.8.3.
func BenchmarkBSON(b *testing.B) {
	for _, c := range bsonBenchCases(b) {
		b.Run(c.name, func(b *testing.B) {
			b.SetBytes(int64(c.size))
			for b.Loop() {
				if err := c.run(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
