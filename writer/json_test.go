package writer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/types"
)

func TestJSONWriter(t *testing.T) {
	t.Run("new_json_writer/valid_schema", func(t *testing.T) {
		jsonSchema := `{
			"Tag": "name=parquet-go-root",
			"Fields": [
				{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"},
				{"Tag": "name=age, type=INT32"}
			]
		}`

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		jw, err := NewJSONWriter(jsonSchema, fw, WithNP(1))

		require.NoError(t, err)
		require.NotNil(t, jw)
		require.NotNil(t, jw.SchemaHandler)
		require.Equal(t, int64(1), jw.np)

		// Clean up
		err = jw.WriteStop()
		require.NoError(t, err)
	})

	t.Run("new_json_writer/invalid_json_schema", func(t *testing.T) {
		invalidSchema := `{"invalid": json syntax}`

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		jw, err := NewJSONWriter(invalidSchema, fw, WithNP(1))

		require.Error(t, err)
		require.Contains(t, err.Error(), "unmarshal json schema")
		require.Nil(t, jw)
	})

	t.Run("new_json_writer/empty_schema", func(t *testing.T) {
		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		jw, err := NewJSONWriter("", fw, WithNP(1))

		require.Error(t, err)
		require.Contains(t, err.Error(), "unmarshal json schema")
		require.Nil(t, jw)
	})

	t.Run("new_json_writer/malformed_schema", func(t *testing.T) {
		malformedSchema := `{
			"Tag": "name=parquet-go-root",
			"Fields": [
				{"Tag": "name=name, type=INVALID_TYPE"}
			]
		}`

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		jw, err := NewJSONWriter(malformedSchema, fw, WithNP(1))

		require.Error(t, err)
		require.Contains(t, err.Error(), "not a valid Type")
		require.Nil(t, jw)
	})

	t.Run("new_json_writer_from_writer/successful_creation", func(t *testing.T) {
		jsonSchema := `{
			"Tag": "name=parquet-go-root",
			"Fields": [
				{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"},
				{"Tag": "name=age, type=INT32"}
			]
		}`

		var buf bytes.Buffer
		jw, err := NewJSONWriterFromWriter(jsonSchema, &buf, WithNP(2))

		require.NoError(t, err)
		require.NotNil(t, jw)
		require.Equal(t, int64(2), jw.np)

		// Clean up
		err = jw.WriteStop()
		require.NoError(t, err)
	})

	t.Run("new_json_writer_from_writer/invalid_schema", func(t *testing.T) {
		invalidSchema := `not valid json`

		var buf bytes.Buffer
		jw, err := NewJSONWriterFromWriter(invalidSchema, &buf, WithNP(1))

		require.Error(t, err)
		require.Contains(t, err.Error(), "unmarshal json schema")
		require.Nil(t, jw)
	})

	t.Run("new_json_writer_with_options", func(t *testing.T) {
		jsonSchema := `{
			"Tag": "name=parquet-go-root",
			"Fields": [
				{"Tag": "name=id, type=INT32"}
			]
		}`

		var buf bytes.Buffer
		fw := writerfile.NewWriterFile(&buf)
		jw, err := NewJSONWriter(
			jsonSchema, fw,
			WithNP(8),
			WithPageSize(16384),
			WithRowGroupSize(256*1024*1024),
		)
		require.NoError(t, err)
		require.Equal(t, int64(8), jw.np)
		require.Equal(t, int64(16384), jw.pageSize)
		require.Equal(t, int64(256*1024*1024), jw.rowGroupSize)

		err = jw.WriteStop()
		require.NoError(t, err)
	})

	t.Run("write", func(t *testing.T) {
		tests := []struct {
			name           string
			jsonSchema     string
			writeData      []any // Can be string or []byte
			expectWriteErr bool
			expectStopErr  bool
			stopErrMsg     string
			expectRows     *int64                                                // nil means don't check rows
			minBufSize     int                                                   // minimum buffer size after write
			customTest     func(t *testing.T, jw *JSONWriter, buf *bytes.Buffer) // For special test cases
		}{
			{
				name: "simple_data",
				jsonSchema: `{
					"Tag": "name=parquet-go-root",
					"Fields": [
						{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"},
						{"Tag": "name=age, type=INT32"},
						{"Tag": "name=active, type=BOOLEAN"}
					]
				}`,
				writeData: []any{
					`{"name": "Alice", "age": 25, "active": true}`,
					`{"name": "Bob", "age": 30, "active": false}`,
					`{"name": "Charlie", "age": 35, "active": true}`,
				},
				expectWriteErr: false,
				expectStopErr:  false,
				expectRows:     func() *int64 { r := int64(3); return &r }(),
				minBufSize:     4,
			},
			{
				name: "byte_slice_input",
				jsonSchema: `{
					"Tag": "name=parquet-go-root",
					"Fields": [
						{"Tag": "name=id, type=INT32"}
					]
				}`,
				writeData: []any{
					[]byte(`{"id": 42}`),
				},
				expectWriteErr: false,
				expectStopErr:  false,
				minBufSize:     4,
			},
			{
				name: "invalid_json_data",
				jsonSchema: `{
					"Tag": "name=parquet-go-root",
					"Fields": [
						{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"}
					]
				}`,
				writeData: []any{
					`{"name": "Alice"`, // Missing closing brace
				},
				expectWriteErr: false, // Write doesn't validate immediately
				expectStopErr:  true,  // Error occurs during marshaling
				stopErrMsg:     "unexpected EOF",
				minBufSize:     4,
			},
			{
				name: "empty_data",
				jsonSchema: `{
					"Tag": "name=parquet-go-root",
					"Fields": [
						{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"}
					]
				}`,
				writeData:      []any{}, // No data
				expectWriteErr: false,
				expectStopErr:  false,
				minBufSize:     4,
			},
			{
				name: "nested_structure",
				jsonSchema: `{
					"Tag": "name=parquet-go-root",
					"Fields": [
						{"Tag": "name=user_id, type=INT64"},
						{"Tag": "name=metadata, type=BYTE_ARRAY, convertedtype=UTF8"}
					]
				}`,
				writeData: []any{
					`{"user_id": 123, "metadata": "{\"country\": \"US\", \"city\": \"NYC\"}"}`,
				},
				expectWriteErr: false,
				expectStopErr:  false,
				minBufSize:     4,
			},
			{
				name: "null_values",
				jsonSchema: `{
					"Tag": "name=parquet-go-root",
					"Fields": [
						{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"},
						{"Tag": "name=age, type=INT32, repetitiontype=OPTIONAL"}
					]
				}`,
				writeData: []any{
					`{"name": "Alice", "age": 25}`,
					`{"name": "Bob", "age": null}`,
					`{"name": "Charlie"}`, // Missing age field
				},
				expectWriteErr: false,
				expectStopErr:  false,
				minBufSize:     4,
			},
			{
				name: "write_after_stop",
				jsonSchema: `{
					"Tag": "name=parquet-go-root",
					"Fields": [
						{"Tag": "name=id, type=INT32"}
					]
				}`,
				writeData:      []any{},
				expectWriteErr: false,
				expectStopErr:  false,
				minBufSize:     4,
				customTest: func(t *testing.T, jw *JSONWriter, buf *bytes.Buffer) {
					// Stop the writer first
					err := jw.WriteStop()
					require.NoError(t, err)

					// Try to write after stop - should handle gracefully
					_ = jw.Write(`{"id": 1}`)
					// The behavior depends on implementation, just verify no panic
				},
			},
			{
				name: "multiple_stops",
				jsonSchema: `{
					"Tag": "name=parquet-go-root",
					"Fields": [
						{"Tag": "name=id, type=INT32"}
					]
				}`,
				writeData:      []any{},
				expectWriteErr: false,
				expectStopErr:  false,
				minBufSize:     4,
				customTest: func(t *testing.T, jw *JSONWriter, buf *bytes.Buffer) {
					// Call WriteStop multiple times
					err1 := jw.WriteStop()
					_ = jw.WriteStop() // Second call may error, but shouldn't panic

					// At least the first call should succeed
					require.NoError(t, err1)
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var buf bytes.Buffer
				jw, err := NewJSONWriterFromWriter(tt.jsonSchema, &buf, WithNP(1))
				require.NoError(t, err)

				// Handle custom test cases
				if tt.customTest != nil {
					tt.customTest(t, jw, &buf)
					return
				}

				// Write test data
				var writeErr error
				for _, data := range tt.writeData {
					writeErr = jw.Write(data)
					if tt.expectWriteErr {
						break
					}
					require.NoError(t, writeErr)
				}

				if tt.expectWriteErr {
					require.Error(t, writeErr)
					return
				}

				// Stop the writer
				stopErr := jw.WriteStop()
				if tt.expectStopErr {
					require.Error(t, stopErr)
					require.Contains(t, stopErr.Error(), tt.stopErrMsg)
					return
				}
				require.NoError(t, stopErr)

				// Verify buffer size
				require.Greater(t, buf.Len(), tt.minBufSize)

				// Verify row count if specified
				if tt.expectRows != nil {
					pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
					//nolint:staticcheck
					pr, err := reader.NewParquetReader(pf, nil, reader.WithNP(1))
					require.NoError(t, err)

					numRows := pr.GetNumRows()
					require.Equal(t, *tt.expectRows, numRows)

					//nolint:staticcheck
					_ = pr.ReadStop()
					_ = pf.Close()
				}
			})
		}
	})
}

func TestJSONWriterUUID(t *testing.T) {
	jsonSchema := `{
		"Tag": "name=parquet-go-root",
		"Fields": [
			{"Tag": "name=id, type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID"}
		]
	}`

	testCases := map[string]struct {
		value  string
		errMsg string
	}{
		"dashed":         {"550e8400-e29b-41d4-a716-446655440000", ""},
		"urn":            {"urn:uuid:550e8400-e29b-41d4-a716-446655440000", ""},
		"not_a_uuid":     {"not-a-uuid", "parse UUID"},
		"truncated":      {"550e8400-e29b-41d4-a716-44665544", "parse UUID"},
		"square_bracket": {"[550e8400-e29b-41d4-a716-446655440000]", "parse UUID"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			jw, err := NewJSONWriterFromWriter(jsonSchema, &buf, WithNP(1))
			require.NoError(t, err)

			// Write buffers the row, so a conversion failure surfaces on the flush
			// that WriteStop performs rather than from Write itself.
			err = jw.Write(fmt.Sprintf(`{"id": %q}`, tc.value))
			require.NoError(t, err)
			err = jw.WriteStop()
			if tc.errMsg == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
			}
		})
	}

	t.Run("column_wrong_length", func(t *testing.T) {
		badSchema := `{
			"Tag": "name=parquet-go-root",
			"Fields": [
				{"Tag": "name=id, type=FIXED_LEN_BYTE_ARRAY, length=8, logicaltype=UUID"}
			]
		}`
		var buf bytes.Buffer
		_, err := NewJSONWriterFromWriter(badSchema, &buf, WithNP(1))
		require.Error(t, err)
		require.Contains(t, err.Error(), "LogicalType UUID requires FIXED_LEN_BYTE_ARRAY with length 16")
	})
}

func TestJSONWriterValidatesEncryptionColumnKeys(t *testing.T) {
	t.Parallel()

	jsonSchema := `{
		"Tag": "name=parquet_go_root",
		"Fields": [
			{"Tag": "name=id, type=INT32"},
			{"Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8"}
		]
	}`

	t.Run("unknown column encryption path is rejected", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		_, err := NewJSONWriter(
			jsonSchema,
			writerfile.NewWriterFile(&buf),
			WithFooterKey([]byte("0123456789abcdef")),
			WithColumnEncrypted("invalid_name", ColumnKey([]byte("abcdef0123456789"))),
		)
		require.ErrorContains(t, err, "invalid_name")
	})

	t.Run("valid column encryption path is accepted", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		_, err := NewJSONWriter(
			jsonSchema,
			writerfile.NewWriterFile(&buf),
			WithFooterKey([]byte("0123456789abcdef")),
			WithColumnEncrypted("name", ColumnKey([]byte("abcdef0123456789"))),
		)
		require.NoError(t, err)
	})
}

func TestJSONWriterInterval(t *testing.T) {
	jsonSchema := `{
		"Tag": "name=parquet-go-root",
		"Fields": [
			{"Tag": "name=span, type=FIXED_LEN_BYTE_ARRAY, length=12, convertedtype=INTERVAL"}
		]
	}`

	testCases := map[string]struct {
		value    string
		expected []byte
	}{
		"months_days_seconds": {"2 mon 3 day 4.500 sec", []byte{2, 0, 0, 0, 3, 0, 0, 0, 0x94, 0x11, 0, 0}},
		"days_only":           {"1 day", []byte{0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0}},
		"seconds_only":        {"7200.000 sec", []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0xdd, 0x6d, 0}},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			jw, err := NewJSONWriterFromWriter(jsonSchema, &buf, WithNP(1))
			require.NoError(t, err)
			require.NoError(t, jw.Write(fmt.Sprintf(`{"span": %q}`, tc.value)))
			require.NoError(t, jw.WriteStop())

			pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
			//nolint:staticcheck
			pr, err := reader.NewParquetReader(pf, nil, reader.WithNP(1))
			require.NoError(t, err)

			values, _, _, err := pr.ReadColumnByPathWithContext(t.Context(), "parquet-go-root"+common.ParGoPathDelimiter+"span", 1)
			require.NoError(t, err)
			require.Len(t, values, 1)
			require.Equal(t, string(tc.expected), values[0])

			//nolint:staticcheck
			_ = pr.ReadStop()
			_ = pf.Close()
		})
	}
}

func TestJSONWriterIntervalRejectsMalformed(t *testing.T) {
	jsonSchema := `{
		"Tag": "name=parquet-go-root",
		"Fields": [
			{"Tag": "name=span, type=FIXED_LEN_BYTE_ARRAY, length=12, convertedtype=INTERVAL"}
		]
	}`

	for name, value := range map[string]string{
		"not_an_interval":     "garbage",
		"unknown_unit":        "2 mon 3 zzz",
		"negative_seconds":    "-1 sec",
		"overflowing_seconds": "5000000 sec",
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			jw, err := NewJSONWriterFromWriter(jsonSchema, &buf, WithNP(1))
			require.NoError(t, err)

			// Write buffers the row, so a conversion failure surfaces on the flush
			// that WriteStop performs rather than from Write itself.
			require.NoError(t, jw.Write(fmt.Sprintf(`{"span": %q}`, value)))
			err = jw.WriteStop()
			require.Error(t, err)
			require.Contains(t, err.Error(), "parse INTERVAL")
		})
	}
}

func TestJSONWriterTimeRejectsOutOfRange(t *testing.T) {
	// TIME is elapsed time since midnight, so only [0, 24h) is writable. These used to be
	// stored as given and read back as strings such as "00:00:-1.000" and "25:00:00.000",
	// which rewrote as 0 and 25.
	jsonSchema := `{
		"Tag": "name=parquet-go-root",
		"Fields": [
			{"Tag": "name=millis, type=INT32, convertedtype=TIME_MILLIS"},
			{"Tag": "name=micros, type=INT64, logicaltype=TIME, logicaltype.unit=MICROS, logicaltype.isadjustedtoutc=false"}
		]
	}`

	testCases := map[string]struct {
		millis string
		micros string
	}{
		"negative":          {"-1000", "0"},
		"past_midnight":     {"90000000", "0"},
		"micros_negative":   {"0", "-1000000"},
		"micros_full_day":   {"0", "86400000000"},
		"rendered_clock":    {`"25:00:00.000"`, "0"},
		"fractional":        {"1.9", "0"},
		"negative_fraction": {"-0.5", "0"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			jw, err := NewJSONWriterFromWriter(jsonSchema, &buf, WithNP(1))
			require.NoError(t, err)

			// Write buffers the row, so a conversion failure surfaces on the flush
			// that WriteStop performs rather than from Write itself.
			require.NoError(t, jw.Write(fmt.Sprintf(`{"millis": %s, "micros": %s}`, tc.millis, tc.micros)))
			require.Error(t, jw.WriteStop())
		})
	}

	t.Run("in_range_round_trips", func(t *testing.T) {
		var buf bytes.Buffer
		jw, err := NewJSONWriterFromWriter(jsonSchema, &buf, WithNP(1))
		require.NoError(t, err)
		require.NoError(t, jw.Write(`{"millis": "23:59:59.999", "micros": 86399999999}`))
		require.NoError(t, jw.WriteStop())

		pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
		//nolint:staticcheck
		pr, err := reader.NewParquetReader(pf, nil, reader.WithNP(1))
		require.NoError(t, err)

		millis, _, _, err := pr.ReadColumnByPathWithContext(t.Context(), "parquet-go-root"+common.ParGoPathDelimiter+"millis", 1)
		require.NoError(t, err)
		require.Equal(t, int32(86399999), millis[0])

		micros, _, _, err := pr.ReadColumnByPathWithContext(t.Context(), "parquet-go-root"+common.ParGoPathDelimiter+"micros", 1)
		require.NoError(t, err)
		require.Equal(t, int64(86399999999), micros[0])

		//nolint:staticcheck
		_ = pr.ReadStop()
		_ = pf.Close()
	})
}

func TestJSONWriterDecimalPrecision(t *testing.T) {
	// Every value here needs more than float64's ~15.9 significant digits, the precision
	// DECIMAL exists to provide.
	testCases := map[string]struct {
		field    string
		value    string
		unscaled string
		length   int
	}{
		"int64_scale_0": {
			field:    "type=INT64, convertedtype=DECIMAL, scale=0, precision=18",
			value:    "999999999999999999",
			unscaled: "999999999999999999",
		},
		"int64_scale_2": {
			field:    "type=INT64, convertedtype=DECIMAL, scale=2, precision=18",
			value:    `"9999999999999999.99"`,
			unscaled: "999999999999999999",
		},
		"flba_scale_2": {
			field:    "type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=38, length=16",
			value:    `"123456789012345678901234.56"`,
			unscaled: "12345678901234567890123456",
			length:   16,
		},
		"flba_logicaltype": {
			field:    "type=FIXED_LEN_BYTE_ARRAY, logicaltype=DECIMAL, logicaltype.precision=38, logicaltype.scale=2, length=16",
			value:    `"-123456789012345678901234.56"`,
			unscaled: "-12345678901234567890123456",
			length:   16,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			jsonSchema := fmt.Sprintf(`{
				"Tag": "name=parquet-go-root",
				"Fields": [
					{"Tag": "name=v, %s"}
				]
			}`, tc.field)

			var buf bytes.Buffer
			jw, err := NewJSONWriterFromWriter(jsonSchema, &buf, WithNP(1))
			require.NoError(t, err)
			require.NoError(t, jw.Write(fmt.Sprintf(`{"v": %s}`, tc.value)))
			require.NoError(t, jw.WriteStop())

			pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
			//nolint:staticcheck
			pr, err := reader.NewParquetReader(pf, nil, reader.WithNP(1))
			require.NoError(t, err)

			values, _, _, err := pr.ReadColumnByPathWithContext(t.Context(), "parquet-go-root"+common.ParGoPathDelimiter+"v", 1)
			require.NoError(t, err)
			require.Len(t, values, 1)
			if tc.length == 0 {
				expected, ok := new(big.Int).SetString(tc.unscaled, 10)
				require.True(t, ok)
				require.Equal(t, expected.Int64(), values[0])
			} else {
				require.Equal(t, types.StrIntToBinary(tc.unscaled, "BigEndian", tc.length, true), values[0])
			}

			//nolint:staticcheck
			_ = pr.ReadStop()
			_ = pf.Close()
		})
	}
}

func TestJSONWriterDecimalRejectsOutOfRange(t *testing.T) {
	for name, tc := range map[string]struct {
		field  string
		value  string
		errMsg string
	}{
		"not_a_number": {
			field:  "type=INT64, convertedtype=DECIMAL, scale=2, precision=18",
			value:  `"not a number"`,
			errMsg: "parse DECIMAL",
		},
		"exceeds_precision": {
			field:  "type=INT32, convertedtype=DECIMAL, scale=0, precision=9",
			value:  "99999999999",
			errMsg: "exceeds precision 9",
		},
		"int32_overflow": {
			field:  "type=INT32, convertedtype=DECIMAL, scale=0, precision=11",
			value:  "99999999999",
			errMsg: "does not fit in INT32",
		},
		"flba_too_narrow": {
			field:  "type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=38, length=4",
			value:  `"123456789012345678901234.56"`,
			errMsg: "does not fit in 4 bytes",
		},
	} {
		t.Run(name, func(t *testing.T) {
			jsonSchema := fmt.Sprintf(`{
				"Tag": "name=parquet-go-root",
				"Fields": [
					{"Tag": "name=v, %s"}
				]
			}`, tc.field)

			var buf bytes.Buffer
			jw, err := NewJSONWriterFromWriter(jsonSchema, &buf, WithNP(1))
			require.NoError(t, err)
			require.NoError(t, jw.Write(fmt.Sprintf(`{"v": %s}`, tc.value)))
			err = jw.WriteStop()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestJSONWriterTextConvertedTypes(t *testing.T) {
	testCases := map[string]struct {
		convertedType string
		value         string
	}{
		// Values that are also valid base64 must survive as text rather than being decoded.
		"enum_base64_looking": {"ENUM", "TEST"},
		"enum_plain":          {"ENUM", "ACTIVE"},
		"json_base64_looking": {"JSON", "null"},
		"json_object":         {"JSON", `{"a":1}`},
		"utf8_base64_looking": {"UTF8", "TEST"},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			jsonSchema := fmt.Sprintf(`{
				"Tag": "name=parquet-go-root",
				"Fields": [
					{"Tag": "name=v, type=BYTE_ARRAY, convertedtype=%s"}
				]
			}`, tc.convertedType)

			var buf bytes.Buffer
			jw, err := NewJSONWriterFromWriter(jsonSchema, &buf, WithNP(1))
			require.NoError(t, err)
			row, err := json.Marshal(map[string]string{"v": tc.value})
			require.NoError(t, err)
			require.NoError(t, jw.Write(string(row)))
			require.NoError(t, jw.WriteStop())

			pf := buffer.NewBufferReaderFromBytesNoAlloc(buf.Bytes())
			//nolint:staticcheck
			pr, err := reader.NewParquetReader(pf, nil, reader.WithNP(1))
			require.NoError(t, err)

			values, _, _, err := pr.ReadColumnByPathWithContext(t.Context(), "parquet-go-root"+common.ParGoPathDelimiter+"v", 1)
			require.NoError(t, err)
			require.Len(t, values, 1)
			require.Equal(t, tc.value, values[0])

			//nolint:staticcheck
			_ = pr.ReadStop()
			_ = pf.Close()
		})
	}
}

func TestJSONWriterFixedLenByteArrayWidth(t *testing.T) {
	jsonSchema := `{
		"Tag": "name=parquet-go-root",
		"Fields": [
			{"Tag": "name=V, type=FIXED_LEN_BYTE_ARRAY, length=16"}
		]
	}`

	testCases := map[string]struct {
		value  string
		errMsg string
	}{
		"base64-width-match": {
			"YWJjZGVmZ2hpamtsbW5vcA==", "",
		},
		"decoded-width-decides": {
			// Valid base64, but it decodes to 12 bytes rather than the column's 16;
			// the 16 characters themselves are no longer a second reading.
			"0123456789abcdef",
			`FIXED_LEN_BYTE_ARRAY "0123456789abcdef" decodes to 12 bytes, column length is 16`,
		},
		"not-base64": {
			"abc", `FIXED_LEN_BYTE_ARRAY "abc" is not valid base64`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			jw, err := NewJSONWriter(jsonSchema, writerfile.NewWriterFile(&buf), WithNP(1))
			require.NoError(t, err)

			// A JSON row is converted while the row group is flushed, so a value the
			// converter cannot fit reaches the caller from WriteStop rather than Write.
			require.NoError(t, jw.Write(`{"V":"`+tc.value+`"}`))
			err = jw.WriteStop()
			if tc.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.errMsg)
				return
			}
			require.NoError(t, err)
		})
	}
}
