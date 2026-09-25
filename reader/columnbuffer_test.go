package reader

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/writer"
)

func (m *mockColumnBufferFileReader) SetShouldFail(shouldFail bool) {
	m.shouldFail = shouldFail
}

func (m *mockColumnBufferFileReader) SetCloneFails(cloneFails bool) {
	m.cloneFails = cloneFails
}

func (m *mockColumnBufferFileReader) SetOpenFails(openFails bool) {
	m.openFails = openFails
}

func (m *mockColumnBufferFileReader) Seek(offset int64, whence int) (int64, error) {
	if m.shouldFail {
		return 0, fmt.Errorf("mock seek error")
	}
	if m.closed {
		return 0, fmt.Errorf("reader is closed")
	}
	switch whence {
	case io.SeekStart:
		m.offset = offset
	case io.SeekCurrent:
		m.offset += offset
	case io.SeekEnd:
		m.offset = int64(len(m.data)) + offset
	}
	return m.offset, nil
}

func (m *mockColumnBufferFileReader) Close() error {
	m.closed = true
	return nil
}

func (m *mockColumnBufferFileReader) Open(name string) (source.ParquetFileReader, error) {
	if m.shouldFail || m.openFails {
		return nil, fmt.Errorf("mock open error")
	}
	return newMockColumnBufferFileReader(m.data), nil
}

func (m *mockColumnBufferFileReader) Clone() (source.ParquetFileReader, error) {
	if m.cloneFails {
		return nil, fmt.Errorf("mock clone error")
	}
	if m.shouldFail {
		return nil, fmt.Errorf("mock clone error")
	}
	newReader := newMockColumnBufferFileReader(m.data)
	newReader.offset = m.offset
	// propagate flags so behaviors on the cloned reader remain consistent
	newReader.closed = m.closed
	newReader.shouldFail = m.shouldFail
	newReader.cloneFails = m.cloneFails
	newReader.openFails = m.openFails
	m.clones = append(m.clones, newReader)
	return newReader, nil
}

// Helper function to create a mock schema handler with basic setup
func newMockSchemaHandler() *schema.SchemaHandler {
	return &schema.SchemaHandler{
		SchemaElements: []*parquet.SchemaElement{
			{
				Name: "root",
			},
		},
		Infos: []*common.Tag{
			{
				InName: "root",
				ExName: "root",
			},
		},
		MapIndex:       make(map[string]int32),
		IndexMap:       make(map[int32]string),
		InPathToExPath: make(map[string]string),
		ExPathToInPath: make(map[string]string),
	}
}

// helper to build a minimal schema handler containing a root and one leaf at the given path
func newSchemaHandlerWithPath(path string) *schema.SchemaHandler {
	sh := &schema.SchemaHandler{
		SchemaElements: []*parquet.SchemaElement{
			{ // 0: root
				Name: "root",
			},
			{ // 1: leaf
				Name: path,
				Type: common.ToPtr(parquet.Type_INT64),
			},
		},
		Infos:          []*common.Tag{{InName: "root", ExName: "root"}},
		MapIndex:       make(map[string]int32),
		IndexMap:       make(map[int32]string),
		InPathToExPath: make(map[string]string),
		ExPathToInPath: make(map[string]string),
	}
	fq := common.PathToStr([]string{"root", path})
	sh.MapIndex[fq] = 1
	sh.IndexMap[1] = fq
	sh.InPathToExPath[fq] = fq
	sh.ExPathToInPath[fq] = fq
	return sh
}

func TestNewColumnBuffer(t *testing.T) {
	tests := []struct {
		name           string
		setupFile      func() source.ParquetFileReader
		setupFooter    func() *parquet.FileMetaData
		setupSchema    func() *schema.SchemaHandler
		pathStr        string
		expectError    bool
		expectedError  string
		validateResult func(t *testing.T, cb *ColumnBufferType)
	}{
		{
			name: "nil_file",
			setupFile: func() source.ParquetFileReader {
				return nil
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:       "test.field",
			expectError:   true,
			expectedError: "pFile is nil",
		},
		{
			name: "clone_fails",
			setupFile: func() source.ParquetFileReader {
				mock := newMockColumnBufferFileReader([]byte{})
				mock.SetCloneFails(true)
				return mock
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:       "test.field",
			expectError:   true,
			expectedError: "mock clone error",
		},
		{
			name: "nil_footer",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return nil
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:       "test.field",
			expectError:   true,
			expectedError: "footer is nil",
		},
		{
			name: "nil_schema_handler",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{},
				}
			},
			setupSchema: func() *schema.SchemaHandler {
				return nil
			},
			pathStr:       "test.field",
			expectError:   true,
			expectedError: "schema handler is nil",
		},
		{
			name: "empty_path",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{},
				}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:     "",
			expectError: false, // Empty footer means NextRowGroup returns EOF which is handled
			validateResult: func(t *testing.T, cb *ColumnBufferType) {
				require.Empty(t, cb.PathStr)
				require.Equal(t, int64(-1), cb.DataTableNumRows)
				require.Equal(t, int64(0), cb.RowGroupIndex)
			},
		},
		{
			name: "empty_footer_success",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{}, // Empty row groups
				}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:     "test.field",
			expectError: false, // Empty footer means NextRowGroup returns EOF which is handled
			validateResult: func(t *testing.T, cb *ColumnBufferType) {
				require.Equal(t, "test.field", cb.PathStr)
				require.Equal(t, int64(-1), cb.DataTableNumRows)
				require.Equal(t, int64(0), cb.RowGroupIndex)
			},
		},
		{
			name: "single_row_group_column_not_found",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{
						{
							Columns: []*parquet.ColumnChunk{
								{
									MetaData: &parquet.ColumnMetaData{
										PathInSchema: []string{"other_field"},
									},
								},
							},
						},
					},
				}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:       "test.field",
			expectError:   true,
			expectedError: "[NextRowGroup] Column not found: test.field",
		},
		{
			name: "single_row_group_column_found",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{
						{
							Columns: []*parquet.ColumnChunk{
								{
									MetaData: &parquet.ColumnMetaData{
										PathInSchema:   []string{"test_field"},
										DataPageOffset: int64(100),
									},
									FilePath: nil,
								},
							},
						},
					},
				}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:     common.PathToStr([]string{"root", "test_field"}),
			expectError: false,
			validateResult: func(t *testing.T, cb *ColumnBufferType) {
				require.Equal(t, common.PathToStr([]string{"root", "test_field"}), cb.PathStr)
				require.Equal(t, int64(1), cb.RowGroupIndex)
				require.NotNil(t, cb.ChunkHeader)
				require.Equal(t, int64(-1), cb.DataTableNumRows)
			},
		},
		{
			name: "multiple_columns_correct_match",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{
						{
							Columns: []*parquet.ColumnChunk{
								{
									MetaData: &parquet.ColumnMetaData{
										PathInSchema:   []string{"field1"},
										DataPageOffset: int64(50),
									},
								},
								{
									MetaData: &parquet.ColumnMetaData{
										PathInSchema:   []string{"field2"},
										DataPageOffset: int64(100),
									},
								},
								{
									MetaData: &parquet.ColumnMetaData{
										PathInSchema:   []string{"target_field"},
										DataPageOffset: int64(150),
									},
								},
							},
						},
					},
				}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:     common.PathToStr([]string{"root", "target_field"}),
			expectError: false,
			validateResult: func(t *testing.T, cb *ColumnBufferType) {
				require.Equal(t, common.PathToStr([]string{"root", "target_field"}), cb.PathStr)
				// Should find the third column (index 2)
				require.NotNil(t, cb.ChunkHeader)
				expectedPath := []string{"target_field"}
				actualPath := cb.ChunkHeader.MetaData.GetPathInSchema()
				require.Len(t, actualPath, len(expectedPath))
				require.Equal(t, expectedPath[0], actualPath[0])
			},
		},
		{
			name: "with_dictionary_page_offset",
			setupFile: func() source.ParquetFileReader {
				return newMockColumnBufferFileReader([]byte{})
			},
			setupFooter: func() *parquet.FileMetaData {
				return &parquet.FileMetaData{
					RowGroups: []*parquet.RowGroup{
						{
							Columns: []*parquet.ColumnChunk{
								{
									MetaData: &parquet.ColumnMetaData{
										PathInSchema:         []string{"dict_field"},
										DataPageOffset:       int64(200),
										DictionaryPageOffset: common.ToPtr(int64(100)), // Dictionary comes before data
									},
								},
							},
						},
					},
				}
			},
			setupSchema: func() *schema.SchemaHandler {
				return newMockSchemaHandler()
			},
			pathStr:     common.PathToStr([]string{"root", "dict_field"}),
			expectError: false,
			validateResult: func(t *testing.T, cb *ColumnBufferType) {
				require.Equal(t, common.PathToStr([]string{"root", "dict_field"}), cb.PathStr)
				require.NotNil(t, cb.ChunkHeader)
				// The function should use dictionary page offset when available
				require.NotNil(t, cb.ChunkHeader.MetaData.DictionaryPageOffset)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pFile := tt.setupFile()
			footer := tt.setupFooter()
			schemaHandler := tt.setupSchema()

			result, err := NewColumnBuffer(pFile, footer, schemaHandler, tt.pathStr, nil)

			if tt.expectError {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
				require.Nil(t, result)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)

				// Validate basic fields are set correctly
				require.Equal(t, footer, result.Footer)
				require.Equal(t, schemaHandler, result.SchemaHandler)
				require.Equal(t, tt.pathStr, result.PathStr)
				require.NotNil(t, result.PFile)

				if tt.validateResult != nil {
					tt.validateResult(t, result)
				}
			}
		})
	}
}

func TestNewColumnBuffer_EdgeCases(t *testing.T) {
	t.Run("complex_nested_path", func(t *testing.T) {
		mockFile := newMockColumnBufferFileReader([]byte{})
		footer := &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{
					Columns: []*parquet.ColumnChunk{
						{
							MetaData: &parquet.ColumnMetaData{
								PathInSchema:   []string{"nested", "deep", "field"},
								DataPageOffset: int64(100),
							},
						},
					},
				},
			},
		}
		schemaHandler := newMockSchemaHandler()

		result, err := NewColumnBuffer(mockFile, footer, schemaHandler, common.PathToStr([]string{"root", "nested", "deep", "field"}), nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, common.PathToStr([]string{"root", "nested", "deep", "field"}), result.PathStr)
	})

	t.Run("file_path_specified", func(t *testing.T) {
		mockFile := newMockColumnBufferFileReader([]byte{})
		filePath := "external_file.parquet"
		footer := &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{
					Columns: []*parquet.ColumnChunk{
						{
							MetaData: &parquet.ColumnMetaData{
								PathInSchema:   []string{"external_field"},
								DataPageOffset: int64(100),
							},
							FilePath: &filePath,
						},
					},
				},
			},
		}
		schemaHandler := newMockSchemaHandler()

		result, err := NewColumnBuffer(mockFile, footer, schemaHandler, common.PathToStr([]string{"root", "external_field"}), nil)
		require.NoError(t, err)
		require.NotNil(t, result)
		// When FilePath is specified, the function should handle opening the external file
		require.NotNil(t, result.ChunkHeader.FilePath)
		require.Equal(t, filePath, *result.ChunkHeader.FilePath)
	})

	t.Run("readrows_with_error_propagates", func(t *testing.T) {
		footer := &parquet.FileMetaData{
			NumRows: 1,
			RowGroups: []*parquet.RowGroup{{
				Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{PathInSchema: []string{"leaf"}, DataPageOffset: 0, NumValues: 1, Type: parquet.Type_INT64, Codec: parquet.CompressionCodec_UNCOMPRESSED}}},
			}},
		}
		sh := newSchemaHandlerWithPath("bogus") // MapIndex includes root.bogus
		cb := &ColumnBufferType{Footer: footer, SchemaHandler: sh, PathStr: common.PathToStr([]string{"root", "bogus"}), DataTableNumRows: -1}

		_, _, err := cb.ReadRows(1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Column not found")
	})

	t.Run("skiprows_with_error_propagates", func(t *testing.T) {
		footer := &parquet.FileMetaData{
			NumRows: 1,
			RowGroups: []*parquet.RowGroup{{
				Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{PathInSchema: []string{"leaf"}, DataPageOffset: 0, NumValues: 1, Type: parquet.Type_INT64, Codec: parquet.CompressionCodec_UNCOMPRESSED}}},
			}},
		}
		sh := newSchemaHandlerWithPath("bogus")
		cb := &ColumnBufferType{Footer: footer, SchemaHandler: sh, PathStr: common.PathToStr([]string{"root", "bogus"}), DataTableNumRows: -1}

		_, err := cb.SkipRows(1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "Column not found")
	})

	t.Run("gettype_error_on_corrupted_schema", func(t *testing.T) {
		mockFile := newMockColumnBufferFileReader([]byte{})
		footer := &parquet.FileMetaData{RowGroups: []*parquet.RowGroup{}}
		sh := &schema.SchemaHandler{
			SchemaElements: []*parquet.SchemaElement{{Name: "root"}, {Name: "badfield"}}, // No Type set
			MapIndex:       map[string]int32{common.PathToStr([]string{"root", "badfield"}): 1},
		}

		cb, err := NewColumnBuffer(mockFile, footer, sh, common.PathToStr([]string{"root", "badfield"}), nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "path not found")
		require.Nil(t, cb)
	})

	t.Run("nextrowgroup_no_increment_when_empty", func(t *testing.T) {
		mockFile := newMockColumnBufferFileReader([]byte{})
		footer := &parquet.FileMetaData{RowGroups: []*parquet.RowGroup{}}
		sh := newSchemaHandlerWithPath("leaf")

		cb := &ColumnBufferType{
			PFile: mockFile, Footer: footer, SchemaHandler: sh,
			PathStr: common.PathToStr([]string{"root", "leaf"}), DataTableNumRows: -1, RowGroupIndex: 0,
		}

		err := cb.NextRowGroup()
		require.Equal(t, io.EOF, err)
		require.Equal(t, int64(-1), cb.DataTableNumRows) // Should NOT increment
	})
}

func TestNewColumnBuffer_FilePathOpenError(t *testing.T) {
	mockFile := newMockColumnBufferFileReader([]byte{})
	mockFile.SetOpenFails(true)

	filePath := "external.parquet"
	footer := &parquet.FileMetaData{
		RowGroups: []*parquet.RowGroup{
			{Columns: []*parquet.ColumnChunk{{
				MetaData: &parquet.ColumnMetaData{
					PathInSchema:   []string{"leaf"},
					DataPageOffset: 0,
					NumValues:      1,
					Type:           parquet.Type_INT64,
					Codec:          parquet.CompressionCodec_UNCOMPRESSED,
				},
				FilePath: &filePath,
			}}},
		},
	}
	sh := newSchemaHandlerWithPath("leaf")

	cb, err := NewColumnBuffer(mockFile, footer, sh, common.PathToStr([]string{"root", "leaf"}), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mock open error")
	require.Nil(t, cb)
	// The constructor discards the buffer on failure, so the cloned reader it
	// created must be closed rather than leaked.
	require.Len(t, mockFile.clones, 1)
	require.True(t, mockFile.clones[0].closed, "cloned reader must be closed on constructor failure")
}

// A failed Open on an external column-chunk file must leave cbt.PFile intact
// rather than clobbering it with the nil interface Open returns on error;
// otherwise a subsequent ReadStop would call Close on a nil interface and panic.
func TestNextRowGroup_OpenFailurePreservesPFile(t *testing.T) {
	mockFile := newMockColumnBufferFileReader([]byte{})
	mockFile.SetOpenFails(true)

	filePath := "external.parquet"
	cbt := &ColumnBufferType{
		PFile: mockFile,
		Footer: &parquet.FileMetaData{
			RowGroups: []*parquet.RowGroup{
				{Columns: []*parquet.ColumnChunk{{
					MetaData: &parquet.ColumnMetaData{
						PathInSchema:   []string{"leaf"},
						DataPageOffset: 0,
						NumValues:      1,
						Type:           parquet.Type_INT64,
						Codec:          parquet.CompressionCodec_UNCOMPRESSED,
					},
					FilePath: &filePath,
				}}},
			},
		},
		SchemaHandler: newSchemaHandlerWithPath("leaf"),
		PathStr:       common.PathToStr([]string{"root", "leaf"}),
		RowGroupIndex: 0,
	}

	err := cbt.NextRowGroup()
	require.Error(t, err)
	require.Contains(t, err.Error(), "mock open error")
	// PFile must still point at the original reader, not a nil interface.
	require.NotNil(t, cbt.PFile)
	require.Same(t, mockFile, cbt.PFile)
	require.NotPanics(t, func() { _ = cbt.PFile.Close() })
}

// skipCountRecord is a single-column row used to build real multi-page fixtures.
type skipCountRecord struct {
	V int64 `parquet:"name=v, type=INT64"`
}

type dictionaryRecord struct {
	V string `parquet:"name=v, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

// newDictionaryOnlyChunkBuffer builds a chunk containing a valid dictionary page
// followed immediately by EOF, with metadata that still declares data values.
func newDictionaryOnlyChunkBuffer(t *testing.T) *ColumnBufferType {
	t.Helper()

	ctx := context.Background()
	fw := buffer.NewBufferWriter()
	pw, err := writer.NewParquetWriterWithContext(ctx, fw, new(dictionaryRecord))
	require.NoError(t, err)
	for _, value := range []string{"a", "b", "a"} {
		require.NoError(t, pw.WriteWithContext(ctx, dictionaryRecord{V: value}))
	}
	require.NoError(t, pw.WriteStopWithContext(ctx))

	src, err := NewParquetReaderWithContext(ctx, buffer.NewBufferReaderFromBytes(fw.Bytes()), new(dictionaryRecord))
	require.NoError(t, err)
	md := src.Footer.RowGroups[0].Columns[0].MetaData
	require.NotNil(t, md.DictionaryPageOffset)

	dictionaryBytes := fw.Bytes()[*md.DictionaryPageOffset:md.DataPageOffset]
	dictionaryOffset := int64(0)
	footer := &parquet.FileMetaData{
		NumRows: 3,
		Schema:  src.Footer.Schema,
		RowGroups: []*parquet.RowGroup{{
			NumRows: 3,
			Columns: []*parquet.ColumnChunk{{MetaData: &parquet.ColumnMetaData{
				Type:                 md.Type,
				Encodings:            md.Encodings,
				PathInSchema:         md.PathInSchema,
				Codec:                md.Codec,
				NumValues:            md.NumValues,
				TotalCompressedSize:  int64(len(dictionaryBytes)),
				DataPageOffset:       int64(len(dictionaryBytes)),
				DictionaryPageOffset: &dictionaryOffset,
			}}},
		}},
	}
	cb, err := NewColumnBuffer(buffer.NewBufferReaderFromBytes(dictionaryBytes), footer, src.SchemaHandler, src.SchemaHandler.ValueColumns[0], nil)
	require.NoError(t, err)
	return cb
}

// buildThreeRowPage writes a single uncompressed data page holding rows 0,1,2 and
// returns just that page's bytes plus the source reader (for its schema/metadata), so
// tests can splice the page into synthetic footers.
func buildThreeRowPage(t *testing.T) ([]byte, *parquet.ColumnMetaData, *ParquetReader) {
	t.Helper()
	fw := buffer.NewBufferWriter()
	pw, err := writer.NewParquetWriterWithContext(context.Background(), fw, new(skipCountRecord),
		writer.WithPageSize(1<<20), writer.WithRowGroupSize(1<<30),
		writer.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED))
	require.NoError(t, err)
	for i := range int64(3) {
		require.NoError(t, pw.WriteWithContext(context.Background(), skipCountRecord{V: i}))
	}
	require.NoError(t, pw.WriteStopWithContext(context.Background()))

	src, err := NewParquetReader(buffer.NewBufferReaderFromBytes(fw.Bytes()), new(skipCountRecord))
	require.NoError(t, err)
	md := src.Footer.RowGroups[0].Columns[0].MetaData
	require.Nil(t, md.DictionaryPageOffset, "fixture assumes a single data page with no dictionary")
	return fw.Bytes()[md.DataPageOffset : md.DataPageOffset+md.TotalCompressedSize], md, src
}

func chunkFor(md *parquet.ColumnMetaData, offset, numValues, totalCompressedSize int64) *parquet.ColumnChunk {
	return &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{
		PathInSchema:        md.PathInSchema,
		Type:                md.Type,
		Codec:               md.Codec,
		Encodings:           md.Encodings,
		DataPageOffset:      offset,
		NumValues:           numValues,
		TotalCompressedSize: totalCompressedSize,
	}}
}

// newTruncatedChunkBuffer builds a column buffer over a chunk that declares 5 values
// but is backed by a single real 3-row data page followed immediately by EOF.
func newTruncatedChunkBuffer(t *testing.T) *ColumnBufferType {
	t.Helper()
	pageBytes, md, src := buildThreeRowPage(t)
	footer := &parquet.FileMetaData{
		NumRows: 5,
		Schema:  src.Footer.Schema,
		RowGroups: []*parquet.RowGroup{
			{NumRows: 5, Columns: []*parquet.ColumnChunk{chunkFor(md, 0, 5, int64(len(pageBytes)))}},
		},
	}
	cb, err := NewColumnBuffer(buffer.NewBufferReaderFromBytes(pageBytes), footer, src.SchemaHandler, src.SchemaHandler.ValueColumns[0], nil)
	require.NoError(t, err)
	return cb
}

// TestNextRowGroup_NilColumnMetaData guards the nil-pointer panic fuzzing found:
// a footer row group containing a column chunk with no MetaData must be skipped
// rather than dereferenced.
func TestNextRowGroup_NilColumnMetaData(t *testing.T) {
	sh := schema.NewSchemaHandlerFromSchemaList([]*parquet.SchemaElement{
		{Name: "parquet_go_root", NumChildren: common.ToPtr(int32(1)), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
		{Name: "col", Type: common.ToPtr(parquet.Type_INT32), RepetitionType: common.ToPtr(parquet.FieldRepetitionType_REQUIRED)},
	})
	footer := &parquet.FileMetaData{
		NumRows: 1,
		RowGroups: []*parquet.RowGroup{
			{NumRows: 1, Columns: []*parquet.ColumnChunk{{MetaData: nil}}},
		},
	}
	pf := buffer.NewBufferReaderFromBytesNoAlloc([]byte("PAR1\x00\x00\x00\x00PAR1"))

	require.NotPanics(t, func() {
		// Unmatched path: the nil-metadata chunk is skipped, yielding a
		// "column not found" error rather than a panic.
		_, err := NewColumnBuffer(pf, footer, sh, "Parquet_go_root\x01Missing", nil)
		require.Error(t, err)
	})
}
