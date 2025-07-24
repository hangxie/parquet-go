package layout

import (
	"testing"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_NewDictRec(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)
	if dictRec == nil {
		t.Fatal("Expected non-nil DictRecType")
	}
	if dictRec.DictMap == nil {
		t.Error("Expected DictMap to be initialized")
	}
	// DictSlice is not initialized by NewDictRec - it's nil initially
	if dictRec.Type != parquet.Type_INT32 {
		t.Errorf("Expected Type to be INT32, got %v", dictRec.Type)
	}
}

func Test_DictRecToDictPage(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)

	// Add some test values to the dictionary
	dictRec.DictSlice = append(dictRec.DictSlice, int32(1))
	dictRec.DictSlice = append(dictRec.DictSlice, int32(2))
	dictRec.DictSlice = append(dictRec.DictSlice, int32(3))
	dictRec.DictMap[int32(1)] = 0
	dictRec.DictMap[int32(2)] = 1
	dictRec.DictMap[int32(3)] = 2

	page, totalSize, err := DictRecToDictPage(dictRec, 1024, parquet.CompressionCodec_UNCOMPRESSED)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if page == nil {
		t.Fatal("Expected non-nil page")
	}
	if page.Header.Type != parquet.PageType_DICTIONARY_PAGE {
		t.Errorf("Expected DICTIONARY_PAGE type, got %v", page.Header.Type)
	}
	if totalSize <= 0 {
		t.Errorf("Expected positive total size, got %d", totalSize)
	}
}

func Test_DictRecToDictPageWithEmptyDict(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)
	// Don't add any values - test empty dictionary

	page, totalSize, err := DictRecToDictPage(dictRec, 1024, parquet.CompressionCodec_UNCOMPRESSED)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if page == nil {
		t.Fatal("Expected non-nil page")
	}
	if totalSize < 0 {
		t.Errorf("Expected non-negative total size, got %d", totalSize)
	}
}

func Test_DictPageCompress(t *testing.T) {
	page := NewDictPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = common.NewTag()

	// Set up some data for the dictionary page
	page.DataTable = &Table{
		Values: []any{int32(1), int32(2), int32(3)},
	}

	data, err := page.DictPageCompress(parquet.CompressionCodec_UNCOMPRESSED, parquet.Type_INT32)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected non-empty compressed data")
	}
}

func Test_DictPageCompressWithEmptyDataTable(t *testing.T) {
	page := NewDictPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = common.NewTag()
	page.DataTable = &Table{
		Values: []any{}, // empty values slice
	}

	data, err := page.DictPageCompress(parquet.CompressionCodec_UNCOMPRESSED, parquet.Type_INT32)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected non-empty compressed data even with empty values")
	}
}

func Test_TableToDictDataPages(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)

	// Add some dictionary values
	dictRec.DictSlice = append(dictRec.DictSlice, int32(10))
	dictRec.DictSlice = append(dictRec.DictSlice, int32(20))
	dictRec.DictMap[int32(10)] = 0
	dictRec.DictMap[int32(20)] = 1

	// Create a table with values that reference the dictionary
	table := &Table{
		Schema: &parquet.SchemaElement{
			Type: common.ToPtr(parquet.Type_INT32),
			Name: "test_col",
		},
		Values:           []any{int32(10), int32(20), int32(10)},
		DefinitionLevels: []int32{0, 0, 0},
		RepetitionLevels: []int32{0, 0, 0},
		Info:             common.NewTag(),
	}

	pages, totalSize, err := TableToDictDataPages(dictRec, table, 1024, 2, parquet.CompressionCodec_UNCOMPRESSED)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(pages) == 0 {
		t.Error("Expected at least one page")
	}
	if totalSize <= 0 {
		t.Errorf("Expected positive total size, got %d", totalSize)
	}
}

func Test_TableToDictDataPagesWithInvalidType(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)

	// Create a table with invalid schema
	table := &Table{
		Schema: &parquet.SchemaElement{
			Name: "test_col",
			// No Type set
		},
		Values:           []any{int32(1)},
		DefinitionLevels: []int32{0},
		RepetitionLevels: []int32{0},
		Info:             common.NewTag(),
	}

	_, _, err := TableToDictDataPages(dictRec, table, 1024, 1, parquet.CompressionCodec_UNCOMPRESSED)
	if err == nil {
		t.Error("Expected error for invalid schema type")
	}
}

func Test_TableToDictDataPagesWithEmptyTable(t *testing.T) {
	dictRec := NewDictRec(parquet.Type_INT32)

	// Create an empty table
	table := &Table{
		Schema: &parquet.SchemaElement{
			Type: common.ToPtr(parquet.Type_INT32),
			Name: "test_col",
		},
		Values:           []any{},
		DefinitionLevels: []int32{},
		RepetitionLevels: []int32{},
		Info:             common.NewTag(),
	}

	pages, totalSize, err := TableToDictDataPages(dictRec, table, 1024, 1, parquet.CompressionCodec_UNCOMPRESSED)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(pages) != 0 {
		t.Errorf("Expected no pages for empty table, got %d", len(pages))
	}
	if totalSize != 0 {
		t.Errorf("Expected zero total size for empty table, got %d", totalSize)
	}
}

func Test_DictDataPageCompress(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = common.NewTag()

	// Set up DataTable with proper definition and repetition levels
	page.DataTable = &Table{
		DefinitionLevels:   []int32{0, 0, 0, 0},
		RepetitionLevels:   []int32{0, 0, 0, 0},
		MaxDefinitionLevel: 0,
		MaxRepetitionLevel: 0,
	}

	// Test values representing dictionary indices
	values := []int32{0, 1, 0, 2}

	data, err := page.DictDataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, 2, values)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected non-empty compressed data")
	}
}

func Test_DictDataPageCompressWithEmptyValues(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = common.NewTag()

	// Set up DataTable
	page.DataTable = &Table{
		DefinitionLevels:   []int32{},
		RepetitionLevels:   []int32{},
		MaxDefinitionLevel: 0,
		MaxRepetitionLevel: 0,
	}

	// Test with empty values slice
	values := []int32{}

	data, err := page.DictDataPageCompress(parquet.CompressionCodec_UNCOMPRESSED, 1, values)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected non-empty compressed data even with empty values")
	}
}
