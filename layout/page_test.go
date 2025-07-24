package layout

import (
	"testing"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_NewPage(t *testing.T) {
	page := NewPage()
	if page == nil {
		t.Fatal("Expected non-nil page")
	}
	if page.Header == nil {
		t.Error("Expected Header to be initialized")
	}
	if page.Info == nil {
		t.Error("Expected Info to be initialized")
	}
	if page.DataTable != nil {
		t.Error("Expected DataTable to be nil initially")
	}
}

func Test_NewDictPage(t *testing.T) {
	page := NewDictPage()
	if page == nil {
		t.Fatal("Expected non-nil page")
	}
	if page.Header == nil {
		t.Error("Expected Header to be initialized")
	}
	if page.Header.DictionaryPageHeader == nil {
		t.Error("Expected DictionaryPageHeader to be initialized")
	}
	if page.Info == nil {
		t.Error("Expected Info to be initialized")
	}
}

func Test_NewDataPage(t *testing.T) {
	page := NewDataPage()
	if page == nil {
		t.Fatal("Expected non-nil page")
	}
	if page.Header == nil {
		t.Error("Expected Header to be initialized")
	}
	if page.Header.Type != parquet.PageType_DATA_PAGE {
		t.Errorf("Expected page type to be DATA_PAGE, got %v", page.Header.Type)
	}
	if page.Info == nil {
		t.Error("Expected Info to be initialized")
	}
}

func Test_TableToDataPages(t *testing.T) {
	// Create a simple table with INT32 values
	table := &Table{
		Schema: &parquet.SchemaElement{
			Type: common.ToPtr(parquet.Type_INT32),
			Name: "test_col",
		},
		Values:           []any{int32(1), int32(2), int32(3)},
		DefinitionLevels: []int32{0, 0, 0},
		RepetitionLevels: []int32{0, 0, 0},
		Info:             common.NewTag(),
	}

	pages, totalSize, err := TableToDataPages(table, 1024, parquet.CompressionCodec_UNCOMPRESSED)
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

func Test_TableToDataPagesWithInvalidType(t *testing.T) {
	// Create a table with invalid schema type
	table := &Table{
		Schema: &parquet.SchemaElement{
			Name: "test_col",
			// No Type set - this should cause an error
		},
		Values:           []any{int32(1)},
		DefinitionLevels: []int32{0},
		RepetitionLevels: []int32{0},
		Info:             common.NewTag(),
	}

	_, _, err := TableToDataPages(table, 1024, parquet.CompressionCodec_UNCOMPRESSED)
	if err == nil {
		t.Error("Expected error for invalid schema type")
	}
}

func Test_TableToDataPagesWithEmptyTable(t *testing.T) {
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

	pages, totalSize, err := TableToDataPages(table, 1024, parquet.CompressionCodec_UNCOMPRESSED)
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

func Test_PageEncodingValues(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = common.NewTag()

	values := []any{int32(1), int32(2), int32(3)}
	data, err := page.EncodingValues(values)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected non-empty encoded data")
	}
}

func Test_PageEncodingValuesWithEmptyValues(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = common.NewTag()

	values := []any{}
	data, err := page.EncodingValues(values)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Empty values should produce empty data
	if len(data) != 0 {
		t.Errorf("Expected empty encoded data for empty values, got %d bytes", len(data))
	}
}

func Test_PageDataPageCompress(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = common.NewTag()

	// Set up a data table
	page.DataTable = &Table{
		Values:           []any{int32(1), int32(2)},
		DefinitionLevels: []int32{0, 0},
		RepetitionLevels: []int32{0, 0},
	}

	data, err := page.DataPageCompress(parquet.CompressionCodec_UNCOMPRESSED)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected non-empty compressed data")
	}
}

func Test_PageDataPageV2Compress(t *testing.T) {
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = common.NewTag()

	// Set up a data table
	page.DataTable = &Table{
		Values:           []any{int32(1), int32(2)},
		DefinitionLevels: []int32{0, 0},
		RepetitionLevels: []int32{0, 0},
	}

	data, err := page.DataPageV2Compress(parquet.CompressionCodec_UNCOMPRESSED)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(data) == 0 {
		t.Error("Expected non-empty compressed data")
	}
}
