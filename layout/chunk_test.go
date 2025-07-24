package layout

import (
	"testing"

	"github.com/hangxie/parquet-go/v2/common"
	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_PagesToChunk(t *testing.T) {
	// Create test pages
	page1 := NewDataPage()
	page1.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page1.Info = common.NewTag()
	page1.MaxVal = int32(10)
	page1.MinVal = int32(1)
	nullCount := int64(0)
	page1.NullCount = &nullCount

	// Set up page header for DataPage
	page1.Header.DataPageHeader = &parquet.DataPageHeader{
		NumValues: 5,
	}
	page1.Header.UncompressedPageSize = 100
	page1.Header.CompressedPageSize = 80
	page1.RawData = make([]byte, 80)

	page2 := NewDataPage()
	page2.Schema = page1.Schema
	page2.Info = common.NewTag()
	page2.MaxVal = int32(20)
	page2.MinVal = int32(5)
	nullCount2 := int64(1)
	page2.NullCount = &nullCount2

	page2.Header.DataPageHeader = &parquet.DataPageHeader{
		NumValues: 3,
	}
	page2.Header.UncompressedPageSize = 60
	page2.Header.CompressedPageSize = 50
	page2.RawData = make([]byte, 50)

	pages := []*Page{page1, page2}
	chunk, err := PagesToChunk(pages)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if chunk == nil {
		t.Fatal("Expected non-nil chunk")
	}
	if len(chunk.Pages) != 2 {
		t.Errorf("Expected 2 pages in chunk, got %d", len(chunk.Pages))
	}
	if chunk.ChunkHeader == nil {
		t.Error("Expected ChunkHeader to be set")
	}
}

func Test_PagesToChunkWithDataPageV2(t *testing.T) {
	// Create test page with DataPageV2 header
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = common.NewTag()
	page.MaxVal = int32(10)
	page.MinVal = int32(1)
	nullCount := int64(0)
	page.NullCount = &nullCount

	// Set up page header for DataPageV2
	page.Header.DataPageHeaderV2 = &parquet.DataPageHeaderV2{
		NumValues: 5,
	}
	page.Header.UncompressedPageSize = 100
	page.Header.CompressedPageSize = 80
	page.RawData = make([]byte, 80)

	pages := []*Page{page}
	chunk, err := PagesToChunk(pages)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if chunk == nil {
		t.Fatal("Expected non-nil chunk")
	}
}

func Test_PagesToChunkWithInvalidSchema(t *testing.T) {
	// Create page with invalid schema (no type)
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Name: "test_col",
		// No Type set
	}
	page.Info = common.NewTag()
	page.MaxVal = int32(10)
	page.MinVal = int32(1)
	nullCount := int64(0)
	page.NullCount = &nullCount

	page.Header.DataPageHeader = &parquet.DataPageHeader{
		NumValues: 5,
	}

	pages := []*Page{page}
	_, err := PagesToChunk(pages)

	if err == nil {
		t.Error("Expected error for invalid schema")
	}
}

func Test_PagesToChunkWithOmitStats(t *testing.T) {
	// Create page with omit stats enabled
	page := NewDataPage()
	page.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	page.Info = common.NewTag()
	page.Info.OmitStats = true // Enable omit stats
	page.MaxVal = int32(10)
	page.MinVal = int32(1)
	nullCount := int64(0)
	page.NullCount = &nullCount

	page.Header.DataPageHeader = &parquet.DataPageHeader{
		NumValues: 5,
	}
	page.Header.UncompressedPageSize = 100
	page.Header.CompressedPageSize = 80
	page.RawData = make([]byte, 80)

	pages := []*Page{page}
	chunk, err := PagesToChunk(pages)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if chunk == nil {
		t.Fatal("Expected non-nil chunk")
	}
}

func Test_PagesToDictChunk(t *testing.T) {
	// Create dictionary page
	dictPage := NewDictPage()
	dictPage.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	dictPage.Info = common.NewTag()
	dictPage.MaxVal = int32(10)
	dictPage.MinVal = int32(1)
	nullCount := int64(0)
	dictPage.NullCount = &nullCount

	dictPage.Header.DictionaryPageHeader = &parquet.DictionaryPageHeader{
		NumValues: 3,
	}
	dictPage.Header.UncompressedPageSize = 50
	dictPage.Header.CompressedPageSize = 40
	dictPage.RawData = make([]byte, 40)

	// Create data page
	dataPage := NewDataPage()
	dataPage.Schema = dictPage.Schema
	dataPage.Info = common.NewTag()
	dataPage.MaxVal = int32(10)
	dataPage.MinVal = int32(1)
	nullCount2 := int64(0)
	dataPage.NullCount = &nullCount2

	dataPage.Header.DataPageHeader = &parquet.DataPageHeader{
		NumValues: 5,
	}
	dataPage.Header.UncompressedPageSize = 100
	dataPage.Header.CompressedPageSize = 80
	dataPage.RawData = make([]byte, 80)

	pages := []*Page{dictPage, dataPage}
	chunk, err := PagesToDictChunk(pages)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if chunk == nil {
		t.Fatal("Expected non-nil chunk")
	}
	if len(chunk.Pages) != 2 {
		t.Errorf("Expected 2 pages in chunk, got %d", len(chunk.Pages))
	}
}

func Test_PagesToDictChunkWithInvalidSchema(t *testing.T) {
	// Create dictionary page
	dictPage := NewDictPage()
	dictPage.Schema = &parquet.SchemaElement{
		Name: "dict_col",
	}
	dictPage.Info = common.NewTag()

	// Create data page with invalid schema (no Type set)
	dataPage := NewDataPage()
	dataPage.Schema = &parquet.SchemaElement{
		Name: "test_col",
		// No Type set
	}
	dataPage.Info = common.NewTag()

	pages := []*Page{dictPage, dataPage}
	_, err := PagesToDictChunk(pages)

	if err == nil {
		t.Error("Expected error for invalid schema")
	}
}

func Test_DecodeDictChunk(t *testing.T) {
	// Create a chunk with dictionary and data pages
	dictPage := NewDictPage()
	dictPage.Schema = &parquet.SchemaElement{
		Type: common.ToPtr(parquet.Type_INT32),
		Name: "test_col",
	}
	dictPage.Info = common.NewTag()
	dictPage.DataTable = &Table{
		Values: []any{int32(10), int32(20), int32(30)},
	}

	dataPage := NewDataPage()
	dataPage.Schema = dictPage.Schema
	dataPage.Info = common.NewTag()
	dataPage.DataTable = &Table{
		Values: []any{int64(0), int64(2), int64(1)}, // indices into dictionary
	}

	chunk := &Chunk{
		Pages: []*Page{dictPage, dataPage},
	}

	// This should decode the dictionary indices to actual values
	DecodeDictChunk(chunk)

	// Check that the dictionary page was removed
	if len(chunk.Pages) != 1 {
		t.Errorf("Expected 1 page after decode, got %d", len(chunk.Pages))
	}

	// Check that values were decoded
	if len(chunk.Pages[0].DataTable.Values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(chunk.Pages[0].DataTable.Values))
	}

	// Check specific decoded values
	if chunk.Pages[0].DataTable.Values[0] != int32(10) {
		t.Errorf("Expected first value to be 10, got %v", chunk.Pages[0].DataTable.Values[0])
	}
}
