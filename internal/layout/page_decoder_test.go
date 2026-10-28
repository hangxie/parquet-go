package layout

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestPageDecoderHugeLevels(t *testing.T) {
	const count = int32(1<<31 - 1)
	for _, tt := range []struct {
		repetition parquet.FieldRepetitionType
		null       bool
	}{
		{parquet.FieldRepetitionType_REQUIRED, false},
		{parquet.FieldRepetitionType_OPTIONAL, false},
		{parquet.FieldRepetitionType_OPTIONAL, true},
		{parquet.FieldRepetitionType_REPEATED, false},
		{parquet.FieldRepetitionType_REPEATED, true},
	} {
		repetition, null := tt.repetition, tt.null
		t.Run(fmt.Sprintf("%s/null=%t", repetition, null), func(t *testing.T) {
			sh := testSchemaHandler()
			sh.SchemaElements[1].Type = common.ToPtr(parquet.Type_BOOLEAN)
			sh.SchemaElements[1].RepetitionType = common.ToPtr(repetition)
			var data []byte
			level := func(v byte) []byte {
				run := append(encoding.WriteUnsignedVarInt(uint64(count)<<1), v)
				return append([]byte{byte(len(run)), 0, 0, 0}, run...)
			}
			if repetition == parquet.FieldRepetitionType_REPEATED {
				data = append(data, level(0)...)
			}
			if repetition != parquet.FieldRepetitionType_REQUIRED {
				v := byte(1)
				if null {
					v = 0
				}
				data = append(data, level(v)...)
			}
			if !null {
				data = append(data, level(1)...)
			}
			p := NewDataPage()
			p.Path = common.StrToPath(sh.IndexMap[1])
			p.Header.Type = parquet.PageType_DATA_PAGE
			p.Header.DataPageHeader.NumValues = count
			p.Header.DataPageHeader.Encoding = parquet.Encoding_RLE
			require.NoError(t, p.initDecoder(data, sh, int64(count)))
			n, rows, err := p.ReadBatch(false)
			require.NoError(t, err)
			require.Equal(t, int64(PageBatchSize), n)
			require.Equal(t, n, rows)
			require.True(t, p.HasMoreValues())
			n, _, err = p.ReadBatch(false)
			require.NoError(t, err)
			require.Equal(t, int64(PageBatchSize), n)
			require.NoError(t, p.decodeBatchValues())
			require.NoError(t, p.decodeBatchValues())
			for _, v := range p.DataTable.Values {
				if null {
					require.Nil(t, v)
				} else {
					require.Equal(t, true, v)
				}
			}
		})
	}
}

func TestPageDecoderErrors(t *testing.T) {
	sh := testSchemaHandler()
	p := NewDataPage()
	p.Path = common.StrToPath(sh.IndexMap[1])
	p.Header.Type = parquet.PageType_DATA_PAGE
	p.Header.DataPageHeader.NumValues = -1
	require.ErrorContains(t, p.initDecoder(nil, sh, 0), "negative")
	p.Header.DataPageHeader.NumValues = 2
	require.ErrorContains(t, p.initDecoder(nil, sh, 1), "exceeds column chunk")
	p.Header.DataPageHeader = nil
	require.ErrorContains(t, p.initDecoder(nil, sh, 0), "missing")
	p.Header.Type = parquet.PageType_DATA_PAGE_V2
	require.ErrorContains(t, p.initDecoder(nil, sh, 0), "missing")
	p.Header.Type = parquet.PageType_INDEX_PAGE
	require.ErrorContains(t, p.initDecoder(nil, sh, 0), "unsupported")
	_, _, err := p.ReadBatch(true)
	require.Error(t, err)
	require.Error(t, p.decodeBatchValues())
}

func TestPageDecoderSkipFlatRows(t *testing.T) {
	sh := testSchemaHandler()
	sh.SchemaElements[1].Type = common.ToPtr(parquet.Type_BOOLEAN)
	p := NewDataPage()
	p.Path = common.StrToPath(sh.IndexMap[1])
	p.Header.Type = parquet.PageType_DATA_PAGE
	p.Header.DataPageHeader.NumValues = 1<<31 - 1
	p.Header.DataPageHeader.Encoding = parquet.Encoding_RLE
	body := append(encoding.WriteUnsignedVarInt(uint64(1<<31-1)<<1), byte(1))
	data := append([]byte{byte(len(body)), 0, 0, 0}, body...)
	require.NoError(t, p.initDecoder(data, sh, 0))
	n, err := p.SkipFlatRows(1<<31 - 3)
	require.NoError(t, err)
	require.Equal(t, int64(1<<31-3), n)
	n, rows, err := p.ReadBatch(true)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
	require.Equal(t, n, rows)
	require.Equal(t, []any{true, true}, p.DataTable.Values)
	n, err = p.SkipFlatRows(1)
	require.NoError(t, err)
	require.Zero(t, n)
	n, err = p.SkipFlatRows(0)
	require.NoError(t, err)
	require.Zero(t, n)
	p.decoder = nil
	n, err = p.SkipFlatRows(1)
	require.NoError(t, err)
	require.Zero(t, n)
}

func TestPageDecoderMalformedLevelsAndValues(t *testing.T) {
	for _, tt := range []struct {
		name       string
		repetition parquet.FieldRepetitionType
		data       []byte
		method     parquet.Encoding
		want       string
	}{
		{"truncated repetition section", parquet.FieldRepetitionType_REPEATED, nil, parquet.Encoding_PLAIN, "read repetition levels"},
		{"truncated definition section", parquet.FieldRepetitionType_OPTIONAL, nil, parquet.Encoding_PLAIN, "read definition levels"},
		{"truncated definition run", parquet.FieldRepetitionType_OPTIONAL, []byte{1, 0, 0, 0, 2}, parquet.Encoding_PLAIN, "read definition levels"},
		{"truncated repetition run", parquet.FieldRepetitionType_REPEATED, []byte{1, 0, 0, 0, 2, 2, 0, 0, 0, 2, 1}, parquet.Encoding_PLAIN, "read repetition levels"},
		{"repetition level out of range", parquet.FieldRepetitionType_REPEATED, []byte{2, 0, 0, 0, 2, 2, 2, 0, 0, 0, 2, 1}, parquet.Encoding_PLAIN, "page level out of range"},
		{"unknown encoding", parquet.FieldRepetitionType_REQUIRED, nil, parquet.Encoding(-1), "unknown Encoding"},
		{"missing value", parquet.FieldRepetitionType_REQUIRED, nil, parquet.Encoding_PLAIN, "read data page values"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sh := testSchemaHandler()
			sh.SchemaElements[1].RepetitionType = common.ToPtr(tt.repetition)
			p := NewDataPage()
			p.Path = common.StrToPath(sh.IndexMap[1])
			p.Header.Type = parquet.PageType_DATA_PAGE
			p.Header.DataPageHeader.NumValues = 1
			p.Header.DataPageHeader.Encoding = tt.method
			err := p.initDecoder(tt.data, sh, 0)
			if err == nil {
				_, _, err = p.ReadBatch(true)
			}
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestDropRowContinuation(t *testing.T) {
	p := &Page{DataTable: &Table{Values: []any{int64(1), int64(2)}, RepetitionLevels: []int32{1, 1}, DefinitionLevels: []int32{1, 1}, MaxDefinitionLevel: 1}, decoder: &pageDecoder{}}
	require.True(t, p.DropRowContinuation())
	require.Empty(t, p.DataTable.Values)
	require.Equal(t, uint64(2), p.decoder.batchStart)
	p.DataTable = &Table{Values: []any{nil, int64(2), int64(3)}, RepetitionLevels: []int32{1, 0, 1}, DefinitionLevels: []int32{0, 1, 1}, MaxDefinitionLevel: 1}
	require.False(t, p.DropRowContinuation())
	require.Equal(t, []any{int64(2), int64(3)}, p.DataTable.Values)
}

func TestPageDecoderSkippedMalformedValues(t *testing.T) {
	sh := testSchemaHandler()
	sh.SchemaElements[1].Type = common.ToPtr(parquet.Type_BOOLEAN)
	p := NewDataPage()
	p.Path = common.StrToPath(sh.IndexMap[1])
	p.Header.Type = parquet.PageType_DATA_PAGE
	p.Header.DataPageHeader.NumValues = PageBatchSize + 1
	p.Header.DataPageHeader.Encoding = parquet.Encoding_RLE
	require.NoError(t, p.initDecoder([]byte{1, 0, 0, 0, 2}, sh, 0))
	_, _, err := p.ReadBatch(false)
	require.NoError(t, err)
	_, _, err = p.ReadBatch(false)
	require.NoError(t, err)
	require.ErrorContains(t, p.decodeBatchValues(), "skip data page values")
}

func TestPageDecoderImplicitLevelsAcrossBatches(t *testing.T) {
	sh := testSchemaHandler()
	values := make([]any, 2*PageBatchSize+1)
	for i := range values {
		values[i] = int32(i)
	}
	data, err := encoding.WritePlainINT32(values)
	require.NoError(t, err)
	p := NewDataPage()
	p.Path = common.StrToPath(sh.IndexMap[1])
	p.Header.Type = parquet.PageType_DATA_PAGE
	p.Header.DataPageHeader.NumValues = int32(len(values))
	p.Header.DataPageHeader.Encoding = parquet.Encoding_PLAIN
	require.NoError(t, p.initDecoder(data, sh, int64(len(values))))
	n, rows, err := p.ReadBatch(true)
	require.NoError(t, err)
	require.Equal(t, int64(PageBatchSize), n)
	require.Equal(t, n, rows)
	require.Equal(t, values[:PageBatchSize], p.DataTable.Values)
	require.Equal(t, make([]int32, PageBatchSize), p.DataTable.DefinitionLevels)
	require.Equal(t, make([]int32, PageBatchSize), p.DataTable.RepetitionLevels)
	n, err = p.SkipFlatRows(PageBatchSize)
	require.NoError(t, err)
	require.Equal(t, int64(PageBatchSize), n)
	n, rows, err = p.ReadBatch(true)
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
	require.Equal(t, n, rows)
	require.Equal(t, values[2*PageBatchSize:], p.DataTable.Values)
	require.False(t, p.HasMoreValues())
}

func TestPageDecoderByteBudgetPerBatch(t *testing.T) {
	const valueLen = 1024
	sh := testSchemaHandler()
	sh.SchemaElements[1].Type = common.ToPtr(parquet.Type_BYTE_ARRAY)
	// Large shared prefixes make every value after the first depend on prefix state carried from its predecessor.
	values := make([]any, 2*PageBatchSize)
	for i := range values {
		values[i] = fmt.Sprintf("%0*d", valueLen, i)
	}
	data := encoding.WriteDeltaByteArray(values)
	open := func(t *testing.T) *Page {
		p := NewDataPage()
		p.Path = common.StrToPath(sh.IndexMap[1])
		p.Header.Type = parquet.PageType_DATA_PAGE
		p.Header.DataPageHeader.NumValues = int32(len(values))
		p.Header.DataPageHeader.Encoding = parquet.Encoding_DELTA_BYTE_ARRAY
		require.NoError(t, p.initDecoder(data, sh, int64(len(values))))
		return p
	}
	defer func(limit int64) { maxDecodedBatchBytes = limit }(maxDecodedBatchBytes)

	// One batch fits exactly, so the second succeeds only if the budget resets per batch.
	maxDecodedBatchBytes = PageBatchSize * valueLen
	for _, decodeFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("decode-first=%t", decodeFirst), func(t *testing.T) {
			p := open(t)
			_, _, err := p.ReadBatch(decodeFirst)
			require.NoError(t, err)
			if decodeFirst {
				require.Equal(t, values[:PageBatchSize], p.DataTable.Values)
			}
			// A skipped first batch is replayed through skip() when the second batch is decoded.
			_, _, err = p.ReadBatch(true)
			require.NoError(t, err)
			require.Equal(t, values[PageBatchSize:], p.DataTable.Values)
		})
	}

	maxDecodedBatchBytes--
	_, _, err := open(t).ReadBatch(true)
	require.ErrorContains(t, err, "budget")
	// Skipped values draw nothing, so the full shortfall lands on the decoded batch.
	p := open(t)
	_, _, err = p.ReadBatch(false)
	require.NoError(t, err)
	_, _, err = p.ReadBatch(true)
	require.ErrorContains(t, err, "budget")
}
