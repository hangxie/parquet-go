package reader

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/writer"
)

func TestSkipRows_OffsetIndexRoundTrip(t *testing.T) {
	t.Run("dictionary page boundary and interior", func(t *testing.T) {
		type record struct {
			Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
		}
		rows := make([]record, 128)
		for i := range rows {
			rows[i].Name = fmt.Sprintf("value-%02d", i%11)
		}
		data := writeOffsetIndexRecords(t, new(record), rows, writer.WithPageSize(48))

		for _, insidePage := range []bool{false, true} {
			pr, err := NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(data), new(record), WithNP(1))
			require.NoError(t, err)
			index, err := pr.ReadOffsetIndexWithContext(context.Background(), 0, 0)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(index.PageLocations), 3)
			skip := index.PageLocations[2].FirstRowIndex
			if insidePage {
				pageEnd := pr.Footer.RowGroups[0].NumRows
				if len(index.PageLocations) > 3 {
					pageEnd = index.PageLocations[3].FirstRowIndex
				}
				require.Greater(t, pageEnd-skip, int64(1))
				skip++
			}
			require.NoError(t, pr.SkipRows(skip))
			got := make([]record, 1)
			require.NoError(t, pr.Read(&got))
			require.Equal(t, rows[skip], got[0])
			require.NoError(t, pr.ReadStop())
		}

		pr, err := NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(data), new(record), WithNP(1))
		require.NoError(t, err)
		index, err := pr.ReadOffsetIndexWithContext(context.Background(), 0, 0)
		require.NoError(t, err)
		lastPage := index.PageLocations[len(index.PageLocations)-1]
		require.Greater(t, int64(len(rows))-lastPage.FirstRowIndex, int64(1))
		require.NoError(t, pr.SkipRows(lastPage.FirstRowIndex))
		require.NoError(t, pr.SkipRows(1))
		got := make([]record, 1)
		require.NoError(t, pr.Read(&got))
		require.Equal(t, rows[lastPage.FirstRowIndex+1], got[0])
		require.NoError(t, pr.ReadStop())
	})

	t.Run("columns with different page boundaries", func(t *testing.T) {
		type record struct {
			ID     int64   `parquet:"name=id, type=INT64"`
			Label  *string `parquet:"name=label, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
			Values []int64 `parquet:"name=values, type=INT64, repetitiontype=REPEATED"`
		}
		rows := make([]record, 96)
		for i := range rows {
			label := fmt.Sprintf("row-%03d-%s", i, string(bytes.Repeat([]byte{'x'}, i%19)))
			rows[i] = record{ID: int64(i), Label: &label, Values: make([]int64, i%7+1)}
			for j := range rows[i].Values {
				rows[i].Values[j] = int64(i*100 + j)
			}
		}
		data := writeOffsetIndexRecords(t, new(record), rows, writer.WithPageSize(64), writer.WithDataPageVersion(2))
		pr, err := NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(data), new(record), WithNP(3))
		require.NoError(t, err)
		defer func() { require.NoError(t, pr.ReadStop()) }()

		const skip = int64(67)
		require.NoError(t, pr.SkipRows(skip))
		got := make([]record, 1)
		require.NoError(t, pr.Read(&got))
		require.Equal(t, rows[skip], got[0])
	})

	t.Run("missing index falls back", func(t *testing.T) {
		type record struct {
			Value int64 `parquet:"name=value, type=INT64"`
		}
		rows := make([]record, 64)
		for i := range rows {
			rows[i].Value = int64(i)
		}
		data := writeOffsetIndexRecords(t, new(record), rows, writer.WithPageSize(32))
		pr, err := NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(data), new(record), WithNP(1))
		require.NoError(t, err)
		defer func() { require.NoError(t, pr.ReadStop()) }()
		chunk := pr.Footer.RowGroups[0].Columns[0]
		chunk.OffsetIndexOffset = nil
		chunk.OffsetIndexLength = nil

		const skip = int64(37)
		require.NoError(t, pr.SkipRows(skip))
		got := make([]record, 1)
		require.NoError(t, pr.Read(&got))
		require.Equal(t, rows[skip], got[0])
	})
}

func TestSkipRows_EncryptedOffsetIndex(t *testing.T) {
	type record struct {
		ID   int64  `parquet:"name=id, type=INT64"`
		Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY"`
	}
	footerKey := []byte("0123456789abcdef")
	columnKey := []byte("abcdef0123456789")
	rows := make([]record, 128)
	for i := range rows {
		rows[i] = record{ID: int64(i), Name: fmt.Sprintf("name-%02d", i%13)}
	}

	for _, algorithm := range []struct {
		name   string
		option writer.WriterOption
	}{
		{name: "gcm", option: writer.WithEncryptionAlgorithm(writer.EncryptionAESGCMV1)},
		{name: "gcm_ctr", option: writer.WithEncryptionAlgorithm(writer.EncryptionAESGCMCTRV1)},
	} {
		t.Run(algorithm.name, func(t *testing.T) {
			data := writeOffsetIndexRecords(t, new(record), rows,
				writer.WithPageSize(48),
				algorithm.option,
				writer.WithFooterKey(footerKey),
				writer.WithColumnEncrypted("name", writer.ColumnKey(columnKey)),
				writer.WithAADPrefix([]byte("offset-index-test")),
				writer.WithAADFileUnique([]byte("offset-index-001")),
			)
			pr, err := NewParquetReader(
				buffer.NewBufferReaderFromBytesNoAlloc(data),
				new(record),
				WithNP(2),
				WithFooterKey(footerKey),
				WithColumnKey("name", columnKey),
			)
			require.NoError(t, err)
			defer func() { require.NoError(t, pr.ReadStop()) }()

			nameColumn := encryptedColumnIndex(t, pr, "name")
			index, err := pr.ReadOffsetIndexWithContext(context.Background(), 0, nameColumn)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(index.PageLocations), 3)
			skip := index.PageLocations[2].FirstRowIndex
			require.NoError(t, pr.SkipRows(skip))
			got := make([]record, 1)
			require.NoError(t, pr.Read(&got))
			require.Equal(t, rows[skip], got[0])
		})
	}
}

func TestSkipRows_OffsetIndexExternalColumnChunk(t *testing.T) {
	type record struct {
		Value int64 `parquet:"name=value, type=INT64"`
	}
	rows := make([]record, 96)
	for i := range rows {
		rows[i].Value = int64(i)
	}
	data := writeOffsetIndexRecords(t, new(record), rows, writer.WithPageSize(32))
	pf := newRecordingReader("main", map[string][]byte{"main": data, "column.parquet": data})
	pr, err := NewParquetColumnReader(pf, WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()
	index, err := pr.ReadOffsetIndexWithContext(context.Background(), 0, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(index.PageLocations), 3)
	externalPath := "column.parquet"
	pr.Footer.RowGroups[0].Columns[0].FilePath = &externalPath

	skip := index.PageLocations[2].FirstRowIndex
	require.NoError(t, pr.SkipRows(skip))
	values, _, _, err := pr.ReadColumnByIndex(0, 1)
	require.NoError(t, err)
	require.Equal(t, []any{skip}, values)
	require.Positive(t, pf.state.bytesRead(externalPath), "data page must be read from the referenced external file")
}

func TestValidateIndexedSkipTarget(t *testing.T) {
	validMeta := func() *parquet.ColumnMetaData {
		return &parquet.ColumnMetaData{DataPageOffset: 100, TotalCompressedSize: 60}
	}
	validIndex := func() *parquet.OffsetIndex {
		return &parquet.OffsetIndex{PageLocations: []*parquet.PageLocation{
			{Offset: 100, CompressedPageSize: 20, FirstRowIndex: 0},
			{Offset: 120, CompressedPageSize: 20, FirstRowIndex: 10},
			{Offset: 140, CompressedPageSize: 20, FirstRowIndex: 20},
		}}
	}

	tests := []struct {
		name   string
		mutate func(*parquet.ColumnMetaData, *parquet.OffsetIndex)
		want   string
	}{
		{name: "nil page", mutate: func(_ *parquet.ColumnMetaData, index *parquet.OffsetIndex) { index.PageLocations[1] = nil }, want: "page location 1 is nil"},
		{name: "nonzero first row", mutate: func(_ *parquet.ColumnMetaData, index *parquet.OffsetIndex) { index.PageLocations[0].FirstRowIndex = 1 }, want: "want 0"},
		{name: "nonmonotonic rows", mutate: func(_ *parquet.ColumnMetaData, index *parquet.OffsetIndex) { index.PageLocations[2].FirstRowIndex = 10 }, want: "not greater"},
		{name: "negative size", mutate: func(_ *parquet.ColumnMetaData, index *parquet.OffsetIndex) {
			index.PageLocations[1].CompressedPageSize = -1
		}, want: "invalid range"},
		{name: "inconsistent size", mutate: func(_ *parquet.ColumnMetaData, index *parquet.OffsetIndex) {
			index.PageLocations[1].CompressedPageSize = 19
		}, want: "ends at 139"},
		{name: "overlapping offsets", mutate: func(_ *parquet.ColumnMetaData, index *parquet.OffsetIndex) { index.PageLocations[1].Offset = 119 }, want: "next page starts at 119"},
		{name: "outside chunk", mutate: func(_ *parquet.ColumnMetaData, index *parquet.OffsetIndex) { index.PageLocations[2].Offset = 160 }, want: "next page starts at 160"},
		{name: "wrong first offset", mutate: func(meta *parquet.ColumnMetaData, _ *parquet.OffsetIndex) { meta.DataPageOffset = 99 }, want: "does not match"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta, index := validMeta(), validIndex()
			tt.mutate(meta, index)
			_, err := validateIndexedSkipTarget(meta, index, 30, 15)
			require.ErrorContains(t, err, tt.want)
		})
	}

	target, err := validateIndexedSkipTarget(validMeta(), validIndex(), 30, 15)
	require.NoError(t, err)
	require.Equal(t, 1, target.pageIndex)
	require.Equal(t, int64(10), target.firstRow)
	require.Equal(t, 2, target.pageCount)

	_, err = validateIndexedSkipTarget(nil, validIndex(), 30, 15)
	require.ErrorContains(t, err, "column metadata is nil")
	_, err = validateIndexedSkipTarget(validMeta(), &parquet.OffsetIndex{}, 30, 15)
	require.ErrorContains(t, err, "no page locations")
	_, err = validateIndexedSkipTarget(validMeta(), validIndex(), 30, 30)
	require.ErrorContains(t, err, "outside row group")
	invalidChunk := validMeta()
	invalidChunk.TotalCompressedSize = 0
	_, err = validateIndexedSkipTarget(invalidChunk, validIndex(), 30, 15)
	require.ErrorContains(t, err, "invalid column chunk range")
	outsideRow := validIndex()
	outsideRow.PageLocations[2].FirstRowIndex = 30
	_, err = validateIndexedSkipTarget(validMeta(), outsideRow, 30, 15)
	require.ErrorContains(t, err, "outside row group")
	// A last page that ends before the chunk end is accepted: the trailing bytes are
	// padding counted in TotalCompressedSize and, like a sequential read, are never read.
	trailingPadding := validIndex()
	trailingPadding.PageLocations[2].CompressedPageSize = 19
	target, err = validateIndexedSkipTarget(validMeta(), trailingPadding, 30, 25)
	require.NoError(t, err)
	require.Equal(t, 2, target.pageIndex)
	require.Equal(t, 1, target.pageCount)
	// A last page that would extend past the chunk end is still rejected as out of range.
	overrunLastPage := validIndex()
	overrunLastPage.PageLocations[2].CompressedPageSize = 21
	_, err = validateIndexedSkipTarget(validMeta(), overrunLastPage, 30, 25)
	require.ErrorContains(t, err, "invalid range")
}

func TestIndexedPageCursorErrors(t *testing.T) {
	cb := &ColumnBufferType{}
	_, err := cb.newIndexedTransport(buffer.NewBufferReaderFromBytesNoAlloc([]byte{0}), 2, 1)
	require.ErrorContains(t, err, "stopped at 1")

	failingFile := newMockColumnBufferFileReader(nil)
	failingFile.SetShouldFail(true)
	_, err = cb.newIndexedTransport(failingFile, 0, 1)
	require.ErrorContains(t, err, "mock seek error")

	cloneFailure := newMockColumnBufferFileReader(nil)
	cloneFailure.SetCloneFails(true)
	cb = &ColumnBufferType{PFile: cloneFailure, indexedDictionaryPending: true}
	err = cb.ensureIndexedDictionary()
	require.ErrorContains(t, err, "clone column file")
	err = cb.seekToIndexedPage(indexedSkipTarget{}, false)
	require.ErrorContains(t, err, "clone column file")

	dictionaryOffset := int64(5)
	cb = &ColumnBufferType{ChunkHeader: &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{
		DictionaryPageOffset: &dictionaryOffset,
		DataPageOffset:       5,
	}}}
	_, err = cb.readIndexedDictionary(newMockColumnBufferFileReader(nil))
	require.ErrorContains(t, err, "invalid dictionary page offset")

	// A row group that cannot be located makes skipWithOffsetIndex decline the
	// optimization (used=false) rather than error.
	unlocatable := &ColumnBufferType{
		Reader:      &ParquetReader{},
		Footer:      &parquet.FileMetaData{},
		ChunkHeader: &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{NumValues: 4}},
	}
	skipped, used, err := unlocatable.skipWithOffsetIndex(1)
	require.NoError(t, err)
	require.False(t, used)
	require.Zero(t, skipped)

	// seekToIndexedPage closes the cloned handle and surfaces a transport failure when
	// the target data page cannot be reached.
	seekErr := &ColumnBufferType{
		PFile:       buffer.NewBufferReaderFromBytesNoAlloc([]byte{0}),
		ChunkHeader: &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{}},
	}
	err = seekErr.seekToIndexedPage(indexedSkipTarget{offset: 8, chunkEnd: 9, pageCount: 1}, false)
	require.ErrorContains(t, err, "seek to offset 8")

	// ensureIndexedDictionary propagates a dictionary read failure after a successful
	// clone.
	badDictOffset := int64(8)
	ensureErr := &ColumnBufferType{
		PFile:                    buffer.NewBufferReaderFromBytesNoAlloc([]byte{0}),
		indexedDictionaryPending: true,
		ChunkHeader: &parquet.ColumnChunk{MetaData: &parquet.ColumnMetaData{
			DictionaryPageOffset: &badDictOffset,
			DataPageOffset:       8,
		}},
	}
	err = ensureErr.ensureIndexedDictionary()
	require.ErrorContains(t, err, "invalid dictionary page offset")
}

func TestSkipRows_OffsetIndexSeekFailureSurfaces(t *testing.T) {
	type record struct {
		Value int64 `parquet:"name=value, type=INT64"`
	}
	rows := make([]record, 96)
	for i := range rows {
		rows[i].Value = int64(i)
	}
	data := writeOffsetIndexRecords(t, new(record), rows, writer.WithPageSize(32))
	pr, err := NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(data), new(record), WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()
	index, err := pr.ReadOffsetIndexWithContext(context.Background(), 0, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(index.PageLocations), 2)

	cb := pr.ColumnBuffers[pr.SchemaHandler.ValueColumns[0]]
	require.NotNil(t, cb)
	// The offset index is still read through the reader's own file, but the column file
	// can no longer be cloned, so seeking to the target data page fails and the error is
	// surfaced (used=true) instead of silently degrading.
	unclonable := newMockColumnBufferFileReader(nil)
	unclonable.SetCloneFails(true)
	cb.PFile = unclonable

	skipped, used, err := cb.skipWithOffsetIndex(index.PageLocations[1].FirstRowIndex)
	require.True(t, used)
	require.ErrorContains(t, err, "seek row group 0 column 0 to data page")
	require.Zero(t, skipped)

	// Reading a dictionary page from bytes that do not decode into one is reported
	// rather than mistaken for valid dictionary values.
	_, err = cb.readIndexedDictionary(buffer.NewBufferReaderFromBytesNoAlloc(make([]byte, 256)))
	require.ErrorContains(t, err, "dictionary")
}

func TestSkipRows_MalformedOffsetIndexFallsBack(t *testing.T) {
	type record struct {
		Value int64 `parquet:"name=value, type=INT64"`
	}
	rows := make([]record, 64)
	for i := range rows {
		rows[i].Value = int64(i)
	}
	data := writeOffsetIndexRecords(t, new(record), rows, writer.WithPageSize(32))
	inspect, err := NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(data), new(record), WithNP(1))
	require.NoError(t, err)
	index, err := inspect.ReadOffsetIndexWithContext(context.Background(), 0, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(index.PageLocations), 2)
	chunk := inspect.Footer.RowGroups[0].Columns[0]
	offset, length := chunk.GetOffsetIndexOffset(), int(chunk.GetOffsetIndexLength())
	require.NoError(t, inspect.ReadStop())

	// Corrupt the offset index while keeping it a parseable thrift struct, so the reader
	// must detect the invalid page layout during the skip rather than while decoding it.
	index.PageLocations[0].FirstRowIndex = 1
	serializer := thrift.NewTSerializer()
	serializer.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(serializer.Transport)
	malformedModule, err := serializer.Write(context.Background(), index)
	require.NoError(t, err)
	require.Len(t, malformedModule, length)
	copy(data[offset:offset+int64(length)], malformedModule)

	pr, err := NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(data), new(record), WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()
	cb := pr.ColumnBuffers[pr.SchemaHandler.ValueColumns[0]]
	require.NotNil(t, cb)

	// The offset index is only an optimization hint. An invalid one must not fail the
	// skip: it falls back to sequential page reading and still returns the correct row.
	require.NoError(t, pr.SkipRows(17))
	require.False(t, cb.hasIndexedPageCursor())
	require.Positive(t, cb.ChunkReadValues, "fallback must read pages sequentially")
	got := make([]record, 1)
	require.NoError(t, pr.Read(&got))
	require.Equal(t, rows[17], got[0])
}

func TestSkipRows_UnreadableOffsetIndexFallsBack(t *testing.T) {
	type record struct {
		Value int64 `parquet:"name=value, type=INT64"`
	}
	rows := make([]record, 96)
	for i := range rows {
		rows[i].Value = int64(i)
	}
	data := writeOffsetIndexRecords(t, new(record), rows, writer.WithPageSize(32))
	pr, err := NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(data), new(record), WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()
	cb := pr.ColumnBuffers[pr.SchemaHandler.ValueColumns[0]]
	require.NotNil(t, cb)

	// Point the offset index at a region past end of file: reading it fails, but the
	// failure is confined to the index, so the (uncancelled) skip must fall back to
	// sequential reading instead of aborting.
	badOffset := int64(len(data) + 1024)
	pr.Footer.RowGroups[0].Columns[0].OffsetIndexOffset = &badOffset

	require.NoError(t, pr.SkipRows(37))
	require.False(t, cb.hasIndexedPageCursor())
	require.Positive(t, cb.ChunkReadValues, "fallback must read pages sequentially")
	got := make([]record, 1)
	require.NoError(t, pr.Read(&got))
	require.Equal(t, rows[37], got[0])
}

func TestSkipRows_OffsetIndexCancellation(t *testing.T) {
	type record struct {
		Value int64 `parquet:"name=value, type=INT64"`
	}
	rows := make([]record, 64)
	data := writeOffsetIndexRecords(t, new(record), rows, writer.WithPageSize(32))
	pr, err := NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(data), new(record), WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()
	cb := pr.ColumnBuffers[pr.SchemaHandler.ValueColumns[0]]
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cb.PageReadOptions.Context = ctx

	skipped, err := cb.SkipRows(17)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, skipped)
	require.Zero(t, cb.ChunkReadValues)
	require.False(t, cb.hasIndexedPageCursor())
}

func writeOffsetIndexRecords[T any](t *testing.T, schema *T, rows []T, options ...writer.WriterOption) []byte {
	t.Helper()
	var out bytes.Buffer
	options = append([]writer.WriterOption{
		writer.WithNP(1),
		writer.WithRowGroupSize(1 << 30),
		writer.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
	}, options...)
	pw, err := writer.NewParquetWriterWithContext(context.Background(), writerfile.NewWriterFile(&out), schema, options...)
	require.NoError(t, err)
	for _, row := range rows {
		require.NoError(t, pw.WriteWithContext(context.Background(), row))
	}
	require.NoError(t, pw.WriteStopWithContext(context.Background()))
	return append([]byte(nil), out.Bytes()...)
}

func BenchmarkSkipRowsOffsetIndex(b *testing.B) {
	type record struct {
		Value int64 `parquet:"name=value, type=INT64"`
	}
	var out bytes.Buffer
	pw, err := writer.NewParquetWriterWithContext(
		context.Background(),
		writerfile.NewWriterFile(&out),
		new(record),
		writer.WithNP(1),
		writer.WithRowGroupSize(1<<30),
		writer.WithPageSize(128),
		writer.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
	)
	if err != nil {
		b.Fatal(err)
	}
	for i := range int64(4096) {
		if err := pw.WriteWithContext(context.Background(), record{Value: i}); err != nil {
			b.Fatal(err)
		}
	}
	if err := pw.WriteStopWithContext(context.Background()); err != nil {
		b.Fatal(err)
	}
	data := append([]byte(nil), out.Bytes()...)

	for _, depth := range []struct {
		name string
		skip int64
	}{
		{name: "shallow", skip: 17},
		{name: "deep", skip: 3073},
	} {
		for _, indexed := range []bool{true, false} {
			name := "fallback"
			if indexed {
				name = "indexed"
			}
			b.Run(depth.name+"/"+name, func(b *testing.B) {
				runSkipRowsBenchmark(b, data, indexed, depth.skip)
			})
		}
	}
}

func runSkipRowsBenchmark(b *testing.B, data []byte, indexed bool, skip int64) {
	type record struct {
		Value int64 `parquet:"name=value, type=INT64"`
	}
	pf := newRecordingReader("main", map[string][]byte{"main": data})
	pr, err := NewParquetReader(pf, new(record), WithNP(1))
	if err != nil {
		b.Fatal(err)
	}
	if !indexed {
		chunk := pr.Footer.RowGroups[0].Columns[0]
		chunk.OffsetIndexOffset = nil
		chunk.OffsetIndexLength = nil
	}
	b.Cleanup(func() { _ = pr.ReadStop() })
	b.ReportAllocs()
	b.ResetTimer()
	var totalBytes int64
	for range b.N {
		if err := pr.Reset(); err != nil {
			b.Fatal(err)
		}
		pf.state.reset()
		if err := pr.SkipRows(skip); err != nil {
			b.Fatal(err)
		}
		if _, _, _, err := pr.ReadColumnByIndex(0, 1); err != nil {
			b.Fatal(err)
		}
		totalBytes += pf.state.bytesRead(pf.name)
	}
	b.ReportMetric(float64(totalBytes)/float64(b.N), "read-B/op")
}
