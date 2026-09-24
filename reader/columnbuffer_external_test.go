package reader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/writer"
)

type externalRecord struct {
	Value int64 `parquet:"name=value, type=INT64"`
}

// writeExternalChunkFile writes groups row groups of perGroup rows, the nth row holding
// base+n. Only the values depend on base, so every file it writes has the same layout and
// the same offsets are valid in each of them.
func writeExternalChunkFile(t *testing.T, base int64, groups, perGroup int, options ...writer.WriterOption) []byte {
	t.Helper()
	var out bytes.Buffer
	options = append([]writer.WriterOption{
		writer.WithNP(1), writer.WithRowGroupSize(1 << 30), writer.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
	}, options...)
	pw, err := writer.NewParquetWriterWithContext(context.Background(), writerfile.NewWriterFile(&out), new(externalRecord), options...)
	require.NoError(t, err)
	for group := range groups {
		for row := range perGroup {
			require.NoError(t, pw.WriteWithContext(context.Background(), externalRecord{Value: base + int64(group*perGroup+row)}))
		}
		require.NoError(t, pw.FlushWithContext(context.Background(), true))
	}
	require.NoError(t, pw.WriteStopWithContext(context.Background()))
	return append([]byte(nil), out.Bytes()...)
}

// countingFiles serves named in-memory files and counts the handles opened and closed for
// each, so a test can tell which file a column buffer is holding and whether it released it.
type countingFiles struct {
	mu     sync.Mutex
	files  map[string][]byte
	opened map[string]int
	closed map[string]int
}

func newCountingFiles(files map[string][]byte) *countingFiles {
	return &countingFiles{files: files, opened: map[string]int{}, closed: map[string]int{}}
}

func (c *countingFiles) reader(name string) *countingFile {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.opened[name]++
	return &countingFile{set: c, name: name}
}

func (c *countingFiles) counts(name string) (opened, closed int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.opened[name], c.closed[name]
}

type countingFile struct {
	set    *countingFiles
	name   string
	offset int64
	closed bool
}

func (f *countingFile) Read(p []byte) (int, error) {
	if f.closed {
		return 0, fmt.Errorf("read from closed handle on %q", f.name)
	}
	data := f.set.files[f.name]
	if f.offset >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(p, data[f.offset:])
	f.offset += int64(n)
	if f.offset == int64(len(data)) {
		return n, io.EOF
	}
	return n, nil
}

func (f *countingFile) Seek(offset int64, whence int) (int64, error) {
	if f.closed {
		return 0, fmt.Errorf("seek on closed handle on %q", f.name)
	}
	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		f.offset = int64(len(f.set.files[f.name])) + offset
	}
	return f.offset, nil
}

func (f *countingFile) Close() error {
	if f.closed {
		return fmt.Errorf("handle on %q closed twice", f.name)
	}
	f.closed = true
	f.set.mu.Lock()
	f.set.closed[f.name]++
	f.set.mu.Unlock()
	return nil
}

func (f *countingFile) Open(name string) (source.ParquetFileReader, error) {
	if _, ok := f.set.files[name]; !ok {
		return nil, fmt.Errorf("file %q not found", name)
	}
	return f.set.reader(name), nil
}

func (f *countingFile) Clone() (source.ParquetFileReader, error) {
	clone := f.set.reader(f.name)
	clone.offset = f.offset
	return clone, nil
}

// TestReadColumn_ExternalChunkTransitions reads a column whose row groups live in three
// different files, covering every transition: into an external file, between two external
// files, and back to the metadata file.
func TestReadColumn_ExternalChunkTransitions(t *testing.T) {
	const groups, perGroup = 3, 4
	main := writeExternalChunkFile(t, 0, groups, perGroup)
	first := writeExternalChunkFile(t, 1000, groups, perGroup)
	second := writeExternalChunkFile(t, 2000, groups, perGroup)
	require.Equal(t, len(main), len(first))
	require.Equal(t, len(main), len(second))

	files := newCountingFiles(map[string][]byte{"main": main, "first.parquet": first, "second.parquet": second})
	pr, err := NewParquetColumnReader(files.reader("main"), WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()
	require.Len(t, pr.Footer.RowGroups, groups)

	firstPath, secondPath := "first.parquet", "second.parquet"
	pr.Footer.RowGroups[0].Columns[0].FilePath = &firstPath
	pr.Footer.RowGroups[1].Columns[0].FilePath = &secondPath
	// Row group 2 names no file and must be read from the metadata file again.

	values, _, _, err := pr.ReadColumnByIndex(0, groups*perGroup)
	require.NoError(t, err)
	require.Equal(t, []any{
		int64(1000), int64(1001), int64(1002), int64(1003),
		int64(2004), int64(2005), int64(2006), int64(2007),
		int64(8), int64(9), int64(10), int64(11),
	}, values)
}

// TestReadColumn_ExternalChunkHandles checks the handles behind those transitions: an
// external file is released when the buffer moves off it, the metadata file is held for
// the buffer's whole life, and everything is closed once at the end.
func TestReadColumn_ExternalChunkHandles(t *testing.T) {
	const groups, perGroup = 3, 4
	main := writeExternalChunkFile(t, 0, groups, perGroup)
	external := writeExternalChunkFile(t, 1000, groups, perGroup)

	files := newCountingFiles(map[string][]byte{"main": main, "ext.parquet": external})
	pr, err := NewParquetColumnReader(files.reader("main"), WithNP(1))
	require.NoError(t, err)
	externalPath := "ext.parquet"
	pr.Footer.RowGroups[0].Columns[0].FilePath = &externalPath

	_, _, _, err = pr.ReadColumnByIndex(0, groups*perGroup)
	require.NoError(t, err)

	opened, closed := files.counts("ext.parquet")
	require.Equal(t, 1, opened)
	require.Equal(t, 1, closed, "the external handle is released when the buffer moves off it")

	// The reader clones the metadata file per column buffer; the handle the caller passed
	// in stays theirs to close.
	openedMain, closedMain := files.counts("main")
	require.Equal(t, 2, openedMain)
	require.Equal(t, 0, closedMain, "the metadata file is held for the life of the buffer")

	require.NoError(t, pr.ReadStop())
	openedMain, closedMain = files.counts("main")
	require.Equal(t, 2, openedMain)
	require.Equal(t, 1, closedMain, "the buffer's own handle is closed, the caller's is not")
}

// TestReadColumn_ExternalChunkReusesHandle checks that consecutive row groups naming the
// same file are read through one handle rather than reopening it for each.
func TestReadColumn_ExternalChunkReusesHandle(t *testing.T) {
	const groups, perGroup = 3, 4
	main := writeExternalChunkFile(t, 0, groups, perGroup)
	external := writeExternalChunkFile(t, 1000, groups, perGroup)

	files := newCountingFiles(map[string][]byte{"main": main, "ext.parquet": external})
	pr, err := NewParquetColumnReader(files.reader("main"), WithNP(1))
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	externalPath := "ext.parquet"
	pr.Footer.RowGroups[0].Columns[0].FilePath = &externalPath
	pr.Footer.RowGroups[1].Columns[0].FilePath = &externalPath

	values, _, _, err := pr.ReadColumnByIndex(0, groups*perGroup)
	require.NoError(t, err)
	require.Equal(t, []any{
		int64(1000), int64(1001), int64(1002), int64(1003),
		int64(1004), int64(1005), int64(1006), int64(1007),
		int64(8), int64(9), int64(10), int64(11),
	}, values)

	opened, closed := files.counts("ext.parquet")
	require.Equal(t, 1, opened, "one handle serves both row groups that name the file")
	require.Equal(t, 1, closed)
}

// TestSkipRows_ExternalChunkIndexedSkip covers the handle a seek through the offset index
// leaves in place: it is a clone of the chunk's file, and moving to the next row group must
// release it without touching the metadata or external files the buffer owns.
func TestSkipRows_ExternalChunkIndexedSkip(t *testing.T) {
	rows := make([]externalRecord, 96)
	for i := range rows {
		rows[i].Value = int64(i)
	}
	data := writeOffsetIndexRecords(t, new(externalRecord), rows, writer.WithPageSize(32))
	external := make([]externalRecord, len(rows))
	for i := range external {
		external[i].Value = int64(1000 + i)
	}
	externalData := writeOffsetIndexRecords(t, new(externalRecord), external, writer.WithPageSize(32))
	require.Equal(t, len(data), len(externalData))

	files := newCountingFiles(map[string][]byte{"main": data, "ext.parquet": externalData})
	pr, err := NewParquetColumnReader(files.reader("main"), WithNP(1))
	require.NoError(t, err)
	index, err := pr.ReadOffsetIndexWithContext(context.Background(), 0, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(index.PageLocations), 3)

	externalPath := "ext.parquet"
	pr.Footer.RowGroups[0].Columns[0].FilePath = &externalPath

	skip := index.PageLocations[2].FirstRowIndex
	require.NoError(t, pr.SkipRows(skip))
	values, _, _, err := pr.ReadColumnByIndex(0, 1)
	require.NoError(t, err)
	require.Equal(t, []any{int64(1000) + skip}, values)

	require.NoError(t, pr.ReadStop())
	opened, closed := files.counts("ext.parquet")
	require.Equal(t, opened, closed, "every handle on the external file is released")
}

// TestSkipRows_ExternalChunkIndexedRowGroupTransition moves to the next row group after a
// seek through the offset index, so the clone that seek installed is released while the
// metadata file it was cloned from stays open for the row groups that follow.
func TestSkipRows_ExternalChunkIndexedRowGroupTransition(t *testing.T) {
	const groups, perGroup = 3, 96
	main := writeExternalChunkFile(t, 0, groups, perGroup, writer.WithPageSize(32))
	external := writeExternalChunkFile(t, 1000, groups, perGroup, writer.WithPageSize(32))
	require.Equal(t, len(main), len(external))

	files := newCountingFiles(map[string][]byte{"main": main, "ext.parquet": external})
	pr, err := NewParquetColumnReader(files.reader("main"), WithNP(1))
	require.NoError(t, err)
	index, err := pr.ReadOffsetIndexWithContext(context.Background(), 0, 0)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(index.PageLocations), 3)

	externalPath := "ext.parquet"
	pr.Footer.RowGroups[0].Columns[0].FilePath = &externalPath

	skip := index.PageLocations[2].FirstRowIndex
	require.NoError(t, pr.SkipRows(skip))
	// Read to the end of the external row group and on into the next, which is in the
	// metadata file.
	values, _, _, err := pr.ReadColumnByIndex(0, perGroup-skip+2)
	require.NoError(t, err)
	require.Equal(t, int64(1000)+skip, values[0])
	require.Equal(t, int64(1000+perGroup-1), values[perGroup-skip-1])
	require.Equal(t, int64(perGroup), values[perGroup-skip])
	require.Equal(t, int64(perGroup+1), values[perGroup-skip+1])

	require.NoError(t, pr.ReadStop())
	opened, closed := files.counts("ext.parquet")
	require.Equal(t, opened, closed, "every handle on the external file is released")
	openedMain, closedMain := files.counts("main")
	require.Equal(t, openedMain-1, closedMain, "only the handle the caller supplied is left open")
}
