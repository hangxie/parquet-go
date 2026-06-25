package reader

import (
	"bytes"
	"testing"

	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
	"github.com/hangxie/parquet-go/v3/writer"
)

type fuzzReaderRow struct {
	ID   int64    `parquet:"name=id, type=INT64, bloomfilter=true"`
	Name string   `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, bloomfilter=true"`
	Val  *int32   `parquet:"name=val, type=INT32, repetitiontype=OPTIONAL"`
	Tags []string `parquet:"name=tags, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
}

// validReaderSeed builds a small, valid parquet file (with bloom filters and a
// page index) to seed the fuzzer. Coverage-guided mutation of this seed reaches
// the malformed-footer, column-buffer, page-read, bloom, and index error paths.
func validReaderSeed(tb testing.TB) []byte {
	tb.Helper()
	var buf bytes.Buffer
	fw := writerfile.NewWriterFile(&buf)
	pw, err := writer.NewParquetWriter(fw, new(fuzzReaderRow), writer.WithNP(1))
	if err != nil {
		tb.Fatalf("new writer: %v", err)
	}
	v := int32(7)
	rows := []fuzzReaderRow{
		{ID: 1, Name: "alice", Val: &v, Tags: []string{"a", "b"}},
		{ID: 2, Name: "bob", Val: nil, Tags: []string{}},
	}
	for _, r := range rows {
		if err := pw.Write(r); err != nil {
			tb.Fatalf("write: %v", err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		tb.Fatalf("write stop: %v", err)
	}
	return buf.Bytes()
}

// FuzzParquetReader drives arbitrary byte streams through the reader's
// file-reading entry points (footer parse, column buffers, page reads, bloom
// filters, page index). The contract under test is that malformed input is
// rejected with an error rather than panicking. A fixed schema object is used so
// the fuzzer targets the file/footer/page readers rather than the separate
// concern of building Go reflect types from an untrusted footer schema.
func FuzzParquetReader(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("PAR1"))
	f.Add([]byte("PAR1PAR1"))
	f.Add(validReaderSeed(f))

	f.Fuzz(func(t *testing.T, data []byte) {
		fr := buffer.NewBufferReaderFromBytesNoAlloc(data)
		pr, err := NewParquetReader(fr, new(fuzzReaderRow), WithNP(1))
		if err != nil {
			return // malformed footer/schema rejected cleanly
		}

		// Exercise the row-reading path (column buffers -> page reads -> unmarshal).
		_, _ = pr.ReadByNumber(8)

		// Exercise per-column index and bloom-filter readers when present.
		if pr.Footer != nil {
			for rg := range pr.Footer.RowGroups {
				_, _ = pr.ReadOffsetIndex(rg, 0)
				_, _ = pr.ReadColumnIndex(rg, 0)
				_, _ = pr.GetAllPageHeaders(rg, 0)
				_, _ = pr.BloomFilterCheck("id", rg, int64(1))
				_, _ = pr.BloomFilterCheck("name", rg, "alice")
			}
		}

		// Reset and stop must also tolerate whatever state we reached.
		_ = pr.Reset()
		_ = pr.ReadStop()
	})
}
