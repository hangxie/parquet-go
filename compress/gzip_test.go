package compress

import (
	"bytes"
	"testing"

	"github.com/hangxie/parquet-go/v2/parquet"
)

func Test_GzipCompression(t *testing.T) {
	gzipCompressor := compressors[parquet.CompressionCodec_GZIP]
	input := []byte("test data")
	compressed := gzipCompressor.Compress(input)
	output, err := gzipCompressor.Uncompress(compressed)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(input, output) {
		t.Fatalf("expected output %s but was %s", string(input), string(output))
	}
}

func Benchmark_GzipCompression(b *testing.B) {
	gzipCompressor := compressors[parquet.CompressionCodec_GZIP]
	input := []byte("test data")
	b.ReportAllocs()
	for b.Loop() {
		gzipCompressor.Compress(input)
	}
}
