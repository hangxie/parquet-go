package compress

import (
	"testing"

	"github.com/hangxie/parquet-go/v3/parquet"
)

// fuzzCodecs lists the codecs supported by the default compressor. The index
// into this slice is what the fuzzer selects so it can reach every codec.
var fuzzCodecs = []parquet.CompressionCodec{
	parquet.CompressionCodec_UNCOMPRESSED,
	parquet.CompressionCodec_SNAPPY,
	parquet.CompressionCodec_GZIP,
	parquet.CompressionCodec_BROTLI,
	parquet.CompressionCodec_LZ4,
	parquet.CompressionCodec_ZSTD,
	parquet.CompressionCodec_LZ4_RAW,
}

func FuzzUncompress(f *testing.F) {
	// Seed with valid round-tripped payloads for each codec plus a few
	// malformed inputs.
	sample := []byte("the quick brown fox jumps over the lazy dog")
	for i, c := range fuzzCodecs {
		if enc, err := Compress(sample, c); err == nil {
			f.Add(enc, i)
		}
	}
	f.Add([]byte{}, 0)
	f.Add([]byte{0x00}, 1)
	f.Add([]byte{0xff, 0xff, 0xff, 0xff}, 1)

	// Use a bounded decompressor so the fuzzer exercises the decoders and the
	// size-limit logic without building huge buffers.
	comp, err := NewCompressor(WithMaxDecompressedSize(1 << 20))
	if err != nil {
		f.Fatal(err)
	}

	f.Fuzz(func(t *testing.T, buf []byte, codecSel int) {
		if codecSel < 0 {
			codecSel = -codecSel
		}
		_, _ = comp.Uncompress(buf, fuzzCodecs[codecSel%len(fuzzCodecs)])
	})
}
