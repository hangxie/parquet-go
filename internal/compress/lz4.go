//go:build !no_lz4
// +build !no_lz4

package compress

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/pierrec/lz4/v4"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func init() {
	codecFactories[parquet.CompressionCodec_LZ4] = newLZ4Compressor

	lz4WriterPool := sync.Pool{
		New: func() any {
			return lz4.NewWriter(nil)
		},
	}
	defaultCodecs[parquet.CompressionCodec_LZ4] = &codec{
		compress:   lz4Compress(&lz4WriterPool),
		uncompress: lz4Uncompress,
	}
}

func lz4Compress(pool *sync.Pool) func([]byte) ([]byte, error) {
	return func(buf []byte) ([]byte, error) {
		lz4Writer := pool.Get().(*lz4.Writer)
		defer func() {
			lz4Writer.Reset(nil)
			pool.Put(lz4Writer)
		}()
		res := new(bytes.Buffer)
		lz4Writer.Reset(res)
		if _, err := lz4Writer.Write(buf); err != nil {
			return nil, fmt.Errorf("lz4 compress: %w", err)
		}
		if err := lz4Writer.Close(); err != nil {
			return nil, fmt.Errorf("lz4 compress close: %w", err)
		}
		return res.Bytes(), nil
	}
}

// errNotHadoopLZ4 reports that a payload is not Hadoop framed, so another framing should be tried.
var errNotHadoopLZ4 = errors.New("not hadoop lz4 framing")

// hadoopLZ4LengthSize is the size of the length prefixing a block or a chunk.
const hadoopLZ4LengthSize = 4

// hadoopLZ4MaxExpansion bounds how far a raw LZ4 block can expand.
const hadoopLZ4MaxExpansion = 255

// hadoopLZ4Length reads a block or chunk length prefix.
func hadoopLZ4Length(buf []byte) int64 {
	// Hadoop's rawReadInt puts these in a Java int, so the high bit means negative
	// and invalid rather than a length past 2GiB.
	return int64(int32(binary.BigEndian.Uint32(buf[0:4])))
}

// hadoopLZ4Uncompress decodes the Hadoop LZ4 framing that parquet-mr writes for the deprecated LZ4 codec.
func hadoopLZ4Uncompress(buf []byte, maxSize int64) ([]byte, error) {
	// A block length, then chunks each prefixed by their own length. Every length
	// is checked against the payload so an LZ4 frame is rejected, not decoded.
	var res []byte
	var total int64

	// The smallest thing Hadoop writes is an empty block's four-byte zero length.
	if len(buf) == 0 {
		return nil, fmt.Errorf("empty payload: %w", errNotHadoopLZ4)
	}

	for len(buf) > 0 {
		if len(buf) < hadoopLZ4LengthSize {
			return nil, fmt.Errorf("truncated block length: %w", errNotHadoopLZ4)
		}
		blockLen := hadoopLZ4Length(buf)
		buf = buf[hadoopLZ4LengthSize:]

		if blockLen < 0 {
			return nil, fmt.Errorf("negative block length %d: %w", blockLen, errNotHadoopLZ4)
		}

		// BlockDecompressorStream reads a zero length as EOF, so bytes behind one
		// are bytes Hadoop would never return.
		if blockLen == 0 {
			if len(buf) > 0 {
				return nil, fmt.Errorf("%d bytes remain after the zero block length that ends the stream: %w",
					len(buf), errNotHadoopLZ4)
			}
			break
		}

		// Judged before the size limit: a limit error for a buffer that was never
		// Hadoop framed would suppress the frame fallback.
		if blockLen > int64(len(buf))*hadoopLZ4MaxExpansion {
			return nil, fmt.Errorf("block length %d exceeds what %d remaining bytes can produce: %w",
				blockLen, len(buf), errNotHadoopLZ4)
		}

		total += blockLen
		if maxSize > 0 && total > maxSize {
			return nil, fmt.Errorf("decompressed data (%d bytes) exceeds maximum size %d: %w",
				total, maxSize, ErrDecompressedSizeExceeded)
		}

		// The block declares its decompressed size, so grow it onto the result and
		// let its chunks decode straight into it.
		start := int64(len(res))
		res = append(res, make([]byte, blockLen)...)

		var err error
		if buf, err = fillHadoopLZ4Block(buf, res[start:start+blockLen:start+blockLen]); err != nil {
			return nil, err
		}
	}

	return res, nil
}

// fillHadoopLZ4Block decodes chunks into block until it is full, returning what is left of buf.
func fillHadoopLZ4Block(buf, block []byte) ([]byte, error) {
	// A chunk that would overrun the block fails below, and a block the chunks
	// cannot fill runs out of payload, so neither passes as Hadoop framing.
	for filled := 0; filled < len(block); {
		if len(buf) < hadoopLZ4LengthSize {
			return nil, fmt.Errorf("truncated chunk length %d bytes into a %d byte block: %w",
				filled, len(block), errNotHadoopLZ4)
		}
		chunkLen := hadoopLZ4Length(buf)
		buf = buf[hadoopLZ4LengthSize:]
		if chunkLen < 0 {
			return nil, fmt.Errorf("negative chunk length %d: %w", chunkLen, errNotHadoopLZ4)
		}
		if chunkLen == 0 || chunkLen > int64(len(buf)) {
			return nil, fmt.Errorf("chunk length %d out of range for %d remaining bytes: %w",
				chunkLen, len(buf), errNotHadoopLZ4)
		}

		// Decoding into the unfilled tail caps a chunk at the space left.
		count, err := lz4.UncompressBlock(buf[:chunkLen], block[filled:])
		if err != nil || count <= 0 {
			return nil, fmt.Errorf("decode chunk into the %d bytes left of a %d byte block (%v): %w",
				len(block)-filled, len(block), err, errNotHadoopLZ4)
		}
		filled += count
		buf = buf[chunkLen:]
	}
	return buf, nil
}

// lz4Uncompress decodes the deprecated LZ4 codec, which two incompatible framings share.
func lz4Uncompress(buf []byte, maxSize int64) ([]byte, error) {
	// Frame first: a frame is self-describing, while its magic doubles as a valid
	// Hadoop block length (69356824), so the reverse order would misclassify.
	res, frameErr := limitedReadAll(lz4.NewReader(bytes.NewReader(buf)), maxSize)
	if frameErr == nil {
		return res, nil
	}
	// Only a real frame decodes far enough to breach the limit, so report that
	// rather than letting the Hadoop decoder reinterpret the same bytes.
	if errors.Is(frameErr, ErrDecompressedSizeExceeded) {
		return nil, frameErr
	}

	res, err := hadoopLZ4Uncompress(buf, maxSize)
	if err == nil {
		return res, nil
	}
	if errors.Is(err, ErrDecompressedSizeExceeded) {
		return nil, err
	}
	// The frame error leads, but a truncated parquet-mr page needs the Hadoop
	// diagnosis too: alone, the frame error names the wrong framing entirely.
	return nil, errors.Join(frameErr, err)
}

func lz4CompressWithLevel(pool *sync.Pool, cl lz4.CompressionLevel) func([]byte) ([]byte, error) {
	return func(buf []byte) ([]byte, error) {
		lz4Writer := pool.Get().(*lz4.Writer)
		defer func() {
			lz4Writer.Reset(nil)
			pool.Put(lz4Writer)
		}()
		if err := lz4Writer.Apply(lz4.CompressionLevelOption(cl)); err != nil {
			return nil, fmt.Errorf("lz4 compress: %w", err)
		}
		res := new(bytes.Buffer)
		lz4Writer.Reset(res)
		if _, err := lz4Writer.Write(buf); err != nil {
			return nil, fmt.Errorf("lz4 compress: %w", err)
		}
		if err := lz4Writer.Close(); err != nil {
			return nil, fmt.Errorf("lz4 compress close: %w", err)
		}
		return res.Bytes(), nil
	}
}

func newLZ4Compressor(level *int) (*codec, error) {
	cl := lz4.CompressionLevel(1 << 12)
	l := 4
	if level != nil {
		l = *level
		cl = lz4.CompressionLevel(1 << (8 + l))
	}

	// Validate by creating a writer and applying the option
	testWriter := lz4.NewWriter(nil)
	if err := testWriter.Apply(lz4.CompressionLevelOption(cl)); err != nil {
		return nil, fmt.Errorf("invalid lz4 compression level %d: %w", l, err)
	}

	writerPool := sync.Pool{
		New: func() any {
			return lz4.NewWriter(nil)
		},
	}

	return &codec{
		compress:   lz4CompressWithLevel(&writerPool, cl),
		uncompress: lz4Uncompress,
	}, nil
}
