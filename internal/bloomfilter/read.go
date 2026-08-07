package bloomfilter

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// maxHeaderModuleBytes is the ceiling for an encrypted header module. Its plaintext is already
// bounded by the Thrift decoder's message limit, and the module carries a GCM nonce and tag on
// top, so no header that could be decoded is refused earlier however many extension fields it
// carries. Bounds taken from the file only ever tighten this.
const maxHeaderModuleBytes = thrift.DEFAULT_MAX_MESSAGE_SIZE + encryption.GCMOverhead

// ReadOptions configures encrypted bloom filter reads.
type ReadOptions struct {
	Context         context.Context
	Key             []byte
	AADPrefix       []byte
	AADFileUnique   []byte
	RowGroupOrdinal int16
	ColumnOrdinal   int16

	// MaxHeaderModuleSize bounds the encrypted header module, normally the filter's stored
	// length from ColumnMetaData. Zero falls back to maxHeaderModuleBytes. Metadata is part of
	// the file, so it only ever tightens the bound.
	MaxHeaderModuleSize int64
}

// ReadBloomFilter reads a bloom filter from the given ReadSeeker at the specified offset.
// It reads the Thrift-encoded BloomFilterHeader followed by the raw bitset bytes.
//
// Deprecated: use ReadBloomFilterWithContext.
func ReadBloomFilter(r io.ReadSeeker, offset int64) (*Filter, error) {
	return ReadBloomFilterWithContext(context.Background(), r, offset)
}

// ReadBloomFilterWithContext reads a bloom filter using ctx.
func ReadBloomFilterWithContext(ctx context.Context, r io.ReadSeeker, offset int64) (*Filter, error) {
	header, err := readHeaderAt(ctx, r, offset)
	if err != nil {
		return nil, err
	}

	// Re-serialize the header to determine its exact byte size,
	// then seek to the bitset position (offset + headerSize).
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	headerBuf, err := ts.Write(ctx, header)
	if err != nil {
		return nil, fmt.Errorf("serialize bloom filter header to determine size: %w", err)
	}

	bitsetOffset := offset + int64(len(headerBuf))
	if _, err := r.Seek(bitsetOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to bloom filter bitset: %w", err)
	}

	bitset := make([]byte, header.NumBytes)
	if _, err := io.ReadFull(r, bitset); err != nil {
		return nil, fmt.Errorf("read bloom filter bitset: %w", err)
	}

	return FromBitset(bitset)
}

// ReadBloomFilterSize returns a bloom filter's bitset size without reading the bitset.
func ReadBloomFilterSize(ctx context.Context, r io.ReadSeeker, offset int64) (int32, error) {
	header, err := readHeaderAt(ctx, r, offset)
	if err != nil {
		return 0, err
	}
	// The bitset is never read, so its size cannot be checked against the bytes that
	// follow; reject headers a full read would reject once it saw the bitset.
	if err := validateBitsetSize(int(header.NumBytes)); err != nil {
		return 0, err
	}
	return header.NumBytes, nil
}

// ReadEncryptedBloomFilterSize returns an encrypted bloom filter's bitset size without reading the bitset.
func ReadEncryptedBloomFilterSize(r io.ReadSeeker, offset int64, opt ReadOptions) (int32, error) {
	header, err := readEncryptedHeaderAt(r, offset, opt)
	if err != nil {
		return 0, err
	}
	if err := validateBitsetSize(int(header.NumBytes)); err != nil {
		return 0, err
	}
	return header.NumBytes, nil
}

// ReadEncryptedBloomFilter reads an encrypted bloom filter header and bitset
// from the given ReadSeeker at the specified offset.
func ReadEncryptedBloomFilter(r io.ReadSeeker, offset int64, opt ReadOptions) (*Filter, error) {
	header, err := readEncryptedHeaderAt(r, offset, opt)
	if err != nil {
		return nil, err
	}

	// The decrypted header states the bitset size, so the module cannot exceed it plus GCM overhead.
	bitsetLimit := limitToAvailable(r, int64(header.NumBytes)+encryption.GCMOverhead)
	bitsetModule, err := encryption.ReadModule(r, bitsetLimit)
	if err != nil {
		return nil, fmt.Errorf("read encrypted bloom filter bitset: %w", err)
	}
	bitset, err := encryption.DecryptGCM(opt.Key, bloomAAD(opt, encryption.ModuleBloomFilterBitset), bitsetModule)
	if err != nil {
		return nil, fmt.Errorf("decrypt bloom filter bitset: %w", err)
	}
	if int32(len(bitset)) != header.NumBytes {
		return nil, fmt.Errorf("bloom filter bitset length %d does not match header numBytes %d", len(bitset), header.NumBytes)
	}
	return FromBitset(bitset)
}

// readHeaderAt reads and validates the plaintext header stored at offset.
func readHeaderAt(ctx context.Context, r io.ReadSeeker, offset int64) (*parquet.BloomFilterHeader, error) {
	if _, err := r.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to bloom filter offset %d: %w", offset, err)
	}
	header, err := readBloomFilterHeader(ctx, r)
	if err != nil {
		return nil, err
	}
	if header.NumBytes <= 0 {
		return nil, fmt.Errorf("invalid bloom filter header: numBytes=%d", header.NumBytes)
	}
	return header, nil
}

// readEncryptedHeaderAt decrypts and validates the header module stored at offset,
// leaving the reader positioned at the bitset module.
func readEncryptedHeaderAt(r io.ReadSeeker, offset int64, opt ReadOptions) (*parquet.BloomFilterHeader, error) {
	ctx := opt.Context
	if ctx == nil {
		ctx = context.Background()
	}
	if _, err := r.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to bloom filter offset %d: %w", offset, err)
	}

	limit := int64(maxHeaderModuleBytes)
	if opt.MaxHeaderModuleSize > 0 && opt.MaxHeaderModuleSize < limit {
		limit = opt.MaxHeaderModuleSize
	}
	headerModule, err := encryption.ReadModule(r, limitToAvailable(r, limit))
	if err != nil {
		return nil, fmt.Errorf("read encrypted bloom filter header: %w", err)
	}
	headerBytes, err := encryption.DecryptGCM(opt.Key, bloomAAD(opt, encryption.ModuleBloomFilterHeader), headerModule)
	if err != nil {
		return nil, fmt.Errorf("decrypt bloom filter header: %w", err)
	}
	header, err := readBloomFilterHeader(ctx, bytes.NewReader(headerBytes))
	if err != nil {
		return nil, fmt.Errorf("decode decrypted bloom filter header: %w", err)
	}
	if header.NumBytes <= 0 {
		return nil, fmt.Errorf("invalid bloom filter header: numBytes=%d", header.NumBytes)
	}
	return header, nil
}

// limitToAvailable tightens a module size limit to the bytes left in the file, so a length
// prefix cannot authorize an allocation the file could never satisfy. Readers that cannot
// report their length keep the caller's limit.
func limitToAvailable(r io.ReadSeeker, limit int64) int64 {
	cur, err := r.Seek(0, io.SeekCurrent)
	if err != nil {
		return limit
	}
	end, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return limit
	}
	if _, err := r.Seek(cur, io.SeekStart); err != nil {
		return limit
	}
	if available := end - cur; available > 0 && available < limit {
		return available
	}
	return limit
}

func readBloomFilterHeader(ctx context.Context, r io.Reader) (*parquet.BloomFilterHeader, error) {
	header := parquet.NewBloomFilterHeader()
	tpf := thrift.NewTCompactProtocolFactoryConf(nil)
	thriftReader := thrift.NewStreamTransportR(r)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, 1024)
	protocol := tpf.GetProtocol(bufferReader)
	if err := header.Read(ctx, protocol); err != nil {
		return nil, fmt.Errorf("read bloom filter header: %w", err)
	}
	return header, nil
}

func bloomAAD(opt ReadOptions, moduleType encryption.ModuleType) []byte {
	return encryption.AAD(opt.AADPrefix, opt.AADFileUnique, moduleType, opt.RowGroupOrdinal, opt.ColumnOrdinal, 0)
}
