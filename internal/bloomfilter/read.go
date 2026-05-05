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

// ReadOptions configures encrypted bloom filter reads.
type ReadOptions struct {
	Key             []byte
	AADPrefix       []byte
	AADFileUnique   []byte
	RowGroupOrdinal int16
	ColumnOrdinal   int16
}

// ReadBloomFilter reads a bloom filter from the given ReadSeeker at the specified offset.
// It reads the Thrift-encoded BloomFilterHeader followed by the raw bitset bytes.
func ReadBloomFilter(r io.ReadSeeker, offset int64) (*Filter, error) {
	if _, err := r.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to bloom filter offset %d: %w", offset, err)
	}

	// Read the Thrift-encoded header
	header := parquet.NewBloomFilterHeader()
	tpf := thrift.NewTCompactProtocolFactoryConf(nil)
	thriftReader := thrift.NewStreamTransportR(r)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, 1024)
	protocol := tpf.GetProtocol(bufferReader)
	if err := header.Read(context.TODO(), protocol); err != nil {
		return nil, fmt.Errorf("read bloom filter header: %w", err)
	}

	if header.NumBytes <= 0 {
		return nil, fmt.Errorf("invalid bloom filter header: numBytes=%d", header.NumBytes)
	}

	// Re-serialize the header to determine its exact byte size,
	// then seek to the bitset position (offset + headerSize).
	ts := thrift.NewTSerializer()
	ts.Protocol = thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ts.Transport)
	headerBuf, err := ts.Write(context.TODO(), header)
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

// ReadEncryptedBloomFilter reads an encrypted bloom filter header and bitset
// from the given ReadSeeker at the specified offset.
func ReadEncryptedBloomFilter(r io.ReadSeeker, offset int64, opt ReadOptions) (*Filter, error) {
	if _, err := r.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to bloom filter offset %d: %w", offset, err)
	}

	headerModule, err := encryption.ReadModule(r, 0)
	if err != nil {
		return nil, fmt.Errorf("read encrypted bloom filter header: %w", err)
	}
	headerBytes, err := encryption.DecryptGCM(opt.Key, bloomAAD(opt, encryption.ModuleBloomFilterHeader), headerModule)
	if err != nil {
		return nil, fmt.Errorf("decrypt bloom filter header: %w", err)
	}
	header, err := readBloomFilterHeader(bytes.NewReader(headerBytes))
	if err != nil {
		return nil, err
	}
	if header.NumBytes <= 0 {
		return nil, fmt.Errorf("invalid bloom filter header: numBytes=%d", header.NumBytes)
	}

	bitsetModule, err := encryption.ReadModule(r, 0)
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

func readBloomFilterHeader(r io.Reader) (*parquet.BloomFilterHeader, error) {
	header := parquet.NewBloomFilterHeader()
	tpf := thrift.NewTCompactProtocolFactoryConf(nil)
	thriftReader := thrift.NewStreamTransportR(r)
	bufferReader := thrift.NewTBufferedTransport(thriftReader, 1024)
	protocol := tpf.GetProtocol(bufferReader)
	if err := header.Read(context.TODO(), protocol); err != nil {
		return nil, fmt.Errorf("read bloom filter header: %w", err)
	}
	return header, nil
}

func bloomAAD(opt ReadOptions, moduleType encryption.ModuleType) []byte {
	return encryption.AAD(opt.AADPrefix, opt.AADFileUnique, moduleType, opt.RowGroupOrdinal, opt.ColumnOrdinal, 0)
}
