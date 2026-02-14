package bloomfilter

import (
	"context"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v2/parquet"
)

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
