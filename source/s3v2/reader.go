package s3v2

import (
	"context"
	"fmt"
	"io"
	"math"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hangxie/parquet-go/v3/source"
)

type s3ReadClient interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// Compile time check that *s3File implement the source.ParquetFileReader interface.
var _ source.ParquetFileReader = (*s3Reader)(nil)

type s3Reader struct {
	s3File
	client         s3ReadClient
	readOpened     bool
	fileSize       int64
	offset         int64
	whence         int
	version        *string
	socket         io.ReadCloser
	minRequestSize int64
}

const (
	rangeHeader           = "bytes=%d-%d"
	rangeHeaderSuffix     = "bytes=%d"
	defaultMinRequestSize = math.MaxUint32
)

var (
	errWhence        = fmt.Errorf("invalid whence")
	errInvalidOffset = fmt.Errorf("invalid offset")
)

// NewS3FileReader creates an S3 FileReader, to be used with NewParquetReader
func NewS3FileReader(ctx context.Context, bucket, key string, version *string, cfgs ...*aws.Config) (source.ParquetFileReader, error) {
	r, err := NewS3FileReaderWithParams(ctx, S3FileReaderParams{
		Bucket:  bucket,
		Key:     key,
		Version: version,
	})
	if err != nil {
		return nil, fmt.Errorf("new s3 reader: %w", err)
	}
	return r, nil
}

// NewS3FileReaderWithClient is the same as NewS3FileReader but allows passing
// your own S3 client
func NewS3FileReaderWithClient(ctx context.Context, s3Client s3ReadClient, bucket, key string, version *string) (source.ParquetFileReader, error) {
	r, err := NewS3FileReaderWithParams(ctx, S3FileReaderParams{
		Bucket:   bucket,
		Key:      key,
		Version:  version,
		S3Client: s3Client,
	})
	if err != nil {
		return nil, fmt.Errorf("new s3 reader with client: %w", err)
	}
	return r, nil
}

// Close signals write completion and cleans up any
// open streams. Will block until pending uploads are complete.
func (s *s3Reader) Close() error {
	s.closeSocket()
	return nil
}

// Open creates a new S3 File instance to perform concurrent reads
func (s *s3Reader) Open(name string) (source.ParquetFileReader, error) {
	return s.OpenContext(s.ctx, name)
}

func (s *s3Reader) OpenContext(ctx context.Context, name string) (source.ParquetFileReader, error) {
	s.lock.RLock()
	readOpened := s.readOpened
	s.lock.RUnlock()
	if !readOpened {
		if err := s.openRead(ctx); err != nil {
			return nil, fmt.Errorf("open s3 object: %w", err)
		}
	}

	// ColumBuffer passes in an empty string for name
	if len(name) == 0 {
		name = s.key
	}

	// create a new instance
	pf := &s3Reader{
		s3File: s3File{
			ctx:        ctx,
			bucketName: s.bucketName,
			key:        name,
		},
		client:         s.client,
		version:        s.version,
		readOpened:     s.readOpened,
		fileSize:       s.fileSize,
		minRequestSize: s.minRequestSize,
		offset:         0,
	}
	return pf, nil
}

func (s *s3Reader) Clone() (source.ParquetFileReader, error) {
	return s.CloneContext(s.ctx)
}

func (s *s3Reader) CloneContext(ctx context.Context) (source.ParquetFileReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Create a new instance without making network calls
	// Reuse already-known metadata
	return &s3Reader{
		s3File: s3File{
			ctx:        ctx,
			bucketName: s.bucketName,
			key:        s.key,
		},
		client:         s.client,
		readOpened:     s.readOpened,
		fileSize:       s.fileSize,
		version:        s.version,
		minRequestSize: s.minRequestSize,
		offset:         0,
		whence:         0,
	}, nil
}

// S3FileReaderParams contains fields used to initialize and configure an s3Reader object
// for reading.
type S3FileReaderParams struct {
	Bucket string
	Key    string

	// S3Client will be used to issue requests to S3. If not set, a new one will be
	// created. Optional.
	S3Client s3ReadClient
	// Version is the version of the S3 object that will be read. If not set, the newest
	// version will be read. Optional.
	Version *string
	// MinRequestSize controls the amount of data per request that the S3File will ask for
	// from S3. Optional.
	// A large MinRequestSize should improve performance and reduce AWS costs due to
	// number of request. However, in some cases it may increase AWS costs due to data
	// processing or data transfer. For best results, set it at or above the largest of
	// the footer size and the biggest chunk size in the parquet file.
	// S3File will not buffer a large amount of data in memory at one time, regardless
	// of the value of MinRequestSize.
	MinRequestSize int
}

// configured using the S3FileReaderParams object.
func NewS3FileReaderWithParams(ctx context.Context, params S3FileReaderParams) (source.ParquetFileReader, error) {
	s3Client := params.S3Client
	if s3Client == nil {
		cfg, err := getConfig()
		if err != nil {
			return nil, fmt.Errorf("load AWS config: %w", err)
		}
		s3Client = s3.NewFromConfig(cfg)
	}

	minRequestSize := int64(params.MinRequestSize)
	if minRequestSize == 0 {
		minRequestSize = defaultMinRequestSize
	}

	file := &s3Reader{
		s3File: s3File{
			ctx:        ctx,
			bucketName: params.Bucket,
			key:        params.Key,
		},
		client:         s3Client,
		version:        params.Version,
		minRequestSize: minRequestSize,
	}

	return file.Open(params.Key)
}
