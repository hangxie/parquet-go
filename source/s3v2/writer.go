package s3v2

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/hangxie/parquet-go/v2/source"
)

// Compile time check that *s3File implement the source.ParquetFileWriter interface.
var _ source.ParquetFileWriter = (*s3Writer)(nil)

type s3Writer struct {
	s3File
	client             transfermanager.S3APIClient
	writeOpened        bool
	writeDone          chan error
	pipeReader         *io.PipeReader
	pipeWriter         *io.PipeWriter
	tmOptions          []func(*transfermanager.Options)
	uploadInputOptions []func(*transfermanager.UploadObjectInput)
}

// NewS3FileWriter creates an S3 FileWriter, to be used with NewParquetWriter
func NewS3FileWriter(ctx context.Context, bucket, key string, tmOptions []func(*transfermanager.Options)) (source.ParquetFileWriter, error) {
	cfg, err := getConfig()
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	w, err := NewS3FileWriterWithClient(
		ctx,
		s3.NewFromConfig(cfg),
		bucket,
		key,
		tmOptions,
	)
	if err != nil {
		return nil, fmt.Errorf("new s3 writer: %w", err)
	}
	return w, nil
}

// NewS3FileWriterWithClient is the same as NewS3FileWriter but allows passing
// your own S3 client.
func NewS3FileWriterWithClient(ctx context.Context, s3Client transfermanager.S3APIClient, bucket, key string, tmOptions []func(*transfermanager.Options), uploadInputOptions ...func(*transfermanager.UploadObjectInput)) (source.ParquetFileWriter, error) {
	file := &s3Writer{
		s3File: s3File{
			ctx:        ctx,
			bucketName: bucket,
			key:        key,
		},
		client:             s3Client,
		writeDone:          make(chan error),
		tmOptions:          tmOptions,
		uploadInputOptions: uploadInputOptions,
	}

	pf, err := file.Create(key)
	if err != nil {
		return nil, fmt.Errorf("create s3 writer: %w", err)
	}
	return pf, nil
}

// Write len(p) bytes from p to the S3 data stream
func (s *s3Writer) Write(p []byte) (n int, err error) {
	s.lock.RLock()
	writeOpened := s.writeOpened
	s.lock.RUnlock()
	if !writeOpened {
		s.openWrite()
	}

	s.lock.RLock()
	writeError := s.err
	s.lock.RUnlock()
	if writeError != nil {
		return 0, writeError
	}

	// prevent further writes upon error
	bytesWritten, writeError := s.pipeWriter.Write(p)
	if writeError != nil {
		s.lock.Lock()
		s.err = writeError
		s.lock.Unlock()

		s.pipeWriter.CloseWithError(err)
		return 0, writeError
	}

	return bytesWritten, nil
}

// Close signals write completion and cleans up any
// open streams. Will block until pending uploads are complete.
func (s *s3Writer) Close() error {
	if s.pipeWriter != nil {
		if err := s.pipeWriter.Close(); err != nil {
			return fmt.Errorf("close S3 pipe writer: %w", err)
		}
	}

	// wait for pending uploads
	if s.writeDone == nil {
		return nil
	}

	if err := <-s.writeDone; err != nil {
		return fmt.Errorf("S3 upload: %w", err)
	}
	return nil
}

// Create creates a new S3 File instance to perform writes
func (s *s3Writer) Create(key string) (source.ParquetFileWriter, error) {
	pf := &s3Writer{
		s3File: s3File{
			ctx:        s.ctx,
			bucketName: s.bucketName,
			key:        key,
		},
		client:             s.client,
		tmOptions:          s.tmOptions,
		uploadInputOptions: s.uploadInputOptions,
		writeDone:          make(chan error),
	}
	pf.openWrite()
	return pf, nil
}

// openWrite creates an S3 transfer manager that consumes the Reader end of an io.Pipe.
// Calling Close signals write completion.
func (s *s3Writer) openWrite() {
	pr, pw := io.Pipe()
	tm := transfermanager.New(s.client, s.tmOptions...)
	s.lock.Lock()
	s.pipeReader = pr
	s.pipeWriter = pw
	s.writeOpened = true
	s.lock.Unlock()

	uploadParams := &transfermanager.UploadObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(s.key),
		Body:   s.pipeReader,
	}

	for _, f := range s.uploadInputOptions {
		f(uploadParams)
	}

	go func(tm *transfermanager.Client, params *transfermanager.UploadObjectInput, done chan error) {
		defer close(done)

		// upload data and signal done when complete
		_, err := tm.UploadObject(s.ctx, params)
		if err != nil {
			s.lock.Lock()
			s.err = err
			s.lock.Unlock()

			if s.writeOpened {
				s.pipeWriter.CloseWithError(err)
			}
		}

		done <- err
	}(tm, uploadParams, s.writeDone)
}
