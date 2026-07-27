package gcs

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/bobg/gcsobj"

	"github.com/hangxie/parquet-go/v3/source"
)

// Compile time check that *gcsFile implement the source.ParquetFileReader interface.
var _ source.ParquetFileReader = (*gcsReader)(nil)

type gcsReader struct {
	gcsFile
	generation int64
	size       int64
	offset     int64
	gcsReader  *gcsobj.Reader
}

// NewGcsFileReader will create a new GCS file reader for the specific generation.
func NewGcsFileReader(ctx context.Context, projectID, bucketName, name string, generation int64) (*gcsReader, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create storage client: %w", err)
	}

	r, err := NewGcsFileReaderWithClient(ctx, client, projectID, bucketName, name, generation)
	if err != nil {
		return nil, fmt.Errorf("new gcs reader with client: %w", err)
	}

	// Set externalClient to false so we close it when calling `Close`.
	r.externalClient = false

	return r, nil
}

// NewGcsFileReaderWithClient will create a new GCS file reader with the passed client for the specific generation.
func NewGcsFileReaderWithClient(ctx context.Context, client *storage.Client, projectID, bucketName, name string, generation int64) (*gcsReader, error) {
	obj := client.Bucket(bucketName).Object(name).Generation(generation)

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, fmt.Errorf("create new reader: %w", err)
	}
	reader := gcsobj.NewReaderWithSize(ctx, obj, attrs.Size)

	return &gcsReader{
		gcsFile: gcsFile{
			projectID:      projectID,
			bucketName:     bucketName,
			filePath:       name,
			gcsClient:      client,
			object:         obj,
			ctx:            ctx,
			externalClient: true,
		},
		gcsReader:  reader,
		generation: generation,
		size:       attrs.Size,
	}, nil
}

// Open will create a new GCS file reader and open the object named as the
// passed named. If name is left empty the same object as currently opened
// will be re-opened.
func (g *gcsReader) Open(name string) (source.ParquetFileReader, error) {
	return g.OpenContext(g.ctx, name)
}

func (g *gcsReader) OpenContext(ctx context.Context, name string) (source.ParquetFileReader, error) {
	if g.gcsClient == nil {
		r, err := NewGcsFileReader(ctx, g.projectID, g.bucketName, name, -1)
		if err != nil {
			return nil, fmt.Errorf("open gcs reader: %w", err)
		}
		return r, nil
	}
	r, err := NewGcsFileReaderWithClient(ctx, g.gcsClient, g.projectID, g.bucketName, name, -1)
	if err != nil {
		return nil, fmt.Errorf("open gcs reader with client: %w", err)
	}
	return r, nil
}

// Clone will make a copy of reader
func (g gcsReader) Clone() (source.ParquetFileReader, error) {
	return g.CloneContext(g.ctx)
}

func (g gcsReader) CloneContext(ctx context.Context) (source.ParquetFileReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("create new reader: %w", err)
	}
	reader := gcsobj.NewReaderWithSize(ctx, g.object, g.size)

	return &gcsReader{
		gcsFile: gcsFile{
			projectID:      g.projectID,
			bucketName:     g.bucketName,
			filePath:       g.filePath,
			gcsClient:      g.gcsClient,
			object:         g.object,
			ctx:            ctx,
			externalClient: g.externalClient,
		},
		gcsReader:  reader,
		generation: g.generation,
		size:       g.size,
	}, nil
}

// Seek implements io.Seeker.
func (g *gcsReader) Seek(offset int64, whence int) (int64, error) {
	var position int64
	switch whence {
	case io.SeekStart:
		position = offset
	case io.SeekCurrent:
		position = g.offset + offset
	case io.SeekEnd:
		position = g.size + offset
	default:
		return 0, fmt.Errorf("illegal whence value %d", whence)
	}
	position, err := g.gcsReader.Seek(position, io.SeekStart)
	if err == nil {
		g.offset = position
	}
	return position, err
}

// Read implements io.Reader.
func (g *gcsReader) Read(b []byte) (int, error) {
	n, err := g.gcsReader.Read(b)
	g.offset += int64(n)
	return n, err
}

func (g *gcsReader) ReadContext(ctx context.Context, b []byte) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if ctx != g.ctx {
		reader := gcsobj.NewReaderWithSize(ctx, g.object, g.size)
		if _, err := reader.Seek(g.offset, io.SeekStart); err != nil {
			return 0, fmt.Errorf("restore reader offset: %w", err)
		}
		_ = g.gcsReader.Close()
		g.gcsReader = reader
		g.ctx = ctx
	}
	return g.Read(b)
}

// Close implements io.Closer.
func (g *gcsReader) Close() error {
	if !g.externalClient && g.gcsClient != nil {
		if err := g.gcsClient.Close(); err != nil {
			return fmt.Errorf("close GCS client: %w", err)
		}

		g.gcsClient = nil
	}

	if g.gcsReader != nil {
		return g.gcsReader.Close()
	}
	return nil
}
