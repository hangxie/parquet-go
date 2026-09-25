package s3v2

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Seek tracks the offset for the next Read. Has no effect on Write.
func (s *s3Reader) Seek(offset int64, whence int) (int64, error) {
	if whence < io.SeekStart || whence > io.SeekEnd {
		return 0, errWhence
	}

	if s.fileSize > 0 {
		switch whence {
		case io.SeekStart:
			if offset < 0 || offset > s.fileSize {
				return 0, errInvalidOffset
			}
		case io.SeekCurrent:
			offset += s.offset
			if offset < 0 || offset > s.fileSize {
				return 0, errInvalidOffset
			}
		case io.SeekEnd:
			if offset == 0 {
				offset = s.fileSize
			} else if offset > 0 || -offset > s.fileSize {
				return 0, errInvalidOffset
			}
		}
	}

	s.offset = offset
	s.whence = whence

	s.closeSocket()
	return s.offset, nil
}

// Read up to len(p) bytes into p and return the number of bytes read
func (s *s3Reader) Read(p []byte) (n int, err error) {
	return s.ReadContext(s.ctx, p)
}

func (s *s3Reader) ReadContext(ctx context.Context, p []byte) (n int, err error) {
	if s.fileSize > 0 && s.offset >= s.fileSize {
		return 0, io.EOF
	}

	defer func() {
		if err != nil {
			s.closeSocket()
		}
	}()

	if s.socket == nil {
		err = s.openSocket(ctx, int64(len(p)))
		if err != nil {
			return 0, err
		}
	}
	n, err = s.socket.Read(p)
	// Because the chunk size is not infinite, we might hit the end of the socket while
	// there's still data in the file. In this case, we close the socket so that the next
	// read call will request a new one, and we return a nil error so that the caller
	// will not think the file is done.
	if err == io.EOF {
		err = nil
		s.closeSocket()
	}
	s.offset += int64(n)

	return n, err
}

// openSocket issues a new GetObject request to retrieve the next chunk of data from the
// object.
func (s *s3Reader) openSocket(ctx context.Context, numBytes int64) error {
	if numBytes < s.minRequestSize {
		numBytes = s.minRequestSize
	}
	getObjRange := s.getBytesRange(numBytes)
	getObj := &s3.GetObjectInput{
		Bucket:    aws.String(s.bucketName),
		Key:       aws.String(s.key),
		VersionId: s.version,
	}
	if len(getObjRange) > 0 {
		getObj.Range = aws.String(getObjRange)
	}

	out, err := s.client.GetObject(ctx, getObj)
	if err != nil {
		return fmt.Errorf("get S3 object: %w", err)
	}
	s.socket = out.Body
	return nil
}

func (s *s3Reader) closeSocket() {
	if s.socket != nil {
		_ = s.socket.Close()
		s.socket = nil
	}
}

// openRead verifies the requested file is accessible and
// tracks the file size
func (s *s3Reader) openRead(ctx context.Context) error {
	hoi := &s3.HeadObjectInput{
		Bucket:    aws.String(s.bucketName),
		Key:       aws.String(s.key),
		VersionId: s.version,
	}

	hoo, err := s.client.HeadObject(ctx, hoi)
	if err != nil {
		return fmt.Errorf("head object: %w", err)
	}

	s.lock.Lock()
	s.readOpened = true
	if hoo.ContentLength != nil && *hoo.ContentLength != 0 {
		s.fileSize = *hoo.ContentLength
	}
	s.lock.Unlock()

	return nil
}

// getBytesRange returns the range request header string
func (s *s3Reader) getBytesRange(numBytes int64) string {
	var (
		byteRange string
		begin     int64
		end       int64
	)

	// Processing for unknown file size relies on the requester to
	// know which ranges are valid. May occur if caller is missing HEAD permissions.
	if s.fileSize < 1 {
		switch s.whence {
		case io.SeekStart, io.SeekCurrent:
			byteRange = fmt.Sprintf(rangeHeader, s.offset, s.offset+int64(numBytes)-1)
		case io.SeekEnd:
			byteRange = fmt.Sprintf(rangeHeaderSuffix, s.offset)
		}
		return byteRange
	}

	switch s.whence {
	case io.SeekStart, io.SeekCurrent:
		begin = s.offset
	case io.SeekEnd:
		begin = s.fileSize + s.offset
	default:
		return byteRange
	}

	endIndex := s.fileSize - 1
	if begin < 0 {
		begin = 0
	}
	end = begin + int64(numBytes) - 1
	if end > endIndex {
		end = endIndex
	}

	byteRange = fmt.Sprintf(rangeHeader, begin, end)
	return byteRange
}
