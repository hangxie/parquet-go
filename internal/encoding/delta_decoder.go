package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// DeltaDecoder decodes a delta stream while retaining only the current miniblock.
type DeltaDecoder struct {
	reader    *bytes.Reader
	header    deltaHeader
	left      uint64
	first     bool
	value     int64
	minDelta  int64
	widths    []byte
	miniblock uint64
	bits      *IntDecoder
	miniLeft  uint64
	int32     bool
}

// NewDeltaDecoder opens a delta binary packed stream.
func NewDeltaDecoder(reader *bytes.Reader, maxValues uint64, int32Values bool) (*DeltaDecoder, error) {
	h, err := readDeltaHeader(reader, maxValues)
	if err != nil {
		return nil, err
	}
	if h.numValues == 0 {
		return &DeltaDecoder{reader: reader}, nil
	}
	d := &DeltaDecoder{reader: reader, header: h, left: h.numValues, first: true, int32: int32Values}
	d.value = int64(h.firstValueZigZag>>1) ^ -int64(h.firstValueZigZag&1)
	if d.int32 {
		d.value = int64(int32(d.value))
	}
	return d, nil
}

func (d *DeltaDecoder) nextMiniblock() error {
	if d.miniblock == uint64(len(d.widths)) {
		zigzag, err := binary.ReadUvarint(d.reader)
		if err != nil {
			return err
		}
		d.minDelta = int64(zigzag>>1) ^ -int64(zigzag&1)
		if d.int32 {
			d.minDelta = int64(int32(d.minDelta))
		}
		d.widths, err = readBitWidths(d.reader, d.header.numMiniblocks)
		if err != nil {
			return err
		}
		d.miniblock = 0
	}
	width := uint64(d.widths[d.miniblock])
	if width > 64 {
		return fmt.Errorf("bit width %d exceeds 64", width)
	}
	d.miniblock++
	// Consume full miniblock padding to keep the following stream positioned correctly.
	full := d.header.numValuesInMiniblock * width / 8
	if full > uint64(d.reader.Len()) {
		return fmt.Errorf("miniblock needs %d bytes but %d remain", full, d.reader.Len())
	}
	d.miniLeft = min(d.left, d.header.numValuesInMiniblock)
	if width == 0 {
		d.bits = NewConstantDecoder(0, d.miniLeft)
		return nil
	}
	var err error
	start, _ := d.reader.Seek(0, io.SeekCurrent)
	d.bits, err = NewIntDecoder(d.reader, width, d.miniLeft, false, false)
	if err != nil {
		return err
	}
	_, _ = d.reader.Seek(start+int64(full), io.SeekStart)
	return nil
}

// Next returns one delta-decoded value.
func (d *DeltaDecoder) Next() (int64, error) {
	if d.left == 0 {
		return 0, io.EOF
	}
	if d.first {
		d.first = false
		d.left--
		return d.value, nil
	}
	if d.miniLeft == 0 {
		if err := d.nextMiniblock(); err != nil {
			return 0, err
		}
	}
	delta, err := d.bits.Next()
	if err != nil {
		return 0, err
	}
	d.value += delta + d.minDelta
	if d.int32 {
		d.value = int64(int32(d.value))
	}
	d.left--
	d.miniLeft--
	return d.value, nil
}

// Skip advances the delta cursor, handling zero-width miniblocks without expansion.
func (d *DeltaDecoder) Skip(count uint64) error {
	if count > d.left {
		return io.ErrUnexpectedEOF
	}
	for count > 0 {
		if d.first {
			d.first = false
			d.left--
			count--
			continue
		}
		if d.miniLeft == 0 {
			if err := d.nextMiniblock(); err != nil {
				return err
			}
		}
		if d.bits.width == 0 {
			n := min(count, d.miniLeft)
			d.value += int64(n) * d.minDelta
			if d.int32 {
				d.value = int64(int32(d.value))
			}
			d.miniLeft -= n
			d.left -= n
			count -= n
		} else {
			if _, err := d.Next(); err != nil {
				return err
			}
			count--
		}
	}
	return nil
}

// End locates the end of the encoded stream without expanding any miniblocks.
func (d *DeltaDecoder) End() (int64, error) {
	clone := *d
	reader := *d.reader
	clone.reader = &reader
	if clone.first && clone.left > 0 {
		clone.left--
		clone.first = false
	}
	clone.left -= clone.miniLeft
	clone.miniLeft = 0
	for clone.left > 0 {
		if clone.miniblock == uint64(len(clone.widths)) {
			if _, err := binary.ReadUvarint(clone.reader); err != nil {
				return 0, err
			}
			var err error
			clone.widths, err = readBitWidths(clone.reader, clone.header.numMiniblocks)
			if err != nil {
				return 0, err
			}
			clone.miniblock = 0
		}
		width := uint64(clone.widths[clone.miniblock])
		if width > 64 {
			return 0, fmt.Errorf("bit width %d exceeds 64", width)
		}
		size := clone.header.numValuesInMiniblock * width / 8
		if size > uint64(clone.reader.Len()) {
			return 0, io.ErrUnexpectedEOF
		}
		_, _ = clone.reader.Seek(int64(size), io.SeekCurrent)
		clone.miniblock++
		clone.left -= min(clone.left, clone.header.numValuesInMiniblock)
	}
	return clone.reader.Seek(0, io.SeekCurrent)
}
