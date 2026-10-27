package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// IntDecoder retains a hybrid or bit-packed stream without expanding its runs.
type IntDecoder struct {
	reader *bytes.Reader
	width  uint64
	left   uint64
	run    uint64
	value  int64
	packed bool
	data   []byte
	bit    uint64
}

// NewConstantDecoder returns an implicit level stream.
func NewConstantDecoder(value int64, count uint64) *IntDecoder {
	return &IntDecoder{left: count, run: count, value: value}
}

// NewIntDecoder opens a bounded hybrid or bit-packed stream.
func NewIntDecoder(reader *bytes.Reader, width, count uint64, hybrid, prefixed bool) (*IntDecoder, error) {
	if err := validateCount(count); err != nil {
		return nil, err
	}
	if width > 64 {
		return nil, fmt.Errorf("bit width %d exceeds 64", width)
	}
	if prefixed {
		var length uint32
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			return nil, err
		}
		if uint64(length) > uint64(reader.Len()) {
			return nil, fmt.Errorf("declared length %d exceeds remaining data %d", length, reader.Len())
		}
		data := make([]byte, int(length))
		// The length was checked against the bytes.Reader above.
		_, _ = reader.Read(data)
		reader = bytes.NewReader(data)
	}
	d := &IntDecoder{reader: reader, width: width, left: count}
	if !hybrid && count > 0 {
		d.run, d.packed = count, true
		if err := d.readPacked(); err != nil {
			return nil, err
		}
	}
	return d, nil
}

func (d *IntDecoder) readPacked() error {
	size := (d.run*d.width + 7) / 8
	if size > uint64(d.reader.Len()) {
		return fmt.Errorf("bit-packed run needs %d bytes but %d remain", size, d.reader.Len())
	}
	d.data = make([]byte, size)
	_, err := io.ReadFull(d.reader, d.data)
	d.bit = 0
	return err
}

func (d *IntDecoder) nextRun() error {
	header, err := binary.ReadUvarint(d.reader)
	if err != nil {
		return err
	}
	d.packed = header&1 != 0
	d.run = header >> 1
	if d.run == 0 {
		return fmt.Errorf("empty hybrid run")
	}
	if d.packed {
		d.value = 0
		if d.run > (d.left+7)/8 {
			d.run = d.left
		} else {
			d.run = min(d.run*8, d.left)
		}
		return d.readPacked()
	}
	d.run = min(d.run, d.left)
	var data [8]byte
	if _, err := io.ReadFull(d.reader, data[:(d.width+7)/8]); err != nil {
		return err
	}
	d.value = int64(binary.LittleEndian.Uint64(data[:]))
	return nil
}

// Next returns one integer from the stream.
func (d *IntDecoder) Next() (int64, error) {
	if d.left == 0 {
		return 0, io.EOF
	}
	if d.run == 0 {
		if err := d.nextRun(); err != nil {
			return 0, err
		}
	}
	value := d.value
	if d.packed {
		var bits uint64
		for used := uint64(0); used < d.width; {
			shift := d.bit % 8
			n := min(8-shift, d.width-used)
			bits |= (uint64(d.data[d.bit/8]) >> shift & ((1 << n) - 1)) << used
			d.bit += n
			used += n
		}
		value = int64(bits)
	}
	d.run--
	d.left--
	return value, nil
}

// Skip advances without allocating or expanding repeated values.
func (d *IntDecoder) Skip(count uint64) error {
	if count > d.left {
		return io.ErrUnexpectedEOF
	}
	for count > 0 {
		if d.run == 0 {
			if err := d.nextRun(); err != nil {
				return err
			}
		}
		n := min(count, d.run)
		if d.packed {
			d.bit += n * d.width
		}
		d.run -= n
		d.left -= n
		count -= n
	}
	return nil
}

// Count counts matching values using a copy of the cursor.
func (d *IntDecoder) Count(value int64) (uint64, error) {
	clone := *d
	if d.reader != nil {
		reader := *d.reader
		clone.reader = &reader
	}
	var count uint64
	for clone.left > 0 {
		if clone.run == 0 {
			if err := clone.nextRun(); err != nil {
				return 0, err
			}
		}
		if !clone.packed || clone.width == 0 {
			if clone.value == value {
				count += clone.run
			}
			clone.left -= clone.run
			clone.run = 0
		} else {
			// The current run is already loaded and has at least one value.
			v, _ := clone.Next()
			if v == value {
				count++
			}
		}
	}
	return count, nil
}

// CountPrefix counts matching values in the next n entries using a copy of the cursor.
func (d *IntDecoder) CountPrefix(value int64, n uint64) (uint64, error) {
	if n > d.left {
		return 0, io.ErrUnexpectedEOF
	}
	clone := *d
	clone.left = n
	clone.run = min(clone.run, n)
	return clone.Count(value)
}
