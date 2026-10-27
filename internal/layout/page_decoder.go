package layout

import (
	"bytes"
	"fmt"
	"math/bits"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/internal/encoding"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/schema"
)

// PageBatchSize bounds decoded scratch space independently of page value counts.
const PageBatchSize = 1024

type pageDecoder struct {
	repetitions *encoding.IntDecoder
	definitions *encoding.IntDecoder
	values      *valueDecoder
	valueData   *bytes.Reader
	method      parquet.Encoding
	schema      *parquet.SchemaElement
	template    *Table
	left        uint64
	count       uint64
	emitted     uint64
	batchStart  uint64
}

func newLevelDecoder(reader *bytes.Reader, maxLevel int32, count uint64) (*encoding.IntDecoder, error) {
	if maxLevel == 0 {
		return encoding.NewConstantDecoder(0, count), nil
	}
	return encoding.NewIntDecoder(reader, uint64(bits.Len32(uint32(maxLevel))), count, true, true)
}

func (p *Page) initDecoder(buf []byte, sh *schema.SchemaHandler, chunkValues int64) error {
	var count int32
	var method parquet.Encoding
	switch p.Header.GetType() {
	case parquet.PageType_DATA_PAGE:
		if p.Header.DataPageHeader == nil {
			return fmt.Errorf("data page missing DataPageHeader")
		}
		count, method = p.Header.DataPageHeader.NumValues, p.Header.DataPageHeader.Encoding
	case parquet.PageType_DATA_PAGE_V2:
		if p.Header.DataPageHeaderV2 == nil {
			return fmt.Errorf("data page v2 missing DataPageHeaderV2")
		}
		count, method = p.Header.DataPageHeaderV2.NumValues, p.Header.DataPageHeaderV2.Encoding
	default:
		return fmt.Errorf("unsupported page type: %v", p.Header.GetType())
	}
	if count < 0 {
		return fmt.Errorf("negative page value count %d", count)
	}
	if chunkValues > 0 && int64(count) > chunkValues {
		return fmt.Errorf("data page value count %d exceeds column chunk total %d", count, chunkValues)
	}
	name := common.PathToStr(p.Path)
	se := sh.SchemaElements[sh.MapIndex[name]]
	maxRL, _ := sh.MaxRepetitionLevel(p.Path)
	maxDL, _ := sh.MaxDefinitionLevel(p.Path)
	reader := bytes.NewReader(buf)
	rl, err := newLevelDecoder(reader, maxRL, uint64(count))
	if err != nil {
		return fmt.Errorf("read repetition levels: %w", err)
	}
	dl, err := newLevelDecoder(reader, maxDL, uint64(count))
	if err != nil {
		return fmt.Errorf("read definition levels: %w", err)
	}
	nonNulls, err := dl.Count(int64(maxDL))
	if err != nil {
		return fmt.Errorf("read definition levels: %w", err)
	}
	p.decoder = &pageDecoder{
		repetitions: rl, definitions: dl, valueData: reader, method: method, schema: se,
		template: &Table{Path: p.Path, Schema: se, RepetitionType: se.GetRepetitionType(), MaxRepetitionLevel: maxRL, MaxDefinitionLevel: maxDL},
		left:     uint64(count), count: nonNulls,
	}
	return nil
}

// HasMoreValues reports whether this page retains undecoded level/value entries.
func (p *Page) HasMoreValues() bool { return p.decoder != nil && p.decoder.left > 0 }

// ReadBatch decodes the next bounded batch of levels and optionally its values.
func (p *Page) ReadBatch(decodeValues bool) (int64, int64, error) {
	d := p.decoder
	if d == nil {
		return 0, 0, fmt.Errorf("page decoder is not initialized")
	}
	count := min(d.left, PageBatchSize)
	table := NewTableFromTable(d.template)
	table.RepetitionType = d.template.RepetitionType
	table.Values = make([]any, count)
	table.RepetitionLevels = make([]int32, count)
	table.DefinitionLevels = make([]int32, count)
	d.batchStart = d.emitted
	var rows int64
	for i := range count {
		rl, err := d.repetitions.Next()
		if err != nil {
			return 0, 0, fmt.Errorf("read repetition levels: %w", err)
		}
		dl, err := d.definitions.Next()
		if err != nil {
			return 0, 0, fmt.Errorf("read definition levels: %w", err)
		}
		if rl < 0 || rl > int64(table.MaxRepetitionLevel) || dl < 0 || dl > int64(table.MaxDefinitionLevel) {
			return 0, 0, fmt.Errorf("page level out of range")
		}
		table.RepetitionLevels[i], table.DefinitionLevels[i] = int32(rl), int32(dl)
		if rl == 0 {
			rows++
		}
		if dl == int64(table.MaxDefinitionLevel) {
			d.emitted++
		}
	}
	d.left -= count
	p.DataTable = table
	if decodeValues {
		if err := p.decodeBatchValues(); err != nil {
			return 0, 0, err
		}
	}
	return int64(count), rows, nil
}

func (p *Page) decodeBatchValues() error {
	d := p.decoder
	if d == nil {
		return fmt.Errorf("page decoder is not initialized")
	}
	if d.values == nil {
		var err error
		d.values, err = newValueDecoder(d.valueData, d.method, d.schema.GetType(), d.count, uint64(d.schema.GetTypeLength()))
		if err != nil {
			return fmt.Errorf("read data page values: %w", err)
		}
	}
	// Skipped batches retain only their non-null count, not their decoded values.
	if d.values.index > d.batchStart {
		return nil
	}
	if err := d.values.skip(d.batchStart - d.values.index); err != nil {
		return fmt.Errorf("skip data page values: %w", err)
	}
	for i, dl := range p.DataTable.DefinitionLevels {
		if dl != p.DataTable.MaxDefinitionLevel {
			continue
		}
		value, err := d.values.next()
		if err != nil {
			return fmt.Errorf("read data page values: %w", err)
		}
		p.DataTable.Values[i] = value
	}
	return nil
}

// SkipFlatRows skips rows without materializing levels in a non-repeated column.
func (p *Page) SkipFlatRows(rows int64) (int64, error) {
	d := p.decoder
	if d == nil || d.template.MaxRepetitionLevel != 0 || rows <= 0 {
		return 0, nil
	}
	n := min(uint64(rows), d.left)
	nonNulls, err := d.definitions.CountPrefix(int64(d.template.MaxDefinitionLevel), n)
	if err != nil {
		return 0, err
	}
	if err := d.definitions.Skip(n); err != nil {
		return 0, err
	}
	if err := d.repetitions.Skip(n); err != nil {
		return 0, err
	}
	d.emitted += nonNulls
	d.left -= n
	return int64(n), nil
}

// DropRowContinuation discards the tail of a row skipped in a preceding batch.
func (p *Page) DropRowContinuation() bool {
	table := p.DataTable
	n := 0
	for n < len(table.Values) && table.RepetitionLevels[n] != 0 {
		if p.decoder != nil && table.DefinitionLevels[n] == table.MaxDefinitionLevel {
			p.decoder.batchStart++
		}
		n++
	}
	table.Values = table.Values[n:]
	table.RepetitionLevels = table.RepetitionLevels[n:]
	table.DefinitionLevels = table.DefinitionLevels[n:]
	return len(table.Values) == 0
}
