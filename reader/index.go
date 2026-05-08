package reader

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/source"
)

// ReadColumnIndex reads the column index for a row-group column. It returns nil
// when the column chunk does not have a column index.
func (pr *ParquetReader) ReadColumnIndex(rowGroupIndex, columnIndex int) (*parquet.ColumnIndex, error) {
	column, rowGroupOrdinal, columnOrdinal, err := pr.indexColumn(rowGroupIndex, columnIndex)
	if err != nil {
		return nil, err
	}
	if !column.IsSetColumnIndexOffset() || !column.IsSetColumnIndexLength() {
		return nil, nil
	}

	buf, err := pr.readIndexModule(column, rowGroupOrdinal, columnOrdinal, column.GetColumnIndexOffset(), column.GetColumnIndexLength(), encryption.ModuleColumnIndex)
	if err != nil {
		return nil, err
	}
	index := parquet.NewColumnIndex()
	if err := readCompactThrift(buf, index); err != nil {
		return nil, fmt.Errorf("read column index: %w", err)
	}
	return index, nil
}

// ReadOffsetIndex reads the offset index for a row-group column. It returns nil
// when the column chunk does not have an offset index.
func (pr *ParquetReader) ReadOffsetIndex(rowGroupIndex, columnIndex int) (*parquet.OffsetIndex, error) {
	column, rowGroupOrdinal, columnOrdinal, err := pr.indexColumn(rowGroupIndex, columnIndex)
	if err != nil {
		return nil, err
	}
	if !column.IsSetOffsetIndexOffset() || !column.IsSetOffsetIndexLength() {
		return nil, nil
	}

	buf, err := pr.readIndexModule(column, rowGroupOrdinal, columnOrdinal, column.GetOffsetIndexOffset(), column.GetOffsetIndexLength(), encryption.ModuleOffsetIndex)
	if err != nil {
		return nil, err
	}
	index := parquet.NewOffsetIndex()
	if err := readCompactThrift(buf, index); err != nil {
		return nil, fmt.Errorf("read offset index: %w", err)
	}
	return index, nil
}

func (pr *ParquetReader) indexColumn(rowGroupIndex, columnIndex int) (*parquet.ColumnChunk, int16, int16, error) {
	if pr == nil || pr.Footer == nil {
		return nil, 0, 0, fmt.Errorf("reader footer is nil")
	}
	if rowGroupIndex < 0 || rowGroupIndex >= len(pr.Footer.RowGroups) {
		return nil, 0, 0, fmt.Errorf("invalid row group index: %d (valid range: 0-%d)", rowGroupIndex, len(pr.Footer.RowGroups)-1)
	}
	rowGroup := pr.Footer.RowGroups[rowGroupIndex]
	if rowGroup == nil {
		return nil, 0, 0, fmt.Errorf("row group %d is nil", rowGroupIndex)
	}
	if columnIndex < 0 || columnIndex >= len(rowGroup.Columns) {
		return nil, 0, 0, fmt.Errorf("invalid column index: %d (valid range: 0-%d)", columnIndex, len(rowGroup.Columns)-1)
	}

	rowGroupOrdinal := int16(rowGroupIndex)
	if rowGroup.IsSetOrdinal() {
		rowGroupOrdinal = rowGroup.GetOrdinal()
	}
	return rowGroup.Columns[columnIndex], rowGroupOrdinal, int16(columnIndex), nil
}

func (pr *ParquetReader) readIndexModule(column *parquet.ColumnChunk, rowGroupOrdinal, columnOrdinal int16, offset int64, length int32, moduleType encryption.ModuleType) ([]byte, error) {
	if pr.PFile == nil {
		return nil, fmt.Errorf("file reader is nil")
	}
	pf, err := pr.PFile.Clone()
	if err != nil {
		return nil, fmt.Errorf("clone file reader: %w", err)
	}
	defer func() { _ = pf.Close() }()

	if column.GetCryptoMetadata() == nil {
		return readPlainIndexModule(pf, offset, length)
	}

	algorithm := pr.encryptionAlgorithm()
	if algorithm == nil {
		return nil, fmt.Errorf("encrypted index missing file encryption algorithm")
	}
	aadPrefix, aadFileUnique, err := pr.footerAADParts(algorithm)
	if err != nil {
		return nil, err
	}
	key, err := pr.resolveColumnKey(column)
	if err != nil {
		return nil, err
	}
	if _, err := pf.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to index offset %d: %w", offset, err)
	}
	module, err := encryption.ReadModule(pf, int64(length))
	if err != nil {
		return nil, fmt.Errorf("read encrypted index module: %w", err)
	}
	aad := encryption.AAD(aadPrefix, aadFileUnique, moduleType, rowGroupOrdinal, columnOrdinal, 0)
	plain, err := encryption.DecryptGCM(key, aad, module)
	if err != nil {
		return nil, fmt.Errorf("decrypt index module: %w", err)
	}
	return plain, nil
}

func readPlainIndexModule(pf source.ParquetFileReader, offset int64, length int32) ([]byte, error) {
	if length < 0 {
		return nil, fmt.Errorf("negative index length: %d", length)
	}
	if _, err := pf.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to index offset %d: %w", offset, err)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(pf, buf); err != nil {
		return nil, fmt.Errorf("read index module: %w", err)
	}
	return buf, nil
}

type thriftReadable interface {
	Read(context.Context, thrift.TProtocol) error
}

func readCompactThrift(buf []byte, v thriftReadable) error {
	protocol := thrift.NewTCompactProtocolConf(thrift.NewStreamTransportR(bytes.NewReader(buf)), &thrift.TConfiguration{})
	return v.Read(context.Background(), protocol)
}
