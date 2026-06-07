package writer

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

// mixedRecord schema: id (plaintext), name (column-key), score (plaintext).
// Bloom filter on name exercises the encrypted-bloom-filter path; the
// other columns exercise the plaintext bloom-filter path.
type mixedRecord struct {
	ID    int64   `parquet:"name=id, type=INT64"`
	Name  string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, bloomfilter=true"`
	Score float64 `parquet:"name=score, type=DOUBLE"`
}

type plaintextDictionaryRecord struct {
	ID       int64  `parquet:"name=id, type=INT64"`
	Category string `parquet:"name=category, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Secret   string `parquet:"name=secret, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func writeMixedFile(t *testing.T, opts ...WriterOption) []byte {
	t.Helper()
	var buf bytes.Buffer
	base := []WriterOption{
		WithNP(1),
		WithRowGroupSize(128),
		WithPageSize(64),
		WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		WithAADPrefix([]byte("mixed-test")),
		WithAADFileUnique([]byte("mixed-001")),
	}
	pw, err := NewParquetWriter(
		writerfile.NewWriterFile(&buf),
		new(mixedRecord),
		append(base, opts...)...,
	)
	require.NoError(t, err)
	require.NoError(t, pw.Write(mixedRecord{ID: 1, Name: "alpha", Score: 1.1}))
	require.NoError(t, pw.Write(mixedRecord{ID: 2, Name: "beta", Score: 2.2}))
	require.NoError(t, pw.Write(mixedRecord{ID: 3, Name: "gamma", Score: 3.3}))
	require.NoError(t, pw.WriteStop())
	return buf.Bytes()
}

func writePlaintextDictionaryFile(t *testing.T, opts ...WriterOption) []byte {
	t.Helper()
	var buf bytes.Buffer
	base := []WriterOption{
		WithNP(1),
		WithRowGroupSize(128),
		WithPageSize(64),
		WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		WithAADPrefix([]byte("plain-dict-test")),
		WithAADFileUnique([]byte("plain-dict-001")),
	}
	pw, err := NewParquetWriter(
		writerfile.NewWriterFile(&buf),
		new(plaintextDictionaryRecord),
		append(base, opts...)...,
	)
	require.NoError(t, err)
	for i, category := range []string{"red", "blue", "red", "green", "blue", "red"} {
		require.NoError(t, pw.Write(plaintextDictionaryRecord{
			ID:       int64(i + 1),
			Category: category,
			Secret:   []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}[i],
		}))
	}
	require.NoError(t, pw.WriteStop())
	return buf.Bytes()
}

func readCompactStructFrom(data []byte, offset int64, out thrift.TStruct) (int, error) {
	if offset < 0 || offset > int64(len(data)) {
		return 0, fmt.Errorf("offset %d outside buffer of size %d", offset, len(data))
	}
	buf := data[offset:]
	mem := thrift.NewTMemoryBufferLen(len(buf))
	if _, err := mem.Write(buf); err != nil {
		return 0, err
	}
	protocol := thrift.NewTCompactProtocolConf(mem, &thrift.TConfiguration{})
	if err := out.Read(context.Background(), protocol); err != nil {
		return 0, err
	}
	return len(buf) - int(mem.RemainingBytes()), nil
}

func rawPageHeaderAt(t *testing.T, data []byte, offset int64) (*parquet.PageHeader, int) {
	t.Helper()
	header := parquet.NewPageHeader()
	headerLen, err := readCompactStructFrom(data, offset, header)
	require.NoErrorf(t, err, "could not parse raw page header at offset %d", offset)
	require.Positive(t, headerLen)
	require.Positive(t, header.GetCompressedPageSize())
	bodyEnd := offset + int64(headerLen) + int64(header.GetCompressedPageSize())
	require.LessOrEqual(t, bodyEnd, int64(len(data)))
	return header, headerLen
}

func readPlaintextFooter(t *testing.T, data []byte) *parquet.FileMetaData {
	t.Helper()
	require.GreaterOrEqual(t, len(data), 12)
	require.Equal(t, common.MagicBytes, string(data[:4]))
	require.Equal(t, common.MagicBytes, string(data[len(data)-4:]))
	footerLen := int(binary.LittleEndian.Uint32(data[len(data)-8 : len(data)-4]))
	require.GreaterOrEqual(t, footerLen, 28)
	footerStart := len(data) - 8 - footerLen
	require.GreaterOrEqual(t, footerStart, 4)
	section := data[footerStart : len(data)-8]
	footerBytes := section[:len(section)-28]
	footer := parquet.NewFileMetaData()
	_, err := readCompactStructFrom(footerBytes, 0, footer)
	require.NoError(t, err)
	return footer
}

// TestMixedEncryptedFooterRoundTrip writes a mixed file with one
// column-key column and two plaintext columns, then reads it back with
// both keys and asserts every row.
func TestMixedEncryptedFooterRoundTrip(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")

	data := writeMixedFile(
		t,
		WithFooterKey(footerKey),
		WithColumnEncrypted("name", ColumnKey(nameKey)),
	)

	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(mixedRecord),
		reader.WithFooterKey(footerKey),
		reader.WithColumnKey("name", nameKey),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	rows := make([]mixedRecord, 3)
	require.NoError(t, pr.Read(&rows))
	require.Equal(t, []mixedRecord{
		{ID: 1, Name: "alpha", Score: 1.1},
		{ID: 2, Name: "beta", Score: 2.2},
		{ID: 3, Name: "gamma", Score: 3.3},
	}, rows)
}

// TestMixedEncryptedFooterMetadata verifies plaintext columns omit
// CryptoMetadata and EncryptedColumnMetadata, while the column-key
// column carries ENCRYPTION_WITH_COLUMN_KEY.
func TestMixedEncryptedFooterMetadata(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")

	data := writeMixedFile(
		t,
		WithFooterKey(footerKey),
		WithColumnEncrypted("name", ColumnKey(nameKey)),
	)

	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(mixedRecord),
		reader.WithFooterKey(footerKey),
		reader.WithColumnKey("name", nameKey),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	require.NotEmpty(t, pr.Footer.RowGroups)
	rg := pr.Footer.RowGroups[0]
	require.Len(t, rg.Columns, 3)

	cols := make(map[string]*parquet.ColumnChunk, 3)
	for _, c := range rg.Columns {
		require.NotNil(t, c.MetaData)
		require.NotEmpty(t, c.MetaData.PathInSchema)
		leaf := c.MetaData.PathInSchema[len(c.MetaData.PathInSchema)-1]
		cols[leaf] = c
	}

	require.Nil(t, cols["ID"].CryptoMetadata)
	require.Empty(t, cols["ID"].EncryptedColumnMetadata)
	require.Nil(t, cols["Score"].CryptoMetadata)
	require.Empty(t, cols["Score"].EncryptedColumnMetadata)

	require.NotNil(t, cols["Name"].CryptoMetadata)
	require.True(t, cols["Name"].CryptoMetadata.IsSetENCRYPTION_WITH_COLUMN_KEY())
}

// TestMixedEncryptedFooterPartialRead verifies that reading without the
// column key still allows access to plaintext columns via path-based
// projection. Full-row reads that touch the encrypted column must error.
func TestMixedEncryptedFooterPartialRead(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")

	data := writeMixedFile(
		t,
		WithFooterKey(footerKey),
		WithColumnEncrypted("name", ColumnKey(nameKey)),
	)

	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(mixedRecord),
		reader.WithFooterKey(footerKey),
	)
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

	idValues, _, _, err := pr.ReadColumnByPath(common.ReformPathStr("parquet_go_root.id"), 3)
	require.NoError(t, err)
	require.Equal(t, []any{int64(1), int64(2), int64(3)}, idValues)

	require.NoError(t, pr.Reset())

	scoreValues, _, _, err := pr.ReadColumnByPath(common.ReformPathStr("parquet_go_root.score"), 3)
	require.NoError(t, err)
	require.Equal(t, []any{1.1, 2.2, 3.3}, scoreValues)

	require.NoError(t, pr.Reset())

	_, _, _, err = pr.ReadColumnByPath(common.ReformPathStr("parquet_go_root.name"), 3)
	require.Error(t, err)
	require.ErrorContains(t, err, "decryption key required for column")
}

// TestMixedAllColumnsPlaintextWithEncryptedFooter verifies that mixed
// mode with a footer key and no ColumnKeys is spec-valid: the footer
// encrypts itself but every column is plaintext.
func TestMixedAllColumnsPlaintextWithEncryptedFooter(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")

	data := writeMixedFile(
		t,
		WithFooterKey(footerKey),
	)

	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(mixedRecord),
		reader.WithFooterKey(footerKey),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	rows := make([]mixedRecord, 3)
	require.NoError(t, pr.Read(&rows))
	require.Equal(t, []mixedRecord{
		{ID: 1, Name: "alpha", Score: 1.1},
		{ID: 2, Name: "beta", Score: 2.2},
		{ID: 3, Name: "gamma", Score: 3.3},
	}, rows)

	rg := pr.Footer.RowGroups[0]
	for _, c := range rg.Columns {
		require.Nilf(t, c.CryptoMetadata, "column %v should be plaintext", c.MetaData.PathInSchema)
	}

	_, err = reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(mixedRecord),
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "decryption key required for footer")
}

// TestMixedFooterKeyColumn verifies that
// WithColumnEncrypted(path, ColumnFooterKey()) marks one column as
// encrypted with the footer key while sibling unkeyed columns are
// plaintext.
func TestMixedFooterKeyColumn(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")

	data := writeMixedFile(
		t,
		WithFooterKey(footerKey),
		WithColumnEncrypted("name", ColumnFooterKey()),
	)

	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(mixedRecord),
		reader.WithFooterKey(footerKey),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	rows := make([]mixedRecord, 3)
	require.NoError(t, pr.Read(&rows))
	require.Equal(t, []mixedRecord{
		{ID: 1, Name: "alpha", Score: 1.1},
		{ID: 2, Name: "beta", Score: 2.2},
		{ID: 3, Name: "gamma", Score: 3.3},
	}, rows)

	rg := pr.Footer.RowGroups[0]
	cols := make(map[string]*parquet.ColumnChunk, len(rg.Columns))
	for _, c := range rg.Columns {
		cols[c.MetaData.PathInSchema[len(c.MetaData.PathInSchema)-1]] = c
	}
	require.NotNil(t, cols["Name"].CryptoMetadata)
	require.True(t, cols["Name"].CryptoMetadata.IsSetENCRYPTION_WITH_FOOTER_KEY())
	require.Nil(t, cols["ID"].CryptoMetadata)
	require.Nil(t, cols["Score"].CryptoMetadata)
}

// TestMixedPlaintextFooterRoundTrip covers a plaintext footer with
// mixed columns. A reader with the footer key reads successfully.
func TestMixedPlaintextFooterRoundTrip(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")

	data := writeMixedFile(
		t,
		WithFooterKey(footerKey),
		WithPlaintextFooter(true),
		WithColumnEncrypted("name", ColumnKey(nameKey)),
	)

	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(mixedRecord),
		reader.WithFooterKey(footerKey),
		reader.WithColumnKey("name", nameKey),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	rows := make([]mixedRecord, 3)
	require.NoError(t, pr.Read(&rows))
	require.Equal(t, []mixedRecord{
		{ID: 1, Name: "alpha", Score: 1.1},
		{ID: 2, Name: "beta", Score: 2.2},
		{ID: 3, Name: "gamma", Score: 3.3},
	}, rows)
}

// TestMixedPlaintextFooterMetadataStripping verifies that plaintext
// columns keep their statistics, while encrypted columns under a
// plaintext footer have statistics stripped from plaintext ColumnMetaData.
func TestMixedPlaintextFooterMetadataStripping(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")

	data := writeMixedFile(
		t,
		WithFooterKey(footerKey),
		WithPlaintextFooter(true),
		WithColumnEncrypted("name", ColumnKey(nameKey)),
	)

	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(mixedRecord),
		reader.WithFooterKey(footerKey),
		reader.WithColumnKey("name", nameKey),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	rg := pr.Footer.RowGroups[0]
	cols := make(map[string]*parquet.ColumnChunk, len(rg.Columns))
	for _, c := range rg.Columns {
		cols[c.MetaData.PathInSchema[len(c.MetaData.PathInSchema)-1]] = c
	}

	// Plaintext columns keep their statistics.
	require.NotNil(t, cols["ID"].MetaData.Statistics)
	require.NotNil(t, cols["Score"].MetaData.Statistics)

	// The encrypted column-key column had its statistics stripped from
	// the plaintext copy. The reader rehydrates them from
	// EncryptedColumnMetadata when the column key is available, so we
	// inspect the raw footer instead (parse a fresh copy).
	prRaw, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(mixedRecord),
		reader.WithFooterKey(footerKey),
	)
	require.NoError(t, err)
	defer func() { _ = prRaw.ReadStop() }()
	rgRaw := prRaw.Footer.RowGroups[0]
	for _, c := range rgRaw.Columns {
		if !strings.EqualFold(c.MetaData.PathInSchema[len(c.MetaData.PathInSchema)-1], "name") {
			continue
		}
		require.NotEmpty(t, c.EncryptedColumnMetadata,
			"encrypted column-key column under plaintext footer must store EncryptedColumnMetadata")
	}

	rawFooter := readPlaintextFooter(t, data)
	var rawID, rawName, rawScore *parquet.ColumnChunk
	for _, c := range rawFooter.RowGroups[0].Columns {
		leaf := c.MetaData.PathInSchema[len(c.MetaData.PathInSchema)-1]
		switch {
		case strings.EqualFold(leaf, "id"):
			rawID = c
		case strings.EqualFold(leaf, "name"):
			rawName = c
		case strings.EqualFold(leaf, "score"):
			rawScore = c
		}
	}
	require.NotNil(t, rawID)
	require.NotNil(t, rawName)
	require.NotNil(t, rawScore)
	require.NotNil(t, rawID.MetaData.Statistics)
	require.NotNil(t, rawScore.MetaData.Statistics)
	require.Nil(t, rawName.MetaData.Statistics)
	require.Nil(t, rawName.MetaData.SizeStatistics)
	require.Nil(t, rawName.MetaData.GeospatialStatistics)
}

// TestMixedPlaintextPageRawVerification parses raw bytes at plaintext
// columns' DataPageOffset and DictionaryPageOffset and validates the
// encoded body lengths. It also checks that the encrypted sibling page
// does not parse as a plaintext page header.
func TestMixedPlaintextPageRawVerification(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	secretKey := []byte("abcdef0123456789")

	data := writePlaintextDictionaryFile(
		t,
		WithFooterKey(footerKey),
		WithColumnEncrypted("secret", ColumnKey(secretKey)),
	)

	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(plaintextDictionaryRecord),
		reader.WithFooterKey(footerKey),
		reader.WithColumnKey("secret", secretKey),
	)
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

	rg := pr.Footer.RowGroups[0]
	cols := make(map[string]*parquet.ColumnChunk, len(rg.Columns))
	for _, c := range rg.Columns {
		cols[c.MetaData.PathInSchema[len(c.MetaData.PathInSchema)-1]] = c
	}

	categoryCol := cols["Category"]
	require.Nil(t, categoryCol.CryptoMetadata)
	require.NotNil(t, categoryCol.MetaData)
	require.NotNil(t, categoryCol.MetaData.DictionaryPageOffset)

	columnStart := *categoryCol.MetaData.DictionaryPageOffset
	columnEnd := columnStart + categoryCol.MetaData.TotalCompressedSize

	dictHeader, dictHeaderLen := rawPageHeaderAt(t, data, *categoryCol.MetaData.DictionaryPageOffset)
	require.Equal(t, parquet.PageType_DICTIONARY_PAGE, dictHeader.GetType())
	require.Equal(t, parquet.Encoding_PLAIN, dictHeader.DictionaryPageHeader.GetEncoding())
	dictBodyEnd := *categoryCol.MetaData.DictionaryPageOffset + int64(dictHeaderLen) + int64(dictHeader.GetCompressedPageSize())
	require.LessOrEqual(t, dictBodyEnd, categoryCol.MetaData.DataPageOffset)

	dataHeader, dataHeaderLen := rawPageHeaderAt(t, data, categoryCol.MetaData.DataPageOffset)
	require.Equal(t, parquet.PageType_DATA_PAGE, dataHeader.GetType())
	require.Contains(t, []parquet.Encoding{parquet.Encoding_PLAIN_DICTIONARY, parquet.Encoding_RLE_DICTIONARY}, dataHeader.DataPageHeader.GetEncoding())
	dataBodyEnd := categoryCol.MetaData.DataPageOffset + int64(dataHeaderLen) + int64(dataHeader.GetCompressedPageSize())
	require.LessOrEqual(t, dataBodyEnd, columnEnd)

	secretCol := cols["Secret"]
	require.NotNil(t, secretCol.CryptoMetadata)
	require.True(t, secretCol.CryptoMetadata.IsSetENCRYPTION_WITH_COLUMN_KEY())
	encryptedHeader := parquet.NewPageHeader()
	_, err = readCompactStructFrom(data, secretCol.MetaData.DataPageOffset, encryptedHeader)
	require.Error(t, err, "encrypted page bytes must not parse as a plaintext page header")

	prNoColumnKey, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(plaintextDictionaryRecord),
		reader.WithFooterKey(footerKey),
	)
	require.NoError(t, err)
	defer func() { _ = prNoColumnKey.ReadStop() }()
	values, _, _, err := prNoColumnKey.ReadColumnByPath(common.ReformPathStr("parquet_go_root.category"), 6)
	require.NoError(t, err)
	require.Equal(t, []any{"red", "blue", "red", "green", "blue", "red"}, values)
}

// threeWayRecord covers the three encryption classifications in a single
// row group:
//   - PlainID: plaintext (no entry in ColumnKeys, mixed mode enabled).
//   - FooterTag: footer-key column (entry is {}; CryptoMetadata =
//     ENCRYPTION_WITH_FOOTER_KEY) - also dictionary-encoded with bloom
//     filter to cover both module paths.
//   - SecretName: column-key column (ENCRYPTION_WITH_COLUMN_KEY) with
//     bloom filter to cover the encrypted bloom-filter path.
//   - PlainScore: plaintext, also with bloom filter to cover the plain
//     bloom-filter path explicitly.
type threeWayRecord struct {
	PlainID    int64   `parquet:"name=plain_id, type=INT64"`
	FooterTag  string  `parquet:"name=footer_tag, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, bloomfilter=true"`
	SecretName string  `parquet:"name=secret_name, type=BYTE_ARRAY, convertedtype=UTF8, bloomfilter=true"`
	PlainScore float64 `parquet:"name=plain_score, type=DOUBLE, bloomfilter=true"`
}

func writeThreeWayFile(t *testing.T, opts ...WriterOption) []byte {
	t.Helper()
	var buf bytes.Buffer
	base := []WriterOption{
		WithNP(1),
		WithRowGroupSize(128),
		WithPageSize(64),
		WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		WithAADPrefix([]byte("three-way")),
		WithAADFileUnique([]byte("three-way-001")),
	}
	pw, err := NewParquetWriter(
		writerfile.NewWriterFile(&buf),
		new(threeWayRecord),
		append(base, opts...)...,
	)
	require.NoError(t, err)
	for i, name := range []string{"alpha", "beta", "gamma", "alpha", "beta"} {
		require.NoError(t, pw.Write(threeWayRecord{
			PlainID:    int64(i + 1),
			FooterTag:  []string{"red", "blue", "red", "green", "blue"}[i],
			SecretName: name,
			PlainScore: float64(i+1) * 1.1,
		}))
	}
	require.NoError(t, pw.WriteStop())
	return buf.Bytes()
}

// TestThreeWayMixRoundTrip verifies that the three classifications
// coexist correctly in one row group and round-trip. The same file also
// covers dictionary encoding on a footer-key column and bloom filters on
// plaintext, footer-key, and column-key columns.
func TestThreeWayMixRoundTrip(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")

	data := writeThreeWayFile(
		t,
		WithFooterKey(footerKey),
		WithColumnEncrypted("footer_tag", ColumnFooterKey()),
		WithColumnEncrypted("secret_name", ColumnKey(nameKey)),
	)

	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(threeWayRecord),
		reader.WithFooterKey(footerKey),
		reader.WithColumnKey("secret_name", nameKey),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	rows := make([]threeWayRecord, 5)
	require.NoError(t, pr.Read(&rows))
	require.Equal(t, []threeWayRecord{
		{PlainID: 1, FooterTag: "red", SecretName: "alpha", PlainScore: 1.1},
		{PlainID: 2, FooterTag: "blue", SecretName: "beta", PlainScore: 2.2},
		{PlainID: 3, FooterTag: "red", SecretName: "gamma", PlainScore: 3.3000000000000003},
		{PlainID: 4, FooterTag: "green", SecretName: "alpha", PlainScore: 4.4},
		{PlainID: 5, FooterTag: "blue", SecretName: "beta", PlainScore: 5.5},
	}, rows)

	// Verify the footer carries the expected CryptoMetadata variants.
	rg := pr.Footer.RowGroups[0]
	cols := make(map[string]*parquet.ColumnChunk, len(rg.Columns))
	for _, c := range rg.Columns {
		cols[c.MetaData.PathInSchema[len(c.MetaData.PathInSchema)-1]] = c
	}
	require.Nil(t, cols["PlainID"].CryptoMetadata)
	require.Nil(t, cols["PlainScore"].CryptoMetadata)
	require.NotNil(t, cols["FooterTag"].CryptoMetadata)
	require.True(t, cols["FooterTag"].CryptoMetadata.IsSetENCRYPTION_WITH_FOOTER_KEY())
	require.NotNil(t, cols["SecretName"].CryptoMetadata)
	require.True(t, cols["SecretName"].CryptoMetadata.IsSetENCRYPTION_WITH_COLUMN_KEY())
}

// TestThreeWayBloomFilterAccess verifies bloom-filter lookups for each
// classification when the appropriate keys are configured.
func TestThreeWayBloomFilterAccess(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")

	data := writeThreeWayFile(
		t,
		WithFooterKey(footerKey),
		WithColumnEncrypted("footer_tag", ColumnFooterKey()),
		WithColumnEncrypted("secret_name", ColumnKey(nameKey)),
	)

	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(threeWayRecord),
		reader.WithFooterKey(footerKey),
		reader.WithColumnKey("secret_name", nameKey),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	// Plaintext column bloom filter - accessible with no special key,
	// but the reader still needs the footer key to open the file.
	found, err := pr.BloomFilterCheck("plain_score", 0, 1.1)
	require.NoError(t, err)
	require.True(t, found)

	// Footer-key column bloom filter - decrypts with the footer key.
	found, err = pr.BloomFilterCheck("footer_tag", 0, "red")
	require.NoError(t, err)
	require.True(t, found)

	// Column-key column bloom filter - decrypts with the column key.
	found, err = pr.BloomFilterCheck("secret_name", 0, "alpha")
	require.NoError(t, err)
	require.True(t, found)
}

// TestThreeWayColumnIndexAccess verifies column index lookups for each
// classification when the appropriate keys are configured. Plaintext
// indexes parse without modular decryption.
func TestThreeWayColumnIndexAccess(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")

	data := writeThreeWayFile(
		t,
		WithFooterKey(footerKey),
		WithColumnEncrypted("footer_tag", ColumnFooterKey()),
		WithColumnEncrypted("secret_name", ColumnKey(nameKey)),
	)

	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(threeWayRecord),
		reader.WithFooterKey(footerKey),
		reader.WithColumnKey("secret_name", nameKey),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	rg := pr.Footer.RowGroups[0]
	require.Len(t, rg.Columns, 4)
	for i := range rg.Columns {
		idx, err := pr.ReadColumnIndex(0, i)
		require.NoErrorf(t, err, "column %d ColumnIndex must decode under configured keys", i)
		require.NotNil(t, idx)
		off, err := pr.ReadOffsetIndex(0, i)
		require.NoErrorf(t, err, "column %d OffsetIndex must decode under configured keys", i)
		require.NotNil(t, off)
	}
}

// TestMixedPlaintextFooterNoKeyOpens covers the no-keys branch: a
// mixed-mode plaintext-footer file opens without keys, plaintext
// columns read, and the footer signature is not verified.
func TestMixedPlaintextFooterNoKeyOpens(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")

	data := writeMixedFile(
		t,
		WithFooterKey(footerKey),
		WithPlaintextFooter(true),
		WithColumnEncrypted("name", ColumnKey(nameKey)),
	)

	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(mixedRecord),
		// no keys at all
	)
	require.NoError(t, err)
	defer func() { _ = pr.ReadStop() }()

	idValues, _, _, err := pr.ReadColumnByPath(common.ReformPathStr("parquet_go_root.id"), 3)
	require.NoError(t, err)
	require.Equal(t, []any{int64(1), int64(2), int64(3)}, idValues)
}

// TestColumnEncryptedTypoFailsValidation verifies that a path that does
// not match a leaf in the schema errors at writer construction, for every
// sub-option.
func TestColumnEncryptedTypoFailsValidation(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	otherKey := []byte("abcdef0123456789")

	cases := []struct {
		name string
		opt  WriterOption
	}{
		{"footer-key (zero-opt)", WithColumnEncrypted("nmae")},
		{"footer-key (explicit)", WithColumnEncrypted("nmae", ColumnFooterKey())},
		{"column-key literal", WithColumnEncrypted("nmae", ColumnKey(otherKey))},
		{"column-key by metadata", WithColumnEncrypted("nmae", ColumnKeyByMetadata([]byte("kid")))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := createTestParquetWriter(
				new(mixedRecord),
				WithFooterKey(footerKey),
				WithKeyRetriever(func([]byte) ([]byte, error) { return otherKey, nil }),
				tc.opt,
			)
			require.Error(t, err)
			require.ErrorContains(t, err, `path "nmae" resolves to "parquet_go_root.nmae"`)
			require.ErrorContains(t, err, "does not match any schema column")
		})
	}
}
