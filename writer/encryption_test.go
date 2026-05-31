package writer

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/common"
	internalEncryption "github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/internal/layout"
	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/schema"
	"github.com/hangxie/parquet-go/v3/source/buffer"
	"github.com/hangxie/parquet-go/v3/source/writerfile"
)

const (
	testFooterKeyID = "footer-key"
	testNameKeyID   = "name-key"
)

type encryptedWriterRecord struct {
	ID   int32  `parquet:"name=id, type=INT32"`
	Name string `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, bloomfilter=true"`
}

func TestEncryptedWriterRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		writerOptions []WriterOption
		readerOptions []reader.ReaderOption
		wantTailMagic string
	}{
		{
			name: "encrypted footer aes gcm",
			writerOptions: []WriterOption{
				WithFooterKey([]byte("0123456789abcdef")),
				WithAADPrefix([]byte("writer-test")),
				WithAADFileUnique([]byte("file-unique1")),
			},
			readerOptions: []reader.ReaderOption{
				reader.WithFooterKey([]byte("0123456789abcdef")),
			},
			wantTailMagic: common.MagicBytesEncrypted,
		},
		{
			name: "plaintext footer aes gcm ctr with column key",
			writerOptions: []WriterOption{
				WithEncryptionAlgorithm(EncryptionAESGCMCTRV1),
				WithFooterKey([]byte("0123456789abcdef")),
				WithAADPrefix([]byte("writer-test")),
				WithAADFileUnique([]byte("file-unique2")),
				WithPlaintextFooter(true),
				WithColumnKey("name", []byte("abcdef0123456789"), []byte(testNameKeyID)),
			},
			readerOptions: []reader.ReaderOption{
				reader.WithFooterKey([]byte("0123456789abcdef")),
				reader.WithColumnKey("name", []byte("abcdef0123456789")),
			},
			wantTailMagic: common.MagicBytes,
		},
		{
			name: "WithEncryption direct config",
			writerOptions: []WriterOption{
				WithEncryption(EncryptionConfig{
					FooterKey:     []byte("0123456789abcdef"),
					AADPrefix:     []byte("writer-test"),
					AADFileUnique: []byte("file-unique6"),
				}),
			},
			readerOptions: []reader.ReaderOption{
				reader.WithFooterKey([]byte("0123456789abcdef")),
			},
			wantTailMagic: common.MagicBytesEncrypted,
		},
		{
			name: "supply aad prefix not stored in file",
			writerOptions: []WriterOption{
				WithFooterKey([]byte("0123456789abcdef"), []byte(testFooterKeyID)),
				WithAADPrefix([]byte("writer-test")),
				WithAADFileUnique([]byte("file-unique7")),
				WithSupplyAADPrefix(true),
			},
			readerOptions: []reader.ReaderOption{
				reader.WithFooterKey([]byte("0123456789abcdef")),
				reader.WithAADPrefix([]byte("writer-test")),
			},
			wantTailMagic: common.MagicBytesEncrypted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			pw, err := NewParquetWriter(
				writerfile.NewWriterFile(&buf),
				new(encryptedWriterRecord),
				append([]WriterOption{
					WithNP(1),
					WithRowGroupSize(128),
					WithPageSize(32),
					WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
				}, tt.writerOptions...)...,
			)
			require.NoError(t, err)
			require.NoError(t, pw.Write(encryptedWriterRecord{ID: 1, Name: "alpha"}))
			require.NoError(t, pw.Write(encryptedWriterRecord{ID: 2, Name: "beta"}))
			require.NoError(t, pw.WriteStop())

			data := buf.Bytes()
			require.Equal(t, tt.wantTailMagic, string(data[len(data)-4:]))

			pr, err := reader.NewParquetReader(buffer.NewBufferReaderFromBytesNoAlloc(data), new(encryptedWriterRecord), tt.readerOptions...)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, pr.ReadStop())
			}()

			rows := make([]encryptedWriterRecord, 2)
			require.NoError(t, pr.Read(&rows))
			require.Equal(t, []encryptedWriterRecord{{ID: 1, Name: "alpha"}, {ID: 2, Name: "beta"}}, rows)

			columnIndex, err := pr.ReadColumnIndex(0, 1)
			require.NoError(t, err)
			require.NotNil(t, columnIndex)

			found, err := pr.BloomFilterCheck("name", 0, "alpha")
			require.NoError(t, err)
			require.True(t, found)
		})
	}
}

func TestEncryptedWriterAuthFailure(t *testing.T) {
	t.Parallel()

	writeRows := func(t *testing.T, opts ...WriterOption) []byte {
		t.Helper()
		var buf bytes.Buffer
		pw, err := NewParquetWriter(
			writerfile.NewWriterFile(&buf),
			new(encryptedWriterRecord),
			append([]WriterOption{
				WithNP(1),
				WithRowGroupSize(128),
				WithPageSize(32),
				WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
			}, opts...)...,
		)
		require.NoError(t, err)
		require.NoError(t, pw.Write(encryptedWriterRecord{ID: 1, Name: "alpha"}))
		require.NoError(t, pw.WriteStop())
		return buf.Bytes()
	}

	tryRead := func(data []byte, opts ...reader.ReaderOption) error {
		pr, err := reader.NewParquetReader(
			buffer.NewBufferReaderFromBytesNoAlloc(data),
			new(encryptedWriterRecord),
			opts...,
		)
		if err != nil {
			return err
		}
		defer pr.ReadStop() //nolint:errcheck
		rows := make([]encryptedWriterRecord, 1)
		return pr.Read(&rows)
	}

	t.Run("wrong footer key", func(t *testing.T) {
		t.Parallel()
		data := writeRows(
			t,
			WithFooterKey([]byte("0123456789abcdef")),
			WithAADPrefix([]byte("auth-test")),
			WithAADFileUnique([]byte("authtest-001")),
		)
		err := tryRead(data, reader.WithFooterKey([]byte("abcdef0123456789")))
		require.Error(t, err)
	})

	t.Run("wrong column key", func(t *testing.T) {
		t.Parallel()
		data := writeRows(
			t,
			WithFooterKey([]byte("0123456789abcdef")),
			WithColumnKey("name", []byte("abcdef0123456789"), []byte(testNameKeyID)),
			WithAADPrefix([]byte("auth-test")),
			WithAADFileUnique([]byte("authtest-002")),
		)
		err := tryRead(
			data,
			reader.WithFooterKey([]byte("0123456789abcdef")),
			reader.WithColumnKey("name", []byte("1234567890abcdef")),
		)
		require.Error(t, err)
	})

	t.Run("wrong AAD prefix", func(t *testing.T) {
		t.Parallel()
		data := writeRows(
			t,
			WithFooterKey([]byte("0123456789abcdef")),
			WithAADPrefix([]byte("real-prefix")),
			WithAADFileUnique([]byte("authtest-003")),
			WithSupplyAADPrefix(true),
		)
		err := tryRead(
			data,
			reader.WithFooterKey([]byte("0123456789abcdef")),
			reader.WithAADPrefix([]byte("wrong-prefix")),
		)
		require.Error(t, err)
	})
}

func TestEncryptedWriterColumnKeyProtectsBloomAndIndex(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")

	var buf bytes.Buffer
	pw, err := NewParquetWriter(
		writerfile.NewWriterFile(&buf),
		new(encryptedWriterRecord),
		WithNP(1),
		WithRowGroupSize(128),
		WithPageSize(32),
		WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		WithFooterKey(footerKey),
		WithColumnKey("name", nameKey, []byte(testNameKeyID)),
		WithAADPrefix([]byte("bloom-index-test")),
		WithAADFileUnique([]byte("bi-test-001")),
	)
	require.NoError(t, err)
	require.NoError(t, pw.Write(encryptedWriterRecord{ID: 1, Name: "alpha"}))
	require.NoError(t, pw.Write(encryptedWriterRecord{ID: 2, Name: "beta"}))
	require.NoError(t, pw.WriteStop())

	data := buf.Bytes()

	// With correct keys: column index works for every column, bloom filter
	// works for the name column (encrypted with its own key).
	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(encryptedWriterRecord),
		reader.WithFooterKey(footerKey),
		reader.WithColumnKey("name", nameKey),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, pr.ReadStop()) }()

	for rg, rowGroup := range pr.Footer.RowGroups {
		for col := range rowGroup.GetColumns() {
			idx, idxErr := pr.ReadColumnIndex(rg, col)
			require.NoError(t, idxErr)
			require.NotNil(t, idx)
		}
	}

	found, err := pr.BloomFilterCheck("name", 0, "alpha")
	require.NoError(t, err)
	require.True(t, found)

	notFound, err := pr.BloomFilterCheck("name", 0, "nosuchvalue")
	require.NoError(t, err)
	require.False(t, notFound)

	// Without the name column key: the reader can open the file because
	// the encrypted footer contains plaintext column metadata, but reading
	// encrypted name-column modules still requires the name key.
	prMissingNameKey, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(encryptedWriterRecord),
		reader.WithFooterKey(footerKey),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, prMissingNameKey.ReadStop()) }()

	_, err = prMissingNameKey.ReadColumnIndex(0, 1)
	require.ErrorContains(t, err, "decryption key required for column name")

	_, err = prMissingNameKey.BloomFilterCheck("name", 0, "alpha")
	require.ErrorContains(t, err, "decryption key required for column name")
}

func TestEncryptedFooterColumnMetadataLayout(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")

	tests := []struct {
		name              string
		opts              []WriterOption
		wantFooterKeyCol  string // name of column expected to use footer key
		wantColumnKeyCols []string
	}{
		{
			name: "all columns use footer key",
			opts: []WriterOption{
				WithFooterKey(footerKey),
				WithAADFileUnique([]byte("layout-001")),
			},
			wantFooterKeyCol:  "id", // both columns
			wantColumnKeyCols: nil,
		},
		{
			name: "mixed footer-key and column-key columns",
			opts: []WriterOption{
				WithFooterKey(footerKey),
				WithColumnKey("name", nameKey),
				WithAADFileUnique([]byte("layout-002")),
			},
			wantFooterKeyCol:  "id",
			wantColumnKeyCols: []string{"name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			pw, err := NewParquetWriter(
				writerfile.NewWriterFile(&buf),
				new(encryptedWriterRecord),
				append([]WriterOption{
					WithNP(1),
					WithRowGroupSize(128),
					WithPageSize(32),
					WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
				}, tt.opts...)...,
			)
			require.NoError(t, err)
			require.NoError(t, pw.Write(encryptedWriterRecord{ID: 1, Name: "alpha"}))
			require.NoError(t, pw.WriteStop())

			columnKeyCols := make(map[string]bool, len(tt.wantColumnKeyCols))
			for _, c := range tt.wantColumnKeyCols {
				columnKeyCols[c] = true
			}

			for rgIdx, rg := range pw.Footer.RowGroups {
				for colIdx, col := range rg.Columns {
					require.NotNil(t, col.CryptoMetadata, "row group %d column %d missing crypto metadata", rgIdx, colIdx)
					if col.CryptoMetadata.IsSetENCRYPTION_WITH_FOOTER_KEY() {
						// Spec: footer-key columns keep ColumnMetaData in
						// the original location; encrypted_column_metadata
						// must not be set.
						require.NotNil(t, col.MetaData, "footer-key column %d MetaData was cleared", colIdx)
						require.Nil(t, col.EncryptedColumnMetadata, "footer-key column %d should not use encrypted_column_metadata", colIdx)
						continue
					}
					path := col.CryptoMetadata.GetENCRYPTION_WITH_COLUMN_KEY().GetPathInSchema()
					require.True(t, columnKeyCols[path[len(path)-1]], "column %s should not use column-specific key", path)
					// Column-key columns in encrypted footer: MetaData stays
					// populated (it's inside the encrypted footer so there is
					// no security concern), and encrypted_column_metadata is
					// also set so column-key-only readers can access it.
					require.NotNil(t, col.MetaData, "column-key column %d MetaData must be kept in encrypted footer", colIdx)
					require.NotEmpty(t, col.EncryptedColumnMetadata, "column-key column %d missing encrypted_column_metadata", colIdx)
				}
			}
		})
	}
}

func TestPlaintextFooterColumnMetadataLayout(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")
	nameKey := []byte("abcdef0123456789")

	var buf bytes.Buffer
	pw, err := NewParquetWriter(
		writerfile.NewWriterFile(&buf),
		new(encryptedWriterRecord),
		WithNP(1),
		WithRowGroupSize(128),
		WithPageSize(32),
		WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		WithFooterKey(footerKey),
		WithColumnKey("name", nameKey),
		WithPlaintextFooter(true),
		WithAADFileUnique([]byte("pf-layout-001")),
	)
	require.NoError(t, err)
	require.NoError(t, pw.Write(encryptedWriterRecord{ID: 1, Name: "alpha"}))
	require.NoError(t, pw.Write(encryptedWriterRecord{ID: 2, Name: "beta"}))
	require.NoError(t, pw.WriteStop())

	for rgIdx, rg := range pw.Footer.RowGroups {
		for colIdx, col := range rg.Columns {
			// Plaintext footer mode keeps ColumnMetaData in its original
			// location so legacy readers can still parse offsets and sizes.
			require.NotNil(t, col.MetaData, "row group %d column %d MetaData was cleared in plaintext footer mode", rgIdx, colIdx)
			// Encrypted columns must strip all sensitive statistics from the
			// plaintext metadata.
			require.Nil(t, col.MetaData.Statistics, "row group %d column %d plaintext Statistics should be stripped", rgIdx, colIdx)
			require.Nil(t, col.MetaData.SizeStatistics, "row group %d column %d plaintext SizeStatistics should be stripped", rgIdx, colIdx)
			require.Nil(t, col.MetaData.GeospatialStatistics, "row group %d column %d plaintext GeospatialStatistics should be stripped", rgIdx, colIdx)
			// The full metadata (with stats) is available via
			// encrypted_column_metadata for authenticated readers.
			require.NotEmpty(t, col.EncryptedColumnMetadata, "row group %d column %d missing encrypted_column_metadata", rgIdx, colIdx)
		}
	}
}

func TestEncryptedWriterPlaintextFooterSignatureVerification(t *testing.T) {
	t.Parallel()

	footerKey := []byte("0123456789abcdef")

	var buf bytes.Buffer
	pw, err := NewParquetWriter(
		writerfile.NewWriterFile(&buf),
		new(encryptedWriterRecord),
		WithNP(1),
		WithRowGroupSize(128),
		WithPageSize(32),
		WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
		WithFooterKey(footerKey),
		WithPlaintextFooter(true),
		WithAADPrefix([]byte("plaintext-footer-test")),
		WithAADFileUnique([]byte("pft-001")),
	)
	require.NoError(t, err)
	require.NoError(t, pw.Write(encryptedWriterRecord{ID: 1, Name: "alpha"}))
	require.NoError(t, pw.WriteStop())

	data := buf.Bytes()

	// Correct key: footer signature verifies and rows are readable.
	pr, err := reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(encryptedWriterRecord),
		reader.WithFooterKey(footerKey),
	)
	require.NoError(t, err)
	rows := make([]encryptedWriterRecord, 1)
	require.NoError(t, pr.Read(&rows))
	require.Equal(t, int32(1), rows[0].ID)
	require.NoError(t, pr.ReadStop())

	// Wrong key: footer signature verification must fail.
	_, err = reader.NewParquetReader(
		buffer.NewBufferReaderFromBytesNoAlloc(data),
		new(encryptedWriterRecord),
		reader.WithFooterKey([]byte("abcdef0123456789")),
	)
	require.Error(t, err)
}

func TestStripRoot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path []string
		want []string
	}{
		{
			name: "strips internal root",
			path: []string{common.ParGoRootInName, "col"},
			want: []string{"col"},
		},
		{
			name: "strips external root",
			path: []string{common.ParGoRootExName, "col"},
			want: []string{"col"},
		},
		{
			name: "no root prefix unchanged",
			path: []string{"col"},
			want: []string{"col"},
		},
		{
			name: "empty path unchanged",
			path: []string{},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, stripRoot(tt.path))
		})
	}
}

func TestKeyForEncryptedColumnErrors(t *testing.T) {
	t.Parallel()

	pw, _, err := createTestParquetWriter(
		new(encryptedWriterRecord),
		WithFooterKey([]byte("0123456789abcdef")),
		WithAADFileUnique([]byte("kfec-test-001")),
	)
	require.NoError(t, err)

	t.Run("unsupported crypto metadata", func(t *testing.T) {
		t.Parallel()
		column := parquet.NewColumnChunk()
		column.CryptoMetadata = &parquet.ColumnCryptoMetaData{}
		_, err := pw.keyForEncryptedColumn(column)
		require.ErrorContains(t, err, "unsupported column crypto metadata")
	})

	t.Run("missing column key", func(t *testing.T) {
		t.Parallel()
		column := parquet.NewColumnChunk()
		column.CryptoMetadata = &parquet.ColumnCryptoMetaData{
			ENCRYPTION_WITH_COLUMN_KEY: &parquet.EncryptionWithColumnKey{
				PathInSchema: []string{"nonexistent_column"},
			},
		}
		_, err := pw.keyForEncryptedColumn(column)
		require.ErrorContains(t, err, "missing encryption key for column")
	})
}

func TestEncryptPageUpdatesCompressedPageSize(t *testing.T) {
	t.Parallel()

	// AES-GCM body module: 4 (prefix) + 12 (nonce) + bodyLen + 16 (tag) = bodyLen + 32
	// AES-CTR body module: 4 (prefix) + 12 (nonce) + bodyLen = bodyLen + 16
	tests := []struct {
		name           string
		algorithm      EncryptionAlgorithm
		bodyLen        int
		encryptedExtra int
	}{
		{"GCM adds nonce+tag+prefix", EncryptionAESGCMV1, 10, 32},
		{"CTR adds nonce+prefix", EncryptionAESGCMCTRV1, 20, 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pw, _, err := createTestParquetWriter(
				new(encryptedWriterRecord),
				WithEncryptionAlgorithm(tt.algorithm),
				WithFooterKey([]byte("0123456789abcdef")),
				WithAADFileUnique([]byte("pgsize-test-001")),
			)
			require.NoError(t, err)

			header := parquet.NewPageHeader()
			header.Type = parquet.PageType_DATA_PAGE
			header.DataPageHeader = parquet.NewDataPageHeader()
			header.UncompressedPageSize = int32(tt.bodyLen)
			header.CompressedPageSize = int32(tt.bodyLen)

			headerBuf, err := serializeCompact(header)
			require.NoError(t, err)

			rawData := make([]byte, len(headerBuf)+tt.bodyLen)
			copy(rawData, headerBuf)
			page := &layout.Page{Header: header, RawData: rawData}

			require.NoError(t, pw.encryptPage(page, pw.encryptionState.footerKey, 0, 0, 0))

			require.Equal(t, int32(tt.bodyLen+tt.encryptedExtra), page.Header.CompressedPageSize,
				"CompressedPageSize must reflect encrypted body module size so readers advance correctly")
		})
	}
}

func TestEncryptPageErrors(t *testing.T) {
	t.Parallel()

	pw, _, err := createTestParquetWriter(
		new(encryptedWriterRecord),
		WithFooterKey([]byte("0123456789abcdef")),
		WithAADFileUnique([]byte("ep-test-001")),
	)
	require.NoError(t, err)

	key := pw.encryptionState.footerKey
	header := parquet.NewPageHeader()
	header.Type = parquet.PageType_DATA_PAGE
	header.DataPageHeader = parquet.NewDataPageHeader()
	header.UncompressedPageSize = 10
	header.CompressedPageSize = 10

	t.Run("raw data shorter than header", func(t *testing.T) {
		t.Parallel()
		page := &layout.Page{
			Header:  header,
			RawData: []byte{0x01},
		}
		err := pw.encryptPage(page, key, 0, 0, 0)
		require.ErrorContains(t, err, "page raw data shorter than serialized header")
	})

	t.Run("header re-serialization mismatch", func(t *testing.T) {
		t.Parallel()
		headerBuf, serErr := serializeCompact(header)
		require.NoError(t, serErr)
		rawData := bytes.Repeat([]byte{0xFF}, len(headerBuf)+10)
		page := &layout.Page{
			Header:  header,
			RawData: rawData,
		}
		err := pw.encryptPage(page, key, 0, 0, 0)
		require.ErrorContains(t, err, "page header re-serialization mismatch")
	})
}

func TestEncryptionHelpersNoopWithoutState(t *testing.T) {
	t.Parallel()

	pw := &ParquetWriter{}

	page := &layout.Page{
		Header:  parquet.NewPageHeader(),
		RawData: []byte("unchanged"),
	}
	require.NoError(t, pw.encryptPage(page, []byte("ignored"), 0, 0, 0))
	require.Equal(t, []byte("unchanged"), page.RawData)

	column := parquet.NewColumnChunk()
	column.MetaData = parquet.NewColumnMetaData()
	require.NoError(t, pw.encryptColumnMetadata(0, 0, column))
	require.Nil(t, column.CryptoMetadata)
	require.Nil(t, column.EncryptedColumnMetadata)

	footer := []byte("footer")
	gotFooter, gotMagic, err := pw.encryptFooter(footer)
	require.NoError(t, err)
	require.Equal(t, footer, gotFooter)
	require.Equal(t, common.MagicBytes, gotMagic)
}

func TestEncryptionHelpersHandleMissingMetadata(t *testing.T) {
	t.Parallel()

	pw, _, err := createTestParquetWriter(
		new(encryptedWriterRecord),
		WithFooterKey([]byte("0123456789abcdef")),
		WithAADFileUnique([]byte("missing-md1")),
	)
	require.NoError(t, err)

	tests := []struct {
		name   string
		column *parquet.ColumnChunk
	}{
		{name: "nil column"},
		{name: "nil metadata", column: parquet.NewColumnChunk()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, pw.encryptColumnMetadata(0, 0, tt.column))
		})
	}
}

func TestEncryptionPathHelpersWithoutGeneratedSchema(t *testing.T) {
	t.Parallel()

	t.Run("leaf path set without schema", func(t *testing.T) {
		t.Parallel()
		pw := &ParquetWriter{}
		require.Empty(t, pw.leafColumnPathSet())
	})

	t.Run("column candidates include stripped schema mapping", func(t *testing.T) {
		t.Parallel()
		pw := &ParquetWriter{
			SchemaHandler: &schema.SchemaHandler{
				InPathToExPath: map[string]string{
					"name": common.ReformPathStr("external.name"),
				},
				ExPathToInPath: map[string]string{
					common.ReformPathStr("external.name"): "name",
				},
			},
		}

		require.Contains(
			t,
			pw.columnPathCandidates([]string{common.ParGoRootInName, "name"}),
			common.ReformPathStr("external.name"),
		)
		require.Contains(
			t,
			pw.columnPathCandidates([]string{"external", "name"}),
			"name",
		)
	})
}

func TestEncryptionHelperInvalidKeyErrors(t *testing.T) {
	t.Parallel()

	pw, _, err := createTestParquetWriter(
		new(encryptedWriterRecord),
		WithFooterKey([]byte("0123456789abcdef")),
		WithAADFileUnique([]byte("invalidkey1")),
	)
	require.NoError(t, err)

	t.Run("thrift module", func(t *testing.T) {
		t.Parallel()
		_, err := pw.encryptThriftModule([]byte("short"), internalEncryption.ModuleColumnIndex, 0, 0, []byte("plain"))
		require.ErrorContains(t, err, "encrypt module")
	})

	t.Run("plaintext footer signature", func(t *testing.T) {
		t.Parallel()
		bad := *pw.encryptionState
		bad.footerKey = []byte("short")
		bad.plaintextFooter = true
		localPW := &ParquetWriter{encryptionState: &bad}

		_, _, err := localPW.encryptFooter([]byte("footer"))
		require.ErrorContains(t, err, "sign plaintext footer")
	})

	t.Run("encrypted footer", func(t *testing.T) {
		t.Parallel()
		bad := *pw.encryptionState
		bad.footerKey = []byte("short")
		localPW := &ParquetWriter{encryptionState: &bad}

		_, _, err := localPW.encryptFooter([]byte("footer"))
		require.ErrorContains(t, err, "encrypt footer")
	})
}
