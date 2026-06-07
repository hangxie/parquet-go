//go:build example

package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"

	"github.com/hangxie/parquet-go/v3/parquet"
	"github.com/hangxie/parquet-go/v3/reader"
	"github.com/hangxie/parquet-go/v3/source/local"
	"github.com/hangxie/parquet-go/v3/writer"
)

const (
	footerKeyID = "footer-key"
	nameKeyID   = "name-key"
	scoreKeyID  = "score-key"
)

var (
	footerKey = []byte("0123456789012345")
	nameKey   = []byte("1234567890123450")
	scoreKey  = []byte("1234567890123451")
)

type student struct {
	ID     int32   `parquet:"name=id, type=INT32"`
	Name   string  `parquet:"name=name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY, bloomfilter=true"`
	Score  float32 `parquet:"name=score, type=FLOAT"`
	Active bool    `parquet:"name=active, type=BOOLEAN"`
}

type encryptedFile struct {
	name        string
	writerOpts  []writer.WriterOption
	readerOpts  []reader.ReaderOption
	wantRows    []student
	checkBloom  bool
	checkIndex  bool
	description string
}

func main() {
	dir := filepath.Join(os.TempDir(), "parquet-go-encrypt-write")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Fatal("create output directory: ", err)
	}

	rows := []student{
		{ID: 1, Name: "alice", Score: 98.5, Active: true},
		{ID: 2, Name: "bob", Score: 87.25, Active: false},
		{ID: 3, Name: "carol", Score: 91.75, Active: true},
	}

	examples := []encryptedFile{
		{
			name: "encrypted_footer.parquet",
			writerOpts: []writer.WriterOption{
				writer.WithFooterKey(footerKey, []byte(footerKeyID)),
				writer.WithColumnEncrypted("id", writer.ColumnFooterKey()),
				writer.WithColumnEncrypted("name", writer.ColumnFooterKey()),
				writer.WithColumnEncrypted("score", writer.ColumnFooterKey()),
				writer.WithColumnEncrypted("active", writer.ColumnFooterKey()),
				writer.WithAADPrefix([]byte("encrypt-write")),
				writer.WithAADFileUnique([]byte("encrypted01")),
			},
			readerOpts: []reader.ReaderOption{
				reader.WithFooterKey(footerKey),
			},
			wantRows:    rows,
			checkBloom:  true,
			checkIndex:  true,
			description: "AES-GCM encrypted footer, footer key for all columns",
		},
		{
			name: "encrypted_footer_column_keys.parquet",
			writerOpts: []writer.WriterOption{
				writer.WithFooterKey(footerKey, []byte(footerKeyID)),
				writer.WithColumnEncrypted("name", writer.ColumnKey(nameKey, []byte(nameKeyID))),
				writer.WithColumnEncrypted("score", writer.ColumnKey(scoreKey, []byte(scoreKeyID))),
				writer.WithAADPrefix([]byte("encrypt-write")),
				writer.WithAADFileUnique([]byte("columnkeys1")),
			},
			readerOpts: []reader.ReaderOption{
				reader.WithFooterKey(footerKey),
				reader.WithColumnKey("name", nameKey),
				reader.WithColumnKey("score", scoreKey),
			},
			wantRows:    rows,
			checkBloom:  true,
			checkIndex:  true,
			description: "AES-GCM encrypted footer with column-specific keys",
		},
		{
			name: "plaintext_footer_column_keys.parquet",
			writerOpts: []writer.WriterOption{
				writer.WithFooterKey(footerKey, []byte(footerKeyID)),
				writer.WithColumnEncrypted("name", writer.ColumnKey(nameKey, []byte(nameKeyID))),
				writer.WithColumnEncrypted("score", writer.ColumnKey(scoreKey, []byte(scoreKeyID))),
				writer.WithAADPrefix([]byte("encrypt-write")),
				writer.WithAADFileUnique([]byte("plaintext1")),
				writer.WithPlaintextFooter(true),
			},
			readerOpts: []reader.ReaderOption{
				reader.WithFooterKey(footerKey),
				reader.WithColumnKey("name", nameKey),
				reader.WithColumnKey("score", scoreKey),
			},
			wantRows:    rows,
			checkBloom:  true,
			checkIndex:  true,
			description: "plaintext footer signature with encrypted columns",
		},
		{
			name: "gcm_ctr_external_aad.parquet",
			writerOpts: []writer.WriterOption{
				writer.WithEncryptionAlgorithm(writer.EncryptionAESGCMCTRV1),
				writer.WithFooterKey(footerKey, []byte(footerKeyID)),
				writer.WithColumnEncrypted("name", writer.ColumnKey(nameKey, []byte(nameKeyID))),
				writer.WithAADPrefix([]byte("external-prefix")),
				writer.WithAADFileUnique([]byte("gcmctrfile1")),
				writer.WithSupplyAADPrefix(true),
			},
			readerOpts: []reader.ReaderOption{
				reader.WithFooterKey(footerKey),
				reader.WithColumnKey("name", nameKey),
				reader.WithAADPrefix([]byte("external-prefix")),
			},
			wantRows:    rows,
			checkBloom:  true,
			checkIndex:  true,
			description: "AES-GCM-CTR pages with externally supplied AAD prefix",
		},
		{
			name: "key_retriever.parquet",
			writerOpts: []writer.WriterOption{
				writer.WithFooterKeyMetadata([]byte(footerKeyID)),
				writer.WithColumnEncrypted("name", writer.ColumnKeyByMetadata([]byte(nameKeyID))),
				writer.WithKeyRetriever(keyRetriever),
				writer.WithAADPrefix([]byte("encrypt-write")),
				writer.WithAADFileUnique([]byte("retriever01")),
			},
			readerOpts: []reader.ReaderOption{
				reader.WithKeyRetriever(keyRetriever),
			},
			wantRows:    rows,
			checkBloom:  true,
			checkIndex:  true,
			description: "keys resolved from key metadata while writing and reading",
		},
		{
			// Mixed columns: name encrypted with its own key, score
			// encrypted with the footer key, every other column written as
			// plaintext (the default for unlisted columns).
			name: "mixed_columns.parquet",
			writerOpts: []writer.WriterOption{
				writer.WithFooterKey(footerKey, []byte(footerKeyID)),
				writer.WithColumnEncrypted("name", writer.ColumnKey(nameKey, []byte(nameKeyID))),
				writer.WithColumnEncrypted("score", writer.ColumnFooterKey()),
				writer.WithAADPrefix([]byte("encrypt-write")),
				writer.WithAADFileUnique([]byte("mixed-cols1")),
			},
			readerOpts: []reader.ReaderOption{
				reader.WithFooterKey(footerKey),
				reader.WithColumnKey("name", nameKey),
			},
			wantRows:    rows,
			checkBloom:  true,
			checkIndex:  true,
			description: "mixed: name=column key, score=footer key, others plaintext",
		},
		{
			// All columns plaintext with an encrypted footer. The footer
			// key is still required to open the file because schema and
			// row-group metadata live inside the encrypted footer.
			name: "encrypted_footer_all_plaintext_columns.parquet",
			writerOpts: []writer.WriterOption{
				writer.WithFooterKey(footerKey, []byte(footerKeyID)),
				writer.WithAADPrefix([]byte("encrypt-write")),
				writer.WithAADFileUnique([]byte("only-footer1")),
			},
			readerOpts: []reader.ReaderOption{
				reader.WithFooterKey(footerKey),
			},
			wantRows:    rows,
			checkBloom:  true,
			checkIndex:  true,
			description: "encrypted footer hides schema, every column body is plaintext",
		},
	}

	for _, example := range examples {
		path := filepath.Join(dir, example.name)
		if err := writeEncrypted(path, example.wantRows, example.writerOpts); err != nil {
			log.Fatalf("%s: write encrypted parquet: %v", example.name, err)
		}
		if err := readBack(path, example); err != nil {
			log.Fatalf("%s: read back encrypted parquet: %v", example.name, err)
		}
		log.Printf("%s ok: %s", path, example.description)
	}
}

func keyRetriever(keyMetadata []byte) ([]byte, error) {
	keys := map[string][]byte{
		footerKeyID: footerKey,
		nameKeyID:   nameKey,
		scoreKeyID:  scoreKey,
	}
	key, ok := keys[string(keyMetadata)]
	if !ok {
		return nil, fmt.Errorf("unknown key metadata %q", keyMetadata)
	}
	return key, nil
}

func writeEncrypted(path string, rows []student, opts []writer.WriterOption) error {
	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		return fmt.Errorf("open writer: %w", err)
	}
	defer fw.Close()

	writerOpts := append([]writer.WriterOption{
		writer.WithNP(1),
		writer.WithRowGroupSize(128),
		writer.WithPageSize(32),
		writer.WithCompressionCodec(parquet.CompressionCodec_UNCOMPRESSED),
	}, opts...)
	pw, err := writer.NewParquetWriter(fw, new(student), writerOpts...)
	if err != nil {
		return fmt.Errorf("create parquet writer: %w", err)
	}

	for _, row := range rows {
		if err := pw.Write(row); err != nil {
			return fmt.Errorf("write row: %w", err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("stop writer: %w", err)
	}
	return nil
}

func readBack(path string, example encryptedFile) error {
	fr, err := local.NewLocalFileReader(path)
	if err != nil {
		return fmt.Errorf("open reader: %w", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, new(student), example.readerOpts...)
	if err != nil {
		return fmt.Errorf("create parquet reader: %w", err)
	}
	defer func() {
		if err := pr.ReadStop(); err != nil {
			log.Printf("close parquet reader: %v", err)
		}
	}()

	got := make([]student, len(example.wantRows))
	if err := pr.Read(&got); err != nil {
		return fmt.Errorf("read rows: %w", err)
	}
	if !reflect.DeepEqual(example.wantRows, got) {
		return fmt.Errorf("rows mismatch: got %+v want %+v", got, example.wantRows)
	}

	if example.checkIndex {
		for rg, rowGroup := range pr.Footer.RowGroups {
			for col := range rowGroup.GetColumns() {
				index, err := pr.ReadColumnIndex(rg, col)
				if err != nil {
					return fmt.Errorf("read column index rg %d col %d: %w", rg, col, err)
				}
				if index == nil {
					return fmt.Errorf("missing column index for rg %d col %d", rg, col)
				}
			}
		}
	}

	if example.checkBloom {
		found, err := pr.BloomFilterCheck("name", 0, example.wantRows[0].Name)
		if err != nil {
			return fmt.Errorf("check encrypted bloom filter: %w", err)
		}
		if !found {
			return fmt.Errorf("bloom filter did not find %q", example.wantRows[0].Name)
		}
	}

	return nil
}
