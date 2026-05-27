//go:build example

package main

import (
	"errors"
	"log"

	"github.com/hangxie/parquet-go/v3/common"
	"github.com/hangxie/parquet-go/v3/reader"
	sourcehttp "github.com/hangxie/parquet-go/v3/source/http"
)

// fileURL is a parquet-testing reference file with a plaintext footer.
// Plaintext columns: Boolean_field, Int32_field, Int64_field, Int96_field,
//
//	Ba_field, Flba_field.
//
// Encrypted columns: Float_field (floatKey), Double_field (doubleKey).
// The footer is signed with a separate footer key.
const fileURL = "https://raw.githubusercontent.com/apache/parquet-testing/master/data/encrypt_columns_plaintext_footer.parquet.encrypted"

// plaintextColumns lists schema paths the reader will touch in the positive
// demo. None of them are encrypted, so no encryption key is required.
var plaintextColumns = []string{
	"Schema.Boolean_field",
	"Schema.Int32_field",
	"Schema.Int64_field",
	"Schema.Ba_field",
}

// encryptedColumn is touched in the negative demo. Reading it without the
// column key must fail.
const encryptedColumn = "Schema.Float_field"

func main() {
	if err := readPlaintextColumnsNoKeys(); err != nil {
		log.Fatal("read plaintext columns: ", err)
	}
	if err := confirmEncryptedColumnReadFails(); err != nil {
		log.Fatal("negative case: ", err)
	}
}

// readPlaintextColumnsNoKeys opens the file without any encryption key and
// reads each listed plaintext column directly by path. The plaintext footer
// makes the file metadata accessible without keys, and the reader defers any
// work on encrypted column chunks until they are touched, so this succeeds
// with no key options at all.
func readPlaintextColumnsNoKeys() error {
	fr, err := sourcehttp.NewHttpReader(fileURL, false, false, map[string]string{})
	if err != nil {
		return err
	}
	defer func() { _ = fr.Close() }()

	pr, err := reader.NewParquetReader(fr, nil)
	if err != nil {
		return err
	}
	defer func() { _ = pr.ReadStop() }()

	numRows := pr.GetNumRows()
	log.Printf("opened %d-row file without any encryption key", numRows)
	for _, path := range plaintextColumns {
		values, _, _, err := pr.ReadColumnByPath(common.ReformPathStr(path), numRows)
		if err != nil {
			return err
		}
		log.Printf("  %s: %d values (e.g. %v)", path, len(values), preview(values))
	}
	return nil
}

// confirmEncryptedColumnReadFails opens the same file without any encryption
// key and asks for the encrypted Float_field column. The reader is expected
// to fail with a clear key-related error: the no-key property only holds
// when encrypted columns stay untouched.
func confirmEncryptedColumnReadFails() error {
	fr, err := sourcehttp.NewHttpReader(fileURL, false, false, map[string]string{})
	if err != nil {
		return err
	}
	defer func() { _ = fr.Close() }()

	pr, err := reader.NewParquetReader(fr, nil)
	if err != nil {
		return err
	}
	defer func() { _ = pr.ReadStop() }()

	if _, _, _, err := pr.ReadColumnByPath(common.ReformPathStr(encryptedColumn), pr.GetNumRows()); err != nil {
		log.Printf("expected failure reading encrypted column without key: %v", err)
		return nil
	}
	return errors.New("expected error when reading encrypted column without key, got nil")
}

func preview(values []any) []any {
	if len(values) <= 3 {
		return values
	}
	return values[:3]
}
