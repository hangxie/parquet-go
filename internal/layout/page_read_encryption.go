package layout

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"

	"github.com/hangxie/parquet-go/v3/internal/encryption"
	"github.com/hangxie/parquet-go/v3/parquet"
)

// PageEncryptionAlgorithm identifies the Parquet page encryption algorithm.
type PageEncryptionAlgorithm int

const (
	PageEncryptionAESGCM PageEncryptionAlgorithm = iota + 1
	PageEncryptionAESGCMCTR
)

// PageDecryptor carries the state needed to decrypt encrypted page modules.
type PageDecryptor struct {
	Algorithm       PageEncryptionAlgorithm
	Key             []byte
	AADPrefix       []byte
	AADFileUnique   []byte
	RowGroupOrdinal int16
	ColumnOrdinal   int16
	PageOrdinal     int16
}

func decryptPageHeader(module []byte, decryptor *PageDecryptor) (*parquet.PageHeader, error) {
	tries := []encryption.ModuleType{encryption.ModuleDataPageHeader, encryption.ModuleDictionaryPageHeader}
	var lastErr error
	for _, moduleType := range tries {
		aad := pageAAD(decryptor, moduleType)
		plain, err := encryption.DecryptGCM(decryptor.Key, aad, module)
		if err != nil {
			lastErr = err
			continue
		}
		pageHeader, err := readPageHeaderFromBytes(plain)
		if err != nil {
			return nil, err
		}
		if pageHeader.GetType() == parquet.PageType_DATA_PAGE || pageHeader.GetType() == parquet.PageType_DATA_PAGE_V2 {
			return pageHeader, nil
		}
		if pageHeader.GetType() == parquet.PageType_DICTIONARY_PAGE {
			return pageHeader, nil
		}
		return nil, fmt.Errorf("unsupported encrypted page type: %v", pageHeader.GetType())
	}
	return nil, fmt.Errorf("decrypt page header: %w", lastErr)
}

func readPageHeaderFromBytes(buf []byte) (*parquet.PageHeader, error) {
	protocol := thrift.NewTCompactProtocolConf(thrift.NewStreamTransportR(bytes.NewReader(buf)), &thrift.TConfiguration{})
	pageHeader := parquet.NewPageHeader()
	if err := pageHeader.Read(context.TODO(), protocol); err != nil {
		return nil, err
	}
	return pageHeader, nil
}

func readEncryptedPageBody(thriftReader *thrift.TBufferedTransport, pageHeader *parquet.PageHeader, opt PageReadOptions) ([]byte, error) {
	module, err := encryption.ReadModule(thriftReader, opt.MaxPageSize)
	if err != nil {
		return nil, err
	}

	moduleType := encryptedPageModuleType(pageHeader)
	if opt.Decryptor.Algorithm == PageEncryptionAESGCMCTR {
		return encryption.DecryptCTR(opt.Decryptor.Key, module)
	}
	return encryption.DecryptGCM(opt.Decryptor.Key, pageAAD(opt.Decryptor, moduleType), module)
}

func encryptedPageModuleType(pageHeader *parquet.PageHeader) encryption.ModuleType {
	if pageHeader.GetType() == parquet.PageType_DICTIONARY_PAGE {
		return encryption.ModuleDictionaryPage
	}
	return encryption.ModuleDataPage
}

func pageAAD(decryptor *PageDecryptor, moduleType encryption.ModuleType) []byte {
	pageOrdinal := decryptor.PageOrdinal
	if moduleType == encryption.ModuleDictionaryPage || moduleType == encryption.ModuleDictionaryPageHeader {
		pageOrdinal = 0
	}
	return encryption.AAD(
		decryptor.AADPrefix,
		decryptor.AADFileUnique,
		moduleType,
		decryptor.RowGroupOrdinal,
		decryptor.ColumnOrdinal,
		pageOrdinal,
	)
}
