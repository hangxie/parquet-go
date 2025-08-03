package parquet

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"
)

func Test_AdvancedParquetStructures(t *testing.T) {
	ctx := context.Background()
	transport := thrift.NewTMemoryBuffer()
	protocol := thrift.NewTBinaryProtocolConf(transport, nil)

	xxHash := NewXxHash()
	err := xxHash.Write(ctx, protocol)
	require.NoError(t, err)

	newXxHash := NewXxHash()
	err = newXxHash.Read(ctx, protocol)
	require.NoError(t, err)
	require.True(t, xxHash.Equals(newXxHash))

	transport.Reset()
	bloomHash := NewBloomFilterHash()
	bloomHash.XXHASH = NewXxHash()

	err = bloomHash.Write(ctx, protocol)
	require.NoError(t, err)

	newBloomHash := NewBloomFilterHash()
	err = newBloomHash.Read(ctx, protocol)
	require.NoError(t, err)
	require.True(t, bloomHash.Equals(newBloomHash))
	require.NotNil(t, newBloomHash.XXHASH)

	count := bloomHash.CountSetFieldsBloomFilterHash()
	require.Equal(t, 1, count)

	transport.Reset()
	bloomComp := NewBloomFilterCompression()
	bloomComp.UNCOMPRESSED = NewUncompressed()

	err = bloomComp.Write(ctx, protocol)
	require.NoError(t, err)

	newBloomComp := NewBloomFilterCompression()
	require.NoError(t, newBloomComp.Read(ctx, protocol))
	require.True(t, bloomComp.Equals(newBloomComp))
	require.NotNil(t, newBloomComp.UNCOMPRESSED)

	compCount := bloomComp.CountSetFieldsBloomFilterCompression()
	require.Equal(t, 1, compCount)

	transport.Reset()
	uncompressed := NewUncompressed()
	require.NoError(t, uncompressed.Write(ctx, protocol))

	newUncompressed := NewUncompressed()
	require.NoError(t, newUncompressed.Read(ctx, protocol))
	require.True(t, uncompressed.Equals(newUncompressed))

	transport.Reset()
	bloomHeader := NewBloomFilterHeader()
	bloomHeader.NumBytes = 1024
	bloomHeader.Algorithm = NewBloomFilterAlgorithm()
	bloomHeader.Algorithm.BLOCK = NewSplitBlockAlgorithm()
	bloomHeader.Hash = NewBloomFilterHash()
	bloomHeader.Hash.XXHASH = NewXxHash()
	bloomHeader.Compression = NewBloomFilterCompression()
	bloomHeader.Compression.UNCOMPRESSED = NewUncompressed()
	require.NoError(t, bloomHeader.Write(ctx, protocol))

	newBloomHeader := NewBloomFilterHeader()
	require.NoError(t, newBloomHeader.Read(ctx, protocol))
	require.Equal(t, bloomHeader.NumBytes, newBloomHeader.NumBytes)
	require.NotNil(t, newBloomHeader.Algorithm)
	require.NotNil(t, newBloomHeader.Algorithm.BLOCK)
	require.NotNil(t, newBloomHeader.Hash)
	require.NotNil(t, newBloomHeader.Hash.XXHASH)
	require.NotNil(t, newBloomHeader.Compression)
	require.NotNil(t, newBloomHeader.Compression.UNCOMPRESSED)

	transport.Reset()
	pageHeader := NewPageHeader()
	pageHeader.Type = PageType_DATA_PAGE
	pageHeader.UncompressedPageSize = 2048
	pageHeader.CompressedPageSize = 1024
	crc := int32(0x12345678)
	pageHeader.Crc = &crc
	pageHeader.DataPageHeader = NewDataPageHeader()
	pageHeader.DataPageHeader.NumValues = 1000
	pageHeader.DataPageHeader.Encoding = Encoding_PLAIN
	pageHeader.DataPageHeader.DefinitionLevelEncoding = Encoding_RLE
	pageHeader.DataPageHeader.RepetitionLevelEncoding = Encoding_RLE
	require.NoError(t, pageHeader.Write(ctx, protocol))

	newPageHeader := NewPageHeader()
	require.NoError(t, newPageHeader.Read(ctx, protocol))
	require.Equal(t, pageHeader.Type, newPageHeader.Type)
	require.Equal(t, pageHeader.UncompressedPageSize, newPageHeader.UncompressedPageSize)
	require.Equal(t, pageHeader.CompressedPageSize, newPageHeader.CompressedPageSize)
	require.NotNil(t, newPageHeader.Crc)
	require.Equal(t, *pageHeader.Crc, *newPageHeader.Crc)
	require.NotNil(t, newPageHeader.DataPageHeader)

	transport.Reset()
	dictPageHeader := NewPageHeader()
	dictPageHeader.Type = PageType_DICTIONARY_PAGE
	dictPageHeader.UncompressedPageSize = 512
	dictPageHeader.CompressedPageSize = 256
	dictPageHeader.DictionaryPageHeader = NewDictionaryPageHeader()
	dictPageHeader.DictionaryPageHeader.NumValues = 100
	dictPageHeader.DictionaryPageHeader.Encoding = Encoding_PLAIN_DICTIONARY
	require.NoError(t, dictPageHeader.Write(ctx, protocol))

	newDictPageHeader := NewPageHeader()
	require.NoError(t, newDictPageHeader.Read(ctx, protocol))
	require.Equal(t, dictPageHeader.Type, newDictPageHeader.Type)
	require.NotNil(t, newDictPageHeader.DictionaryPageHeader)

	transport.Reset()
	v2PageHeader := NewPageHeader()
	v2PageHeader.Type = PageType_DATA_PAGE_V2
	v2PageHeader.UncompressedPageSize = 4096
	v2PageHeader.CompressedPageSize = 2048
	v2PageHeader.DataPageHeaderV2 = NewDataPageHeaderV2()
	v2PageHeader.DataPageHeaderV2.NumValues = 2000
	v2PageHeader.DataPageHeaderV2.NumRows = 1980
	v2PageHeader.DataPageHeaderV2.Encoding = Encoding_DELTA_BINARY_PACKED
	require.NoError(t, v2PageHeader.Write(ctx, protocol))

	newV2PageHeader := NewPageHeader()
	require.NoError(t, newV2PageHeader.Read(ctx, protocol))
	require.Equal(t, v2PageHeader.Type, newV2PageHeader.Type)
	require.NotNil(t, newV2PageHeader.DataPageHeaderV2)
}

func Test_ParquetFileFormatStructures(t *testing.T) {
	ctx := context.Background()
	transport := thrift.NewTMemoryBuffer()
	protocol := thrift.NewTBinaryProtocolConf(transport, nil)

	sortCol := NewSortingColumn()
	sortCol.ColumnIdx = 3
	sortCol.Descending = true
	sortCol.NullsFirst = false
	require.NoError(t, sortCol.Write(ctx, protocol))

	newSortCol := NewSortingColumn()
	require.NoError(t, newSortCol.Read(ctx, protocol))
	require.Equal(t, sortCol.ColumnIdx, newSortCol.ColumnIdx)
	require.Equal(t, sortCol.Descending, newSortCol.Descending)
	require.Equal(t, sortCol.NullsFirst, newSortCol.NullsFirst)

	transport.Reset()
	colMeta := NewColumnMetaData()
	colMeta.Type = Type_INT32
	colMeta.Encodings = []Encoding{Encoding_PLAIN, Encoding_RLE}
	colMeta.PathInSchema = []string{"root", "field1"}
	colMeta.Codec = CompressionCodec_SNAPPY
	colMeta.NumValues = 10000
	colMeta.TotalUncompressedSize = 40000
	colMeta.TotalCompressedSize = 20000
	keyValueMeta := []*KeyValue{
		{Key: "test_key", Value: thrift.StringPtr("test_value")},
	}
	colMeta.KeyValueMetadata = keyValueMeta
	colMeta.DataPageOffset = 1024
	indexPageOffset := int64(2048)
	colMeta.IndexPageOffset = &indexPageOffset
	dictPageOffset := int64(512)
	colMeta.DictionaryPageOffset = &dictPageOffset
	colMeta.Statistics = NewStatistics()
	colMeta.Statistics.Max = []byte("max_col")
	colMeta.Statistics.Min = []byte("min_col")
	require.NoError(t, colMeta.Write(ctx, protocol))

	newColMeta := NewColumnMetaData()
	require.NoError(t, newColMeta.Read(ctx, protocol))
	require.Equal(t, colMeta.Type, newColMeta.Type)
	require.Equal(t, len(colMeta.Encodings), len(newColMeta.Encodings))
	require.Equal(t, len(colMeta.PathInSchema), len(newColMeta.PathInSchema))
	require.Equal(t, colMeta.Codec, newColMeta.Codec)
	require.Equal(t, colMeta.NumValues, newColMeta.NumValues)
	require.Equal(t, colMeta.DataPageOffset, newColMeta.DataPageOffset)
	require.NotNil(t, newColMeta.Statistics)

	transport.Reset()
	kv := &KeyValue{}
	kv.Key = "metadata_key"
	kv.Value = thrift.StringPtr("metadata_value")
	require.NoError(t, kv.Write(ctx, protocol))

	newKV := &KeyValue{}
	require.NoError(t, newKV.Read(ctx, protocol))
	require.Equal(t, kv.Key, newKV.Key)
	require.NotNil(t, newKV.Value)
	require.Equal(t, *kv.Value, *newKV.Value)

	transport.Reset()
	encFooter := NewEncryptionWithFooterKey()
	require.NoError(t, encFooter.Write(ctx, protocol))

	newEncFooter := NewEncryptionWithFooterKey()
	require.NoError(t, newEncFooter.Read(ctx, protocol))
	require.True(t, encFooter.Equals(newEncFooter))

	transport.Reset()
	encCol := NewEncryptionWithColumnKey()
	encCol.PathInSchema = []string{"root", "encrypted_field"}
	encCol.KeyMetadata = []byte("key_metadata_bytes")
	require.NoError(t, encCol.Write(ctx, protocol))

	newEncCol := NewEncryptionWithColumnKey()
	require.NoError(t, newEncCol.Read(ctx, protocol))
	require.Equal(t, len(encCol.PathInSchema), len(newEncCol.PathInSchema))
	require.True(t, bytes.Equal(newEncCol.KeyMetadata, encCol.KeyMetadata))

	transport.Reset()
	cryptoMeta := NewColumnCryptoMetaData()
	cryptoMeta.ENCRYPTION_WITH_FOOTER_KEY = NewEncryptionWithFooterKey()
	require.NoError(t, cryptoMeta.Write(ctx, protocol))

	newCryptoMeta := NewColumnCryptoMetaData()
	require.NoError(t, newCryptoMeta.Read(ctx, protocol))
	require.True(t, cryptoMeta.Equals(newCryptoMeta))
	require.NotNil(t, newCryptoMeta.ENCRYPTION_WITH_FOOTER_KEY)

	cryptoCount := cryptoMeta.CountSetFieldsColumnCryptoMetaData()
	require.Equal(t, 1, cryptoCount)

	transport.Reset()
	colChunk := NewColumnChunk()
	filePath := "test_file.parquet"
	colChunk.FilePath = &filePath
	colChunk.FileOffset = 1024
	colChunk.MetaData = NewColumnMetaData()
	colChunk.MetaData.Type = Type_BYTE_ARRAY
	colChunk.MetaData.Encodings = []Encoding{Encoding_PLAIN}
	colChunk.MetaData.PathInSchema = []string{"root", "chunk_field"}
	colChunk.MetaData.Codec = CompressionCodec_GZIP
	colChunk.MetaData.NumValues = 5000
	colChunk.MetaData.DataPageOffset = 2048
	colIndexOffset := int64(4096)
	colChunk.ColumnIndexOffset = &colIndexOffset
	colIndexLength := int32(256)
	colChunk.ColumnIndexLength = &colIndexLength
	colChunk.CryptoMetadata = NewColumnCryptoMetaData()
	colChunk.CryptoMetadata.ENCRYPTION_WITH_FOOTER_KEY = NewEncryptionWithFooterKey()
	colChunk.EncryptedColumnMetadata = []byte("encrypted_metadata")
	require.NoError(t, colChunk.Write(ctx, protocol))

	newColChunk := NewColumnChunk()
	require.NoError(t, newColChunk.Read(ctx, protocol))
	require.NotNil(t, newColChunk.FilePath)
	require.Equal(t, *colChunk.FilePath, *newColChunk.FilePath)
	require.Equal(t, colChunk.FileOffset, newColChunk.FileOffset)
	require.NotNil(t, newColChunk.MetaData)
	require.NotNil(t, newColChunk.ColumnIndexOffset)
	require.Equal(t, *colChunk.ColumnIndexOffset, *newColChunk.ColumnIndexOffset)
	require.NotNil(t, newColChunk.CryptoMetadata)
}

func Test_TopLevelParquetStructures(t *testing.T) {
	ctx := context.Background()
	transport := thrift.NewTMemoryBuffer()
	protocol := thrift.NewTBinaryProtocolConf(transport, nil)

	rowGroup := NewRowGroup()
	rowGroup.Columns = []*ColumnChunk{
		{
			FileOffset: 1024,
			MetaData: &ColumnMetaData{
				Type:           Type_INT32,
				Encodings:      []Encoding{Encoding_PLAIN},
				PathInSchema:   []string{"root", "id"},
				Codec:          CompressionCodec_SNAPPY,
				NumValues:      1000,
				DataPageOffset: 2048,
			},
		},
		{
			FileOffset: 4096,
			MetaData: &ColumnMetaData{
				Type:           Type_BYTE_ARRAY,
				Encodings:      []Encoding{Encoding_PLAIN},
				PathInSchema:   []string{"root", "name"},
				Codec:          CompressionCodec_GZIP,
				NumValues:      1000,
				DataPageOffset: 8192,
			},
		},
	}
	rowGroup.TotalByteSize = 100000
	rowGroup.NumRows = 1000
	fileOffset := int64(16384)
	rowGroup.FileOffset = &fileOffset
	totalCompressedSize := int64(50000)
	rowGroup.TotalCompressedSize = &totalCompressedSize
	ordinal := int16(0)
	rowGroup.Ordinal = &ordinal
	require.NoError(t, rowGroup.Write(ctx, protocol))

	newRowGroup := NewRowGroup()
	require.NoError(t, newRowGroup.Read(ctx, protocol))
	require.Equal(t, len(rowGroup.Columns), len(newRowGroup.Columns))
	require.Equal(t, rowGroup.TotalByteSize, newRowGroup.TotalByteSize)
	require.Equal(t, rowGroup.NumRows, newRowGroup.NumRows)
	require.NotNil(t, newRowGroup.FileOffset)
	require.Equal(t, *rowGroup.FileOffset, *newRowGroup.FileOffset)

	transport.Reset()
	fileMetadata := NewFileMetaData()
	fileMetadata.Version = 2
	fileMetadata.Schema = []*SchemaElement{
		{
			Name:        "root",
			NumChildren: thrift.Int32Ptr(2),
		},
		{
			Name:           "id",
			Type:           &[]Type{Type_INT32}[0],
			RepetitionType: &[]FieldRepetitionType{FieldRepetitionType_REQUIRED}[0],
		},
		{
			Name:           "name",
			Type:           &[]Type{Type_BYTE_ARRAY}[0],
			RepetitionType: &[]FieldRepetitionType{FieldRepetitionType_OPTIONAL}[0],
			LogicalType:    &LogicalType{STRING: NewStringType()},
		},
	}
	fileMetadata.NumRows = 10000
	fileMetadata.RowGroups = []*RowGroup{rowGroup}
	fileMetadata.KeyValueMetadata = []*KeyValue{
		{Key: "created_by", Value: thrift.StringPtr("parquet-go test")},
		{Key: "version", Value: thrift.StringPtr("1.0")},
	}
	createdBy := "parquet-go test suite"
	fileMetadata.CreatedBy = &createdBy
	columnOrders := []*ColumnOrder{
		{TYPE_ORDER: NewTypeDefinedOrder()},
		{TYPE_ORDER: NewTypeDefinedOrder()},
	}
	fileMetadata.ColumnOrders = columnOrders
	require.NoError(t, fileMetadata.Write(ctx, protocol))

	newFileMetadata := NewFileMetaData()
	require.NoError(t, newFileMetadata.Read(ctx, protocol))
	require.Equal(t, fileMetadata.Version, newFileMetadata.Version)
	require.Equal(t, len(fileMetadata.Schema), len(newFileMetadata.Schema))
	require.Equal(t, fileMetadata.NumRows, newFileMetadata.NumRows)
	require.Equal(t, len(fileMetadata.RowGroups), len(newFileMetadata.RowGroups))
	require.Equal(t, len(fileMetadata.KeyValueMetadata), len(newFileMetadata.KeyValueMetadata))
	require.NotNil(t, newFileMetadata.CreatedBy)
	require.Equal(t, *fileMetadata.CreatedBy, *newFileMetadata.CreatedBy)

	transport.Reset()
	typeOrder := NewTypeDefinedOrder()
	require.NoError(t, typeOrder.Write(ctx, protocol))

	newTypeOrder := NewTypeDefinedOrder()
	require.NoError(t, newTypeOrder.Read(ctx, protocol))
	require.True(t, typeOrder.Equals(newTypeOrder))

	transport.Reset()
	colOrder := NewColumnOrder()
	colOrder.TYPE_ORDER = NewTypeDefinedOrder()
	require.NoError(t, colOrder.Write(ctx, protocol))

	newColOrder := NewColumnOrder()
	require.NoError(t, newColOrder.Read(ctx, protocol))
	require.True(t, colOrder.Equals(newColOrder))
	require.NotNil(t, newColOrder.TYPE_ORDER)

	transport.Reset()
	fileCrypto := NewFileCryptoMetaData()
	fileCrypto.EncryptionAlgorithm = NewEncryptionAlgorithm()
	fileCrypto.EncryptionAlgorithm.AES_GCM_V1 = NewAesGcmV1()
	fileCrypto.KeyMetadata = []byte("file_key_metadata")
	require.NoError(t, fileCrypto.Write(ctx, protocol))

	newFileCrypto := NewFileCryptoMetaData()
	require.NoError(t, newFileCrypto.Read(ctx, protocol))
	require.True(t, fileCrypto.EncryptionAlgorithm.Equals(newFileCrypto.EncryptionAlgorithm))
	require.True(t, bytes.Equal(newFileCrypto.KeyMetadata, fileCrypto.KeyMetadata))
}

func Test_IndexingAndStatisticalStructures(t *testing.T) {
	ctx := context.Background()
	transport := thrift.NewTMemoryBuffer()
	protocol := thrift.NewTBinaryProtocolConf(transport, nil)

	pageEncStats := NewPageEncodingStats()
	pageEncStats.PageType = PageType_DATA_PAGE
	pageEncStats.Encoding = Encoding_PLAIN
	pageEncStats.Count = 1000
	require.NoError(t, pageEncStats.Write(ctx, protocol))

	newPageEncStats := NewPageEncodingStats()
	require.NoError(t, newPageEncStats.Read(ctx, protocol))
	require.Equal(t, pageEncStats.PageType, newPageEncStats.PageType)
	require.Equal(t, pageEncStats.Encoding, newPageEncStats.Encoding)
	require.Equal(t, pageEncStats.Count, newPageEncStats.Count)

	transport.Reset()
	pageLocation := NewPageLocation()
	pageLocation.Offset = 4096
	pageLocation.CompressedPageSize = 1024
	pageLocation.FirstRowIndex = 500
	require.NoError(t, pageLocation.Write(ctx, protocol))

	newPageLocation := NewPageLocation()
	require.NoError(t, newPageLocation.Read(ctx, protocol))
	require.Equal(t, pageLocation.Offset, newPageLocation.Offset)
	require.Equal(t, pageLocation.CompressedPageSize, newPageLocation.CompressedPageSize)
	require.Equal(t, pageLocation.FirstRowIndex, newPageLocation.FirstRowIndex)

	transport.Reset()
	offsetIndex := NewOffsetIndex()
	offsetIndex.PageLocations = []*PageLocation{
		pageLocation,
		{
			Offset:             8192,
			CompressedPageSize: 2048,
			FirstRowIndex:      1000,
		},
		{
			Offset:             12288,
			CompressedPageSize: 1536,
			FirstRowIndex:      1500,
		},
	}
	require.NoError(t, offsetIndex.Write(ctx, protocol))

	newOffsetIndex := NewOffsetIndex()
	require.NoError(t, newOffsetIndex.Read(ctx, protocol))
	require.Equal(t, len(offsetIndex.PageLocations), len(newOffsetIndex.PageLocations))
	require.True(t, len(newOffsetIndex.PageLocations) > 0)
	require.Equal(t, offsetIndex.PageLocations[0].Offset, newOffsetIndex.PageLocations[0].Offset)

	transport.Reset()
	columnIndex := NewColumnIndex()
	columnIndex.NullPages = []bool{false, false, true, false}
	columnIndex.MinValues = [][]byte{
		[]byte("min1"),
		[]byte("min2"),
		nil, // null page
		[]byte("min4"),
	}
	columnIndex.MaxValues = [][]byte{
		[]byte("max1"),
		[]byte("max2"),
		nil, // null page
		[]byte("max4"),
	}
	columnIndex.BoundaryOrder = BoundaryOrder_ASCENDING
	columnIndex.NullCounts = []int64{0, 5, 1000, 10}
	require.NoError(t, columnIndex.Write(ctx, protocol))

	newColumnIndex := NewColumnIndex()
	require.NoError(t, newColumnIndex.Read(ctx, protocol))
	require.Equal(t, len(columnIndex.NullPages), len(newColumnIndex.NullPages))
	require.Equal(t, len(columnIndex.MinValues), len(newColumnIndex.MinValues))
	require.Equal(t, columnIndex.BoundaryOrder, newColumnIndex.BoundaryOrder)
	require.Equal(t, len(columnIndex.NullCounts), len(newColumnIndex.NullCounts))

	transport.Reset()
	aesGcm := NewAesGcmV1()
	aesGcm.AadPrefix = []byte("aad_prefix_data")
	aesGcm.AadFileUnique = []byte("aad_file_unique")
	supplyAadPrefix := false
	aesGcm.SupplyAadPrefix = &supplyAadPrefix
	require.NoError(t, aesGcm.Write(ctx, protocol))

	newAesGcm := NewAesGcmV1()
	require.NoError(t, newAesGcm.Read(ctx, protocol))
	require.True(t, aesGcm.Equals(newAesGcm))
	require.True(t, bytes.Equal(newAesGcm.AadPrefix, aesGcm.AadPrefix))
	require.NotNil(t, newAesGcm.SupplyAadPrefix)
	require.Equal(t, *aesGcm.SupplyAadPrefix, *newAesGcm.SupplyAadPrefix)

	transport.Reset()
	aesGcmCtr := NewAesGcmCtrV1()
	aesGcmCtr.AadPrefix = []byte("ctr_aad_prefix")
	aesGcmCtr.AadFileUnique = []byte("ctr_aad_file_unique")
	supplyAadPrefix2 := true
	aesGcmCtr.SupplyAadPrefix = &supplyAadPrefix2
	require.NoError(t, aesGcmCtr.Write(ctx, protocol))

	newAesGcmCtr := NewAesGcmCtrV1()
	require.NoError(t, newAesGcmCtr.Read(ctx, protocol))
	require.True(t, aesGcmCtr.Equals(newAesGcmCtr))
	require.True(t, bytes.Equal(newAesGcmCtr.AadPrefix, aesGcmCtr.AadPrefix))
	require.NotNil(t, newAesGcmCtr.SupplyAadPrefix)
	require.Equal(t, *aesGcmCtr.SupplyAadPrefix, *newAesGcmCtr.SupplyAadPrefix)

	transport.Reset()
	encAlgo := NewEncryptionAlgorithm()
	encAlgo.AES_GCM_CTR_V1 = aesGcmCtr
	require.NoError(t, encAlgo.Write(ctx, protocol))

	newEncAlgo := NewEncryptionAlgorithm()
	require.NoError(t, newEncAlgo.Read(ctx, protocol))
	require.True(t, encAlgo.Equals(newEncAlgo))
	require.NotNil(t, newEncAlgo.AES_GCM_CTR_V1)
}

func Test_UnionAndComplexStructures(t *testing.T) {
	ctx := context.Background()
	transport := thrift.NewTMemoryBuffer()
	protocol := thrift.NewTBinaryProtocolConf(transport, nil)

	logicalType := NewLogicalType()
	logicalType.INTEGER = NewIntType()
	logicalType.INTEGER.BitWidth = 64
	logicalType.INTEGER.IsSigned = false
	require.NoError(t, logicalType.Write(ctx, protocol))

	newLogicalType := NewLogicalType()
	require.NoError(t, newLogicalType.Read(ctx, protocol))
	require.True(t, logicalType.Equals(newLogicalType))

	transport.Reset()
	logicalType2 := NewLogicalType()
	logicalType2.UNKNOWN = NewNullType()
	require.NoError(t, logicalType2.Write(ctx, protocol))

	newLogicalType2 := NewLogicalType()
	require.NoError(t, newLogicalType2.Read(ctx, protocol))
	require.True(t, logicalType2.Equals(newLogicalType2))

	transport.Reset()
	logicalType3 := NewLogicalType()
	logicalType3.JSON = NewJsonType()
	require.NoError(t, logicalType3.Write(ctx, protocol))

	newLogicalType3 := NewLogicalType()
	require.NoError(t, newLogicalType3.Read(ctx, protocol))
	require.True(t, logicalType3.Equals(newLogicalType3))

	transport.Reset()
	logicalType4 := NewLogicalType()
	logicalType4.BSON = NewBsonType()
	require.NoError(t, logicalType4.Write(ctx, protocol))

	newLogicalType4 := NewLogicalType()
	require.NoError(t, newLogicalType4.Read(ctx, protocol))
	require.True(t, logicalType4.Equals(newLogicalType4))

	transport.Reset()
	logicalType5 := NewLogicalType()
	logicalType5.UUID = NewUUIDType()
	require.NoError(t, logicalType5.Write(ctx, protocol))

	newLogicalType5 := NewLogicalType()
	require.NoError(t, newLogicalType5.Read(ctx, protocol))
	require.True(t, logicalType5.Equals(newLogicalType5))

	transport.Reset()
	cryptoMeta2 := NewColumnCryptoMetaData()
	cryptoMeta2.ENCRYPTION_WITH_COLUMN_KEY = NewEncryptionWithColumnKey()
	cryptoMeta2.ENCRYPTION_WITH_COLUMN_KEY.PathInSchema = []string{"root", "encrypted_col"}
	cryptoMeta2.ENCRYPTION_WITH_COLUMN_KEY.KeyMetadata = []byte("column_key_meta")
	require.NoError(t, cryptoMeta2.Write(ctx, protocol))

	newCryptoMeta2 := NewColumnCryptoMetaData()
	require.NoError(t, newCryptoMeta2.Read(ctx, protocol))
	require.True(t, cryptoMeta2.Equals(newCryptoMeta2))

	transport.Reset()
	indexPageHeader := NewPageHeader()
	indexPageHeader.Type = PageType_INDEX_PAGE
	indexPageHeader.UncompressedPageSize = 512
	indexPageHeader.CompressedPageSize = 256
	indexPageHeader.IndexPageHeader = NewIndexPageHeader()
	require.NoError(t, indexPageHeader.Write(ctx, protocol))

	newIndexPageHeader := NewPageHeader()
	require.NoError(t, newIndexPageHeader.Read(ctx, protocol))
	require.Equal(t, indexPageHeader.Type, newIndexPageHeader.Type)
	require.NotNil(t, newIndexPageHeader.IndexPageHeader)

	transport.Reset()
	complexFileMetadata := NewFileMetaData()
	complexFileMetadata.Version = 2
	complexFileMetadata.Schema = []*SchemaElement{
		{
			Name:        "root",
			NumChildren: thrift.Int32Ptr(3),
		},
		{
			Name:           "id",
			Type:           &[]Type{Type_INT64}[0],
			RepetitionType: &[]FieldRepetitionType{FieldRepetitionType_REQUIRED}[0],
		},
		{
			Name:           "timestamp",
			Type:           &[]Type{Type_INT64}[0],
			RepetitionType: &[]FieldRepetitionType{FieldRepetitionType_REQUIRED}[0],
			LogicalType: &LogicalType{
				TIMESTAMP: &TimestampType{
					IsAdjustedToUTC: true,
					Unit:            &TimeUnit{MICROS: NewMicroSeconds()},
				},
			},
		},
		{
			Name:           "data",
			Type:           &[]Type{Type_BYTE_ARRAY}[0],
			RepetitionType: &[]FieldRepetitionType{FieldRepetitionType_OPTIONAL}[0],
			LogicalType:    &LogicalType{JSON: NewJsonType()},
		},
	}
	complexFileMetadata.NumRows = 1000000
	complexFileMetadata.RowGroups = []*RowGroup{
		{
			Columns: []*ColumnChunk{
				{
					FileOffset: 1024,
					MetaData: &ColumnMetaData{
						Type:           Type_INT64,
						Encodings:      []Encoding{Encoding_DELTA_BINARY_PACKED},
						PathInSchema:   []string{"root", "id"},
						Codec:          CompressionCodec_SNAPPY,
						NumValues:      1000000,
						DataPageOffset: 2048,
						Statistics: &Statistics{
							Min: []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01},
							Max: []byte{0x00, 0x0F, 0x42, 0x40, 0x00, 0x00, 0x00, 0x00},
						},
					},
				},
			},
			TotalByteSize: 50000000,
			NumRows:       1000000,
		},
	}
	complexFileMetadata.KeyValueMetadata = []*KeyValue{
		{Key: "writer.model.name", Value: thrift.StringPtr("parquet-go")},
		{Key: "writer.version", Value: thrift.StringPtr("v2.0")},
		{Key: "pandas.version", Value: thrift.StringPtr("1.0.0")},
	}
	complexFileMetadata.CreatedBy = thrift.StringPtr("parquet-go comprehensive test")
	complexFileMetadata.ColumnOrders = []*ColumnOrder{
		{TYPE_ORDER: NewTypeDefinedOrder()},
		{TYPE_ORDER: NewTypeDefinedOrder()},
		{TYPE_ORDER: NewTypeDefinedOrder()},
	}
	encryptionAlgo := NewEncryptionAlgorithm()
	encryptionAlgo.AES_GCM_V1 = NewAesGcmV1()
	encryptionAlgo.AES_GCM_V1.AadPrefix = []byte("file_aad")
	complexFileMetadata.EncryptionAlgorithm = encryptionAlgo
	complexFileMetadata.FooterSigningKeyMetadata = []byte("footer_signing_key")
	require.NoError(t, complexFileMetadata.Write(ctx, protocol))

	newComplexFileMetadata := NewFileMetaData()
	require.NoError(t, newComplexFileMetadata.Read(ctx, protocol))
	require.Equal(t, complexFileMetadata.Version, newComplexFileMetadata.Version)
	require.Equal(t, len(complexFileMetadata.Schema), len(newComplexFileMetadata.Schema))
	require.NotNil(t, newComplexFileMetadata.EncryptionAlgorithm)
	require.True(t, bytes.Equal(newComplexFileMetadata.FooterSigningKeyMetadata, complexFileMetadata.FooterSigningKeyMetadata))
}

func Test_EqualsMethod(t *testing.T) {
	tests := []struct {
		name        string
		setupStats1 func() *Statistics
		setupStats2 func() *Statistics
		expected    bool
	}{
		{
			"both empty",
			func() *Statistics { return NewStatistics() },
			func() *Statistics { return NewStatistics() },
			true,
		},
		{
			"same Max",
			func() *Statistics { s := NewStatistics(); s.Max = []byte("test"); return s },
			func() *Statistics { s := NewStatistics(); s.Max = []byte("test"); return s },
			true,
		},
		{
			"same Min and Max",
			func() *Statistics {
				s := NewStatistics()
				s.Max = []byte("test")
				s.Min = []byte("min")
				return s
			},
			func() *Statistics {
				s := NewStatistics()
				s.Max = []byte("test")
				s.Min = []byte("min")
				return s
			},
			true,
		},
		{
			"same NullCount",
			func() *Statistics {
				s := NewStatistics()
				s.Max = []byte("test")
				s.Min = []byte("min")
				nc := int64(10)
				s.NullCount = &nc
				return s
			},
			func() *Statistics {
				s := NewStatistics()
				s.Max = []byte("test")
				s.Min = []byte("min")
				nc := int64(10)
				s.NullCount = &nc
				return s
			},
			true,
		},
		{
			"same DistinctCount",
			func() *Statistics {
				s := NewStatistics()
				s.Max = []byte("test")
				s.Min = []byte("min")
				nc := int64(10)
				s.NullCount = &nc
				dc := int64(100)
				s.DistinctCount = &dc
				return s
			},
			func() *Statistics {
				s := NewStatistics()
				s.Max = []byte("test")
				s.Min = []byte("min")
				nc := int64(10)
				s.NullCount = &nc
				dc := int64(100)
				s.DistinctCount = &dc
				return s
			},
			true,
		},
		{
			"same MaxValue and MinValue",
			func() *Statistics {
				s := NewStatistics()
				s.Max = []byte("test")
				s.Min = []byte("min")
				nc := int64(10)
				s.NullCount = &nc
				dc := int64(100)
				s.DistinctCount = &dc
				s.MaxValue = []byte("maxval")
				s.MinValue = []byte("minval")
				return s
			},
			func() *Statistics {
				s := NewStatistics()
				s.Max = []byte("test")
				s.Min = []byte("min")
				nc := int64(10)
				s.NullCount = &nc
				dc := int64(100)
				s.DistinctCount = &dc
				s.MaxValue = []byte("maxval")
				s.MinValue = []byte("minval")
				return s
			},
			true,
		},
		{
			"Max vs nil",
			func() *Statistics { s := NewStatistics(); s.Max = []byte("test"); return s },
			func() *Statistics { return NewStatistics() },
			false,
		},
		{
			"different Max contents",
			func() *Statistics { s := NewStatistics(); s.Max = []byte("different1"); return s },
			func() *Statistics { s := NewStatistics(); s.Max = []byte("different2"); return s },
			false,
		},
		{
			"different NullCount",
			func() *Statistics {
				s := NewStatistics()
				nc := int64(5)
				s.NullCount = &nc
				return s
			},
			func() *Statistics {
				s := NewStatistics()
				nc := int64(10)
				s.NullCount = &nc
				return s
			},
			false,
		},
		{
			"different DistinctCount",
			func() *Statistics {
				s := NewStatistics()
				dc := int64(50)
				s.DistinctCount = &dc
				return s
			},
			func() *Statistics {
				s := NewStatistics()
				dc := int64(100)
				s.DistinctCount = &dc
				return s
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats1 := tt.setupStats1()
			stats2 := tt.setupStats2()
			result := stats1.Equals(stats2)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_DecimalTypeEquals(t *testing.T) {
	tests := []struct {
		name     string
		setupDT1 func() *DecimalType
		setupDT2 func() *DecimalType
		expected bool
	}{
		{
			"default values",
			func() *DecimalType { return NewDecimalType() },
			func() *DecimalType { return NewDecimalType() },
			true,
		},
		{
			"same scale",
			func() *DecimalType { dt := NewDecimalType(); dt.Scale = 2; return dt },
			func() *DecimalType { dt := NewDecimalType(); dt.Scale = 2; return dt },
			true,
		},
		{
			"same precision",
			func() *DecimalType { dt := NewDecimalType(); dt.Scale = 2; dt.Precision = 10; return dt },
			func() *DecimalType { dt := NewDecimalType(); dt.Scale = 2; dt.Precision = 10; return dt },
			true,
		},
		{
			"different scale",
			func() *DecimalType { dt := NewDecimalType(); dt.Scale = 2; return dt },
			func() *DecimalType { dt := NewDecimalType(); dt.Scale = 4; return dt },
			false,
		},
		{
			"different precision",
			func() *DecimalType { dt := NewDecimalType(); dt.Precision = 10; return dt },
			func() *DecimalType { dt := NewDecimalType(); dt.Precision = 20; return dt },
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt1 := tt.setupDT1()
			dt2 := tt.setupDT2()
			result := dt1.Equals(dt2)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_TimeUnitEquals(t *testing.T) {
	tests := []struct {
		name     string
		setupTU1 func() *TimeUnit
		setupTU2 func() *TimeUnit
		expected bool
	}{
		{
			"both empty",
			func() *TimeUnit { return NewTimeUnit() },
			func() *TimeUnit { return NewTimeUnit() },
			true,
		},
		{
			"same MILLIS",
			func() *TimeUnit { tu := NewTimeUnit(); tu.MILLIS = NewMilliSeconds(); return tu },
			func() *TimeUnit { tu := NewTimeUnit(); tu.MILLIS = NewMilliSeconds(); return tu },
			true,
		},
		{
			"same MICROS",
			func() *TimeUnit { tu := NewTimeUnit(); tu.MICROS = NewMicroSeconds(); return tu },
			func() *TimeUnit { tu := NewTimeUnit(); tu.MICROS = NewMicroSeconds(); return tu },
			true,
		},
		{
			"same NANOS",
			func() *TimeUnit { tu := NewTimeUnit(); tu.NANOS = NewNanoSeconds(); return tu },
			func() *TimeUnit { tu := NewTimeUnit(); tu.NANOS = NewNanoSeconds(); return tu },
			true,
		},
		{
			"MILLIS vs MICROS",
			func() *TimeUnit { tu := NewTimeUnit(); tu.MILLIS = NewMilliSeconds(); return tu },
			func() *TimeUnit { tu := NewTimeUnit(); tu.MICROS = NewMicroSeconds(); return tu },
			false,
		},
		{
			"MICROS vs NANOS",
			func() *TimeUnit { tu := NewTimeUnit(); tu.MICROS = NewMicroSeconds(); return tu },
			func() *TimeUnit { tu := NewTimeUnit(); tu.NANOS = NewNanoSeconds(); return tu },
			false,
		},
		{
			"set vs unset",
			func() *TimeUnit { tu := NewTimeUnit(); tu.MILLIS = NewMilliSeconds(); return tu },
			func() *TimeUnit { return NewTimeUnit() },
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tu1 := tt.setupTU1()
			tu2 := tt.setupTU2()
			result := tu1.Equals(tu2)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_TimestampTypeEquals(t *testing.T) {
	tests := []struct {
		name     string
		setupTT1 func() *TimestampType
		setupTT2 func() *TimestampType
		expected bool
	}{
		{
			"default values",
			func() *TimestampType { return NewTimestampType() },
			func() *TimestampType { return NewTimestampType() },
			true,
		},
		{
			"same IsAdjustedToUTC",
			func() *TimestampType { tt := NewTimestampType(); tt.IsAdjustedToUTC = true; return tt },
			func() *TimestampType { tt := NewTimestampType(); tt.IsAdjustedToUTC = true; return tt },
			true,
		},
		{
			"same Unit with MILLIS",
			func() *TimestampType {
				tt := NewTimestampType()
				tt.IsAdjustedToUTC = true
				unit := NewTimeUnit()
				unit.MILLIS = NewMilliSeconds()
				tt.Unit = unit
				return tt
			},
			func() *TimestampType {
				tt := NewTimestampType()
				tt.IsAdjustedToUTC = true
				unit := NewTimeUnit()
				unit.MILLIS = NewMilliSeconds()
				tt.Unit = unit
				return tt
			},
			true,
		},
		{
			"different IsAdjustedToUTC",
			func() *TimestampType { tt := NewTimestampType(); tt.IsAdjustedToUTC = true; return tt },
			func() *TimestampType { tt := NewTimestampType(); tt.IsAdjustedToUTC = false; return tt },
			false,
		},
		{
			"different Unit types",
			func() *TimestampType {
				tt := NewTimestampType()
				unit := NewTimeUnit()
				unit.MILLIS = NewMilliSeconds()
				tt.Unit = unit
				return tt
			},
			func() *TimestampType {
				tt := NewTimestampType()
				unit := NewTimeUnit()
				unit.MICROS = NewMicroSeconds()
				tt.Unit = unit
				return tt
			},
			false,
		},
		{
			"nil vs non-nil Unit",
			func() *TimestampType { tt := NewTimestampType(); tt.Unit = NewTimeUnit(); return tt },
			func() *TimestampType { return NewTimestampType() },
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt1 := tt.setupTT1()
			tt2 := tt.setupTT2()
			result := tt1.Equals(tt2)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_TimeTypeEquals(t *testing.T) {
	tests := []struct {
		name        string
		setupTimeT1 func() *TimeType
		setupTimeT2 func() *TimeType
		expected    bool
	}{
		{
			"default values",
			func() *TimeType { return NewTimeType() },
			func() *TimeType { return NewTimeType() },
			true,
		},
		{
			"same IsAdjustedToUTC",
			func() *TimeType { tt := NewTimeType(); tt.IsAdjustedToUTC = true; return tt },
			func() *TimeType { tt := NewTimeType(); tt.IsAdjustedToUTC = true; return tt },
			true,
		},
		{
			"same Unit with MICROS",
			func() *TimeType {
				tt := NewTimeType()
				tt.IsAdjustedToUTC = true
				unit := NewTimeUnit()
				unit.MICROS = NewMicroSeconds()
				tt.Unit = unit
				return tt
			},
			func() *TimeType {
				tt := NewTimeType()
				tt.IsAdjustedToUTC = true
				unit := NewTimeUnit()
				unit.MICROS = NewMicroSeconds()
				tt.Unit = unit
				return tt
			},
			true,
		},
		{
			"different IsAdjustedToUTC",
			func() *TimeType { tt := NewTimeType(); tt.IsAdjustedToUTC = true; return tt },
			func() *TimeType { tt := NewTimeType(); tt.IsAdjustedToUTC = false; return tt },
			false,
		},
		{
			"different Unit types",
			func() *TimeType {
				tt := NewTimeType()
				unit := NewTimeUnit()
				unit.MICROS = NewMicroSeconds()
				tt.Unit = unit
				return tt
			},
			func() *TimeType {
				tt := NewTimeType()
				unit := NewTimeUnit()
				unit.NANOS = NewNanoSeconds()
				tt.Unit = unit
				return tt
			},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			timeType1 := tt.setupTimeT1()
			timeType2 := tt.setupTimeT2()
			result := timeType1.Equals(timeType2)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_IntTypeEquals(t *testing.T) {
	tests := []struct {
		name     string
		setupIT1 func() *IntType
		setupIT2 func() *IntType
		expected bool
	}{
		{
			"default values",
			func() *IntType { return NewIntType() },
			func() *IntType { return NewIntType() },
			true,
		},
		{
			"same BitWidth",
			func() *IntType { it := NewIntType(); it.BitWidth = 32; return it },
			func() *IntType { it := NewIntType(); it.BitWidth = 32; return it },
			true,
		},
		{
			"same IsSigned",
			func() *IntType { it := NewIntType(); it.BitWidth = 32; it.IsSigned = true; return it },
			func() *IntType { it := NewIntType(); it.BitWidth = 32; it.IsSigned = true; return it },
			true,
		},
		{
			"different BitWidth",
			func() *IntType { it := NewIntType(); it.BitWidth = 32; return it },
			func() *IntType { it := NewIntType(); it.BitWidth = 64; return it },
			false,
		},
		{
			"different IsSigned",
			func() *IntType { it := NewIntType(); it.IsSigned = true; return it },
			func() *IntType { it := NewIntType(); it.IsSigned = false; return it },
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			it1 := tt.setupIT1()
			it2 := tt.setupIT2()
			result := it1.Equals(it2)
			require.Equal(t, tt.expected, result)
		})
	}
}

func Test_VariousString(t *testing.T) {
	tests := []struct {
		name        string
		setupObj    func() interface{ String() string }
		description string
	}{
		{
			"Statistics empty",
			func() interface{ String() string } { return NewStatistics() },
			"should not return empty string",
		},
		{
			"Statistics with Max",
			func() interface{ String() string } {
				s := NewStatistics()
				s.Max = []byte("maxvalue")
				return s
			},
			"should include Max field",
		},
		{
			"Statistics with Max and Min",
			func() interface{ String() string } {
				s := NewStatistics()
				s.Max = []byte("maxvalue")
				s.Min = []byte("minvalue")
				return s
			},
			"should include Max and Min fields",
		},
		{
			"Statistics with NullCount",
			func() interface{ String() string } {
				s := NewStatistics()
				s.Max = []byte("maxvalue")
				s.Min = []byte("minvalue")
				nc := int64(5)
				s.NullCount = &nc
				return s
			},
			"should include NullCount field",
		},
		{
			"Statistics complete",
			func() interface{ String() string } {
				s := NewStatistics()
				s.Max = []byte("maxvalue")
				s.Min = []byte("minvalue")
				nc := int64(5)
				s.NullCount = &nc
				dc := int64(100)
				s.DistinctCount = &dc
				s.MaxValue = []byte("maxval")
				s.MinValue = []byte("minval")
				return s
			},
			"should include all fields",
		},
		{
			"TimeUnit empty",
			func() interface{ String() string } { return NewTimeUnit() },
			"should not return empty string",
		},
		{
			"TimeUnit with MILLIS",
			func() interface{ String() string } {
				tu := NewTimeUnit()
				tu.MILLIS = NewMilliSeconds()
				return tu
			},
			"should include MILLIS field",
		},
		{
			"TimeUnit with MICROS",
			func() interface{ String() string } {
				tu := NewTimeUnit()
				tu.MICROS = NewMicroSeconds()
				return tu
			},
			"should include MICROS field",
		},
		{
			"TimeUnit with NANOS",
			func() interface{ String() string } {
				tu := NewTimeUnit()
				tu.NANOS = NewNanoSeconds()
				return tu
			},
			"should include NANOS field",
		},
		{
			"TimestampType empty",
			func() interface{ String() string } { return NewTimestampType() },
			"should not return empty string",
		},
		{
			"TimestampType with IsAdjustedToUTC",
			func() interface{ String() string } {
				tt := NewTimestampType()
				tt.IsAdjustedToUTC = true
				return tt
			},
			"should include IsAdjustedToUTC field",
		},
		{
			"TimestampType complete",
			func() interface{ String() string } {
				tt := NewTimestampType()
				tt.IsAdjustedToUTC = true
				unit := NewTimeUnit()
				unit.MILLIS = NewMilliSeconds()
				tt.Unit = unit
				return tt
			},
			"should include all fields",
		},
		{
			"TimeType empty",
			func() interface{ String() string } { return NewTimeType() },
			"should not return empty string",
		},
		{
			"TimeType with IsAdjustedToUTC",
			func() interface{ String() string } {
				tt := NewTimeType()
				tt.IsAdjustedToUTC = true
				return tt
			},
			"should include IsAdjustedToUTC field",
		},
		{
			"TimeType complete",
			func() interface{ String() string } {
				tt := NewTimeType()
				tt.IsAdjustedToUTC = true
				unit := NewTimeUnit()
				unit.MICROS = NewMicroSeconds()
				tt.Unit = unit
				return tt
			},
			"should include all fields",
		},
		{
			"IntType empty",
			func() interface{ String() string } { return NewIntType() },
			"should not return empty string",
		},
		{
			"IntType with BitWidth",
			func() interface{ String() string } {
				it := NewIntType()
				it.BitWidth = 32
				return it
			},
			"should include BitWidth field",
		},
		{
			"IntType complete",
			func() interface{ String() string } {
				it := NewIntType()
				it.BitWidth = 32
				it.IsSigned = true
				return it
			},
			"should include all fields",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obj := tt.setupObj()
			result := obj.String()
			require.NotEmpty(t, result, tt.description)
		})
	}
}

func Test_ReadWithErrorHandling(t *testing.T) {
	ctx := context.Background()

	transport := thrift.NewTMemoryBuffer()
	protocol := thrift.NewTBinaryProtocolConf(transport, nil)

	logicalType1 := NewLogicalType()
	logicalType1.STRING = NewStringType()
	require.NoError(t, logicalType1.Write(ctx, protocol))

	readTransport := thrift.NewTMemoryBuffer()
	readTransport.Buffer = transport.Buffer
	readProtocol := thrift.NewTBinaryProtocolConf(readTransport, nil)

	newLogicalType1 := NewLogicalType()
	require.NoError(t, newLogicalType1.Read(ctx, readProtocol))
	require.True(t, newLogicalType1.IsSetSTRING())

	transport2 := thrift.NewTMemoryBuffer()
	protocol2 := thrift.NewTBinaryProtocolConf(transport2, nil)

	logicalType2 := NewLogicalType()
	intType := NewIntType()
	intType.BitWidth = 32
	intType.IsSigned = true
	logicalType2.INTEGER = intType
	require.NoError(t, logicalType2.Write(ctx, protocol2))

	readTransport2 := thrift.NewTMemoryBuffer()
	readTransport2.Buffer = transport2.Buffer
	readProtocol2 := thrift.NewTBinaryProtocolConf(readTransport2, nil)

	newLogicalType2 := NewLogicalType()
	require.NoError(t, newLogicalType2.Read(ctx, readProtocol2))
	require.True(t, newLogicalType2.IsSetINTEGER())
	require.Equal(t, int8(32), newLogicalType2.GetINTEGER().BitWidth)

	transport3 := thrift.NewTMemoryBuffer()
	protocol3 := thrift.NewTBinaryProtocolConf(transport3, nil)

	stats := NewStatistics()
	stats.Max = []byte("max-value")
	stats.Min = []byte("min-value")
	stats.NullCount = thrift.Int64Ptr(42)
	stats.DistinctCount = thrift.Int64Ptr(100)
	stats.MaxValue = []byte("max-logical-value")
	stats.MinValue = []byte("min-logical-value")

	require.NoError(t, stats.Write(ctx, protocol3))

	readTransport3 := thrift.NewTMemoryBuffer()
	readTransport3.Buffer = transport3.Buffer
	readProtocol3 := thrift.NewTBinaryProtocolConf(readTransport3, nil)

	newStats := NewStatistics()
	err := newStats.Read(ctx, readProtocol3)
	require.NoError(t, err)
	require.True(t, bytes.Equal(newStats.Min, stats.Min))
	require.Equal(t, stats.GetNullCount(), newStats.GetNullCount())
	require.Equal(t, stats.GetDistinctCount(), newStats.GetDistinctCount())
	require.True(t, bytes.Equal(newStats.MaxValue, stats.MaxValue))
	require.True(t, bytes.Equal(newStats.MinValue, stats.MinValue))

	transport4 := thrift.NewTMemoryBuffer()
	protocol4 := thrift.NewTBinaryProtocolConf(transport4, nil)

	schemaElem := NewSchemaElement()
	schemaElem.Type = TypePtr(Type_BYTE_ARRAY)
	schemaElem.TypeLength = thrift.Int32Ptr(128)
	schemaElem.RepetitionType = FieldRepetitionTypePtr(FieldRepetitionType_OPTIONAL)
	schemaElem.Name = "comprehensive-field"
	schemaElem.NumChildren = thrift.Int32Ptr(3)
	schemaElem.ConvertedType = ConvertedTypePtr(ConvertedType_UTF8)
	schemaElem.Scale = thrift.Int32Ptr(2)
	schemaElem.Precision = thrift.Int32Ptr(10)
	schemaElem.FieldID = thrift.Int32Ptr(999)
	logicalTypeForSchema := NewLogicalType()
	logicalTypeForSchema.STRING = NewStringType()
	schemaElem.LogicalType = logicalTypeForSchema

	err = schemaElem.Write(ctx, protocol4)
	require.NoError(t, err)

	readTransport4 := thrift.NewTMemoryBuffer()
	readTransport4.Buffer = transport4.Buffer
	readProtocol4 := thrift.NewTBinaryProtocolConf(readTransport4, nil)

	newSchemaElem := NewSchemaElement()
	err = newSchemaElem.Read(ctx, readProtocol4)
	require.NoError(t, err)
	require.Equal(t, newSchemaElem.GetTypeLength(), schemaElem.GetTypeLength())
	require.Equal(t, newSchemaElem.GetRepetitionType(), schemaElem.GetRepetitionType())
	require.Equal(t, newSchemaElem.Name, schemaElem.Name)
	require.Equal(t, newSchemaElem.GetNumChildren(), schemaElem.GetNumChildren())
	require.Equal(t, newSchemaElem.GetConvertedType(), schemaElem.GetConvertedType())
	require.Equal(t, newSchemaElem.GetScale(), schemaElem.GetScale())
	require.Equal(t, newSchemaElem.GetPrecision(), schemaElem.GetPrecision())
	require.Equal(t, newSchemaElem.GetFieldID(), schemaElem.GetFieldID())
	require.True(t, newSchemaElem.IsSetLogicalType())
}

func Test_EqualsEdgeCases(t *testing.T) {
	stats1 := NewStatistics()
	stats1.Max = []byte("test")

	stats2 := NewStatistics()
	require.False(t, stats1.Equals(stats2))
	stats3 := NewStatistics()
	stats3.Max = []byte("different")
	require.False(t, stats1.Equals(stats3))
}

func Test_StatisticsString(t *testing.T) {
	stats := NewStatistics()
	str1 := stats.String()
	require.NotEmpty(t, str1)

	stats.Max = []byte("max")
	stats.Min = []byte("min")
	str2 := stats.String()
	require.NotEqual(t, str1, str2)
}

func Test_OffsetIndex(t *testing.T) {
	oi := NewOffsetIndex()
	require.NotNil(t, oi)

	pageLocations := []*PageLocation{
		{
			Offset:             1024,
			CompressedPageSize: 512,
			FirstRowIndex:      0,
		},
		{
			Offset:             2048,
			CompressedPageSize: 768,
			FirstRowIndex:      100,
		},
	}
	oi.PageLocations = pageLocations
	require.Equal(t, pageLocations, oi.GetPageLocations())

	str := oi.String()
	require.NotEmpty(t, str)
	require.Contains(t, str, "OffsetIndex")

	oi2 := NewOffsetIndex()
	oi2.PageLocations = pageLocations
	require.True(t, oi.Equals(oi2))

	oi3 := NewOffsetIndex()
	oi3.PageLocations = []*PageLocation{
		{
			Offset:             1024,
			CompressedPageSize: 512,
			FirstRowIndex:      0,
		},
	}
	require.False(t, oi.Equals(oi3))
}

func Test_ColumnIndex(t *testing.T) {
	ci := NewColumnIndex()
	require.NotNil(t, ci)

	nullPages := []bool{false, true, false}
	ci.NullPages = nullPages
	require.Equal(t, nullPages, ci.GetNullPages())

	minValues := [][]byte{[]byte("min1"), nil, []byte("min3")}
	ci.MinValues = minValues
	require.Equal(t, minValues, ci.GetMinValues())

	maxValues := [][]byte{[]byte("max1"), nil, []byte("max3")}
	ci.MaxValues = maxValues
	require.Equal(t, maxValues, ci.GetMaxValues())

	boundaryOrder := BoundaryOrder_ASCENDING
	ci.BoundaryOrder = boundaryOrder
	require.Equal(t, boundaryOrder, ci.GetBoundaryOrder())

	nullCounts := []int64{0, 10, 5}
	ci.NullCounts = nullCounts
	require.Equal(t, nullCounts, ci.GetNullCounts())

	str := ci.String()
	require.NotEmpty(t, str)
	require.Contains(t, str, "ColumnIndex")

	ci2 := NewColumnIndex()
	ci2.NullPages = nullPages
	ci2.MinValues = minValues
	ci2.MaxValues = maxValues
	ci2.BoundaryOrder = boundaryOrder
	ci2.NullCounts = nullCounts
	require.True(t, ci.Equals(ci2))

	ci3 := NewColumnIndex()
	ci3.NullPages = []bool{true, true, false}
	ci3.MinValues = minValues
	ci3.MaxValues = maxValues
	ci3.BoundaryOrder = boundaryOrder
	ci3.NullCounts = nullCounts
	require.False(t, ci.Equals(ci3))
}

func Test_AesGcmV1(t *testing.T) {
	aes := NewAesGcmV1()
	require.NotNil(t, aes)

	aadPrefix := []byte("aad_prefix_test")
	aes.AadPrefix = aadPrefix
	require.Equal(t, aadPrefix, aes.GetAadPrefix())

	aadFileUnique := []byte("aad_file_unique_test")
	aes.AadFileUnique = aadFileUnique
	require.Equal(t, aadFileUnique, aes.GetAadFileUnique())

	supplyAadPrefix := false
	aes.SupplyAadPrefix = &supplyAadPrefix
	require.Equal(t, supplyAadPrefix, aes.GetSupplyAadPrefix())
	require.True(t, aes.IsSetSupplyAadPrefix())

	str := aes.String()
	require.NotEmpty(t, str)
	require.Contains(t, str, "AesGcmV1")

	aes2 := NewAesGcmV1()
	aes2.AadPrefix = aadPrefix
	aes2.AadFileUnique = aadFileUnique
	aes2.SupplyAadPrefix = &supplyAadPrefix
	require.True(t, aes.Equals(aes2))

	aes3 := NewAesGcmV1()
	aes3.AadPrefix = []byte("different")
	aes3.AadFileUnique = aadFileUnique
	aes3.SupplyAadPrefix = &supplyAadPrefix
	require.False(t, aes.Equals(aes3))
}

func Test_ColumnOrder(t *testing.T) {
	co := NewColumnOrder()
	require.NotNil(t, co)

	typeOrder := NewTypeDefinedOrder()
	co.TYPE_ORDER = typeOrder
	require.Equal(t, typeOrder, co.GetTYPE_ORDER())
	require.True(t, co.IsSetTYPE_ORDER())

	str := co.String()
	require.NotEmpty(t, str)
	require.Contains(t, str, "ColumnOrder")

	co2 := NewColumnOrder()
	co2.TYPE_ORDER = typeOrder
	require.True(t, co.Equals(co2))

	co3 := NewColumnOrder()
	require.False(t, co.Equals(co3))
}

func Test_PageEncodingStats(t *testing.T) {
	pes := NewPageEncodingStats()
	require.NotNil(t, pes)

	pageType := PageType_DATA_PAGE
	pes.PageType = pageType
	require.Equal(t, pageType, pes.GetPageType())

	encoding := Encoding_PLAIN
	pes.Encoding = encoding
	require.Equal(t, encoding, pes.GetEncoding())

	count := int32(1000)
	pes.Count = count
	require.Equal(t, count, pes.GetCount())

	str := pes.String()
	require.NotEmpty(t, str)
	require.Contains(t, str, "PageEncodingStats")

	pes2 := NewPageEncodingStats()
	pes2.PageType = pageType
	pes2.Encoding = encoding
	pes2.Count = count
	require.True(t, pes.Equals(pes2))
}

func Test_ColumnMetaData_GetType(t *testing.T) {
	cmd := NewColumnMetaData()
	require.NotNil(t, cmd)

	dataType := Type_INT64
	cmd.Type = dataType
	require.Equal(t, dataType, cmd.GetType())

	encodings := []Encoding{Encoding_PLAIN, Encoding_RLE}
	cmd.Encodings = encodings
	require.Equal(t, encodings, cmd.GetEncodings())

	encodingStats := []*PageEncodingStats{
		{
			PageType: PageType_DATA_PAGE,
			Encoding: Encoding_PLAIN,
			Count:    500,
		},
	}
	cmd.EncodingStats = encodingStats
	require.Equal(t, encodingStats, cmd.GetEncodingStats())
}

func Test_BloomFilterHash(t *testing.T) {
	bfh := NewBloomFilterHash()
	require.NotNil(t, bfh)

	xxhash := NewXxHash()
	bfh.XXHASH = xxhash
	require.Equal(t, xxhash, bfh.GetXXHASH())
	require.True(t, bfh.IsSetXXHASH())

	str := bfh.String()
	require.NotEmpty(t, str)
	require.Contains(t, str, "BloomFilterHash")

	bfh2 := NewBloomFilterHash()
	bfh2.XXHASH = xxhash
	require.True(t, bfh.Equals(bfh2))
}

func Test_BloomFilterCompression(t *testing.T) {
	bfc := NewBloomFilterCompression()
	require.NotNil(t, bfc)

	uncompressed := NewUncompressed()
	bfc.UNCOMPRESSED = uncompressed
	require.Equal(t, uncompressed, bfc.GetUNCOMPRESSED())
	require.True(t, bfc.IsSetUNCOMPRESSED())

	str := bfc.String()
	require.NotEmpty(t, str)
	require.Contains(t, str, "BloomFilterCompression")

	bfc2 := NewBloomFilterCompression()
	bfc2.UNCOMPRESSED = uncompressed
	require.True(t, bfc.Equals(bfc2))
}

func Test_BloomFilterHeader(t *testing.T) {
	bfh := NewBloomFilterHeader()
	require.NotNil(t, bfh)

	numBytes := int32(1024)
	bfh.NumBytes = numBytes
	require.Equal(t, numBytes, bfh.GetNumBytes())

	algorithm := NewBloomFilterAlgorithm()
	algorithm.BLOCK = NewSplitBlockAlgorithm()
	bfh.Algorithm = algorithm
	require.Equal(t, algorithm, bfh.GetAlgorithm())
	require.True(t, bfh.IsSetAlgorithm())

	hash := NewBloomFilterHash()
	hash.XXHASH = NewXxHash()
	bfh.Hash = hash
	require.Equal(t, hash, bfh.GetHash())
	require.True(t, bfh.IsSetHash())

	compression := NewBloomFilterCompression()
	compression.UNCOMPRESSED = NewUncompressed()
	bfh.Compression = compression
	require.Equal(t, compression, bfh.GetCompression())
	require.True(t, bfh.IsSetCompression())

	str := bfh.String()
	require.NotEmpty(t, str)
	require.Contains(t, str, "BloomFilterHeader")

	bfh2 := NewBloomFilterHeader()
	bfh2.NumBytes = numBytes
	bfh2.Algorithm = algorithm
	bfh2.Hash = hash
	bfh2.Compression = compression
	require.True(t, bfh.Equals(bfh2))
}

func Test_FileMetaData_Additional(t *testing.T) {
	fmd := NewFileMetaData()
	require.NotNil(t, fmd)

	columnOrders := []*ColumnOrder{
		{TYPE_ORDER: NewTypeDefinedOrder()},
		{TYPE_ORDER: NewTypeDefinedOrder()},
	}
	fmd.ColumnOrders = columnOrders
	require.Equal(t, columnOrders, fmd.GetColumnOrders())
	require.True(t, fmd.IsSetColumnOrders())

	encryptionAlgorithm := NewEncryptionAlgorithm()
	encryptionAlgorithm.AES_GCM_V1 = NewAesGcmV1()
	fmd.EncryptionAlgorithm = encryptionAlgorithm
	require.Equal(t, encryptionAlgorithm, fmd.GetEncryptionAlgorithm())
	require.True(t, fmd.IsSetEncryptionAlgorithm())

	footerSigningKey := []byte("footer_signing_key_metadata")
	fmd.FooterSigningKeyMetadata = footerSigningKey
	require.Equal(t, footerSigningKey, fmd.GetFooterSigningKeyMetadata())
	require.True(t, fmd.IsSetFooterSigningKeyMetadata())

	str := fmd.String()
	require.NotEmpty(t, str)
	require.Contains(t, str, "FileMetaData")
}

func Test_FileCryptoMetaData(t *testing.T) {
	fcmd := NewFileCryptoMetaData()
	require.NotNil(t, fcmd)

	encryptionAlgorithm := NewEncryptionAlgorithm()
	encryptionAlgorithm.AES_GCM_V1 = NewAesGcmV1()
	fcmd.EncryptionAlgorithm = encryptionAlgorithm
	require.Equal(t, encryptionAlgorithm, fcmd.GetEncryptionAlgorithm())
	require.True(t, fcmd.IsSetEncryptionAlgorithm())

	keyMetadata := []byte("file_crypto_key_metadata")
	fcmd.KeyMetadata = keyMetadata
	require.Equal(t, keyMetadata, fcmd.GetKeyMetadata())

	str := fcmd.String()
	require.NotEmpty(t, str)
	require.Contains(t, str, "FileCryptoMetaData")

	fcmd2 := NewFileCryptoMetaData()
	fcmd2.EncryptionAlgorithm = encryptionAlgorithm
	fcmd2.KeyMetadata = keyMetadata
	require.True(t, fcmd.Equals(fcmd2))
}

func Test_AdditionalEdgeCases(t *testing.T) {
	// Test nil getters return appropriate defaults
	oi := NewOffsetIndex()
	require.Equal(t, []*PageLocation(nil), oi.GetPageLocations())

	ci := NewColumnIndex()
	require.Equal(t, []bool(nil), ci.GetNullPages())
	require.Equal(t, [][]byte(nil), ci.GetMinValues())
	require.Equal(t, [][]byte(nil), ci.GetMaxValues())
	require.Equal(t, BoundaryOrder(0), ci.GetBoundaryOrder())
	require.Equal(t, []int64(nil), ci.GetNullCounts())

	aes := NewAesGcmV1()
	require.Equal(t, []byte(nil), aes.GetAadPrefix())
	require.Equal(t, []byte(nil), aes.GetAadFileUnique())
	require.Equal(t, false, aes.GetSupplyAadPrefix())
	require.False(t, aes.IsSetSupplyAadPrefix())

	co := NewColumnOrder()
	require.Nil(t, co.GetTYPE_ORDER())
	require.False(t, co.IsSetTYPE_ORDER())

	bfh := NewBloomFilterHash()
	require.Nil(t, bfh.GetXXHASH())
	require.False(t, bfh.IsSetXXHASH())

	bfc := NewBloomFilterCompression()
	require.Nil(t, bfc.GetUNCOMPRESSED())
	require.False(t, bfc.IsSetUNCOMPRESSED())

	bfheader := NewBloomFilterHeader()
	require.Equal(t, int32(0), bfheader.GetNumBytes())
	require.Nil(t, bfheader.GetAlgorithm())
	require.Nil(t, bfheader.GetHash())
	require.Nil(t, bfheader.GetCompression())
	require.False(t, bfheader.IsSetAlgorithm())
	require.False(t, bfheader.IsSetHash())
	require.False(t, bfheader.IsSetCompression())
}

func Test_StringMethodVariationsStructures(t *testing.T) {
	se := NewSchemaElement()
	se.Name = "test"
	beforeType := se.String()

	fieldType := Type_INT32
	se.Type = &fieldType
	afterType := se.String()
	require.NotEqual(t, beforeType, afterType)
}
