//go:build example

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"
	"time"

	"github.com/hangxie/parquet-go/v2/marshal"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/types"
	"github.com/hangxie/parquet-go/v2/writer"
)

type InnerMap struct {
	Map  map[string]int32 `parquet:"name=Map, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	List []string         `parquet:"name=List, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=DECIMAL, valuescale=2, valueprecision=10"`
}

type VariantType struct {
	Metadata []byte `parquet:"name=metadata, type=BYTE_ARRAY, encoding=DELTA_LENGTH_BYTE_ARRAY, compression=ZSTD"`
	Value    []byte `parquet:"name=value, type=BYTE_ARRAY, encoding=DELTA_LENGTH_BYTE_ARRAY, compression=SNAPPY"`
}

// there is no TIME_NANOS or TIMESTAMP_NANOS
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#deprecated-time-convertedtype
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#deprecated-timestamp-convertedtype
type AllTypes struct {
	Bool              bool                `parquet:"name=Bool, type=BOOLEAN, encoding=BIT_PACKED, compression=UNCOMPRESSED"`
	Int32             int32               `parquet:"name=Int32, type=INT32, encoding=DELTA_BINARY_PACKED, compression=SNAPPY"`
	Int64             int64               `parquet:"name=Int64, type=INT64, encoding=DELTA_BINARY_PACKED, compression=GZIP"`
	Int96             string              `parquet:"name=Int96, type=INT96, compression=LZ4_RAW"`
	Float             float32             `parquet:"name=Float, type=FLOAT, encoding=BYTE_STREAM_SPLIT, compression=ZSTD"`
	Float16Val        string              `parquet:"name=Float16Val, type=FIXED_LEN_BYTE_ARRAY, length=2, logicaltype=FLOAT16, encoding=PLAIN, compression=BROTLI"`
	Double            float64             `parquet:"name=Double, type=DOUBLE, encoding=BYTE_STREAM_SPLIT, compression=UNCOMPRESSED"`
	ByteArray         string              `parquet:"name=ByteArray, type=BYTE_ARRAY, encoding=DELTA_LENGTH_BYTE_ARRAY, compression=SNAPPY"`
	Enum              string              `parquet:"name=Enum, type=BYTE_ARRAY, convertedtype=ENUM, encoding=RLE_DICTIONARY, compression=GZIP"`
	Uuid              string              `parquet:"name=Uuid, type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID, encoding=PLAIN, compression=LZ4_RAW"`
	Json              string              `parquet:"name=Json, type=BYTE_ARRAY, convertedtype=JSON, encoding=DELTA_BYTE_ARRAY, compression=ZSTD"`
	Bson              string              `parquet:"name=Bson, type=BYTE_ARRAY, convertedtype=BSON, compression=BROTLI"`
	Json2             string              `parquet:"name=Json2, type=BYTE_ARRAY, logicaltype=JSON, encoding=PLAIN, compression=UNCOMPRESSED"`
	Bson2             string              `parquet:"name=Bson2, type=BYTE_ARRAY, logicaltype=BSON, encoding=DELTA_BYTE_ARRAY, compression=SNAPPY"`
	FixedLenByteArray string              `parquet:"name=FixedLenByteArray, type=FIXED_LEN_BYTE_ARRAY, length=10, encoding=RLE_DICTIONARY, compression=LZ4_RAW"`
	String            string              `parquet:"name=String, type=BYTE_ARRAY, convertedtype=UTF8, encoding=RLE_DICTIONARY, compression=ZSTD"`
	String2           string              `parquet:"name=String_2, type=BYTE_ARRAY, logicaltype=STRING, encoding=PLAIN, compression=BROTLI"`
	ConvertedInt8     int32               `parquet:"name=Int_8, type=INT32, convertedtype=INT32, convertedtype=INT_8, encoding=BIT_PACKED, compression=UNCOMPRESSED"`
	ConvertedInt16    int32               `parquet:"name=Int_16, type=INT32, convertedtype=INT_16, encoding=PLAIN, compression=SNAPPY"`
	ConvertedInt32    int32               `parquet:"name=Int_32, type=INT32, convertedtype=INT_32, encoding=RLE_DICTIONARY, compression=GZIP"`
	ConvertedInt64    int64               `parquet:"name=Int_64, type=INT64, convertedtype=INT_64, encoding=RLE, compression=LZ4_RAW"`
	ConvertedUint8    int32               `parquet:"name=Uint_8, type=INT32, convertedtype=UINT_8, encoding=BIT_PACKED, compression=ZSTD"`
	ConvertedUint16   int32               `parquet:"name=Uint_16, type=INT32, convertedtype=UINT_16, encoding=DELTA_BINARY_PACKED, compression=BROTLI"`
	ConvertedUint32   int32               `parquet:"name=Uint_32, type=INT32, convertedtype=UINT_32, compression=UNCOMPRESSED"`
	ConvertedUint64   int64               `parquet:"name=Uint_64, type=INT64, convertedtype=UINT_64, encoding=RLE, compression=SNAPPY"`
	Int8Logical       int32               `parquet:"name=Int8Logical, type=INT32, logicaltype=INTEGER, logicaltype.bitwidth=8, logicaltype.issigned=true"`
	Uint16Logical     int32               `parquet:"name=Uint16Logical, type=INT32, logicaltype=INTEGER, logicaltype.bitwidth=16, logicaltype.issigned=false"`
	Date              int32               `parquet:"name=Date, type=INT32, convertedtype=DATE, encoding=DELTA_BINARY_PACKED, compression=GZIP"`
	Date2             int32               `parquet:"name=Date2, type=INT32, logicaltype=DATE, encoding=RLE_DICTIONARY, compression=LZ4_RAW"`
	TimeMillis        int32               `parquet:"name=TimeMillis, type=INT32, convertedtype=TIME_MILLIS, encoding=DELTA_BINARY_PACKED, compression=ZSTD"`
	TimeMillis2       int32               `parquet:"name=TimeMillis2, type=INT32, logicaltype=TIME, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS, encoding=RLE, compression=BROTLI"`
	TimeMicros        int64               `parquet:"name=TimeMicros, type=INT64, convertedtype=TIME_MICROS, encoding=DELTA_BINARY_PACKED, compression=UNCOMPRESSED"`
	TimeMicros2       int64               `parquet:"name=TimeMicros2, type=INT64, logicaltype=TIME, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS, encoding=RLE, compression=SNAPPY"`
	TimeNanos2        int64               `parquet:"name=TimeNanos2, type=INT64, logicaltype=TIME, logicaltype.isadjustedtoutc=false, logicaltype.unit=NANOS, encoding=DELTA_BINARY_PACKED, compression=GZIP"`
	TimestampMillis   int64               `parquet:"name=TimestampMillis, type=INT64, convertedtype=TIMESTAMP_MILLIS, encoding=DELTA_BINARY_PACKED, compression=LZ4_RAW"`
	TimestampMillis2  int64               `parquet:"name=TimestampMillis2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS, encoding=RLE, compression=ZSTD"`
	TimestampMicros   int64               `parquet:"name=TimestampMicros, type=INT64, convertedtype=TIMESTAMP_MICROS, encoding=DELTA_BINARY_PACKED, compression=BROTLI"`
	TimestampMicros2  int64               `parquet:"name=TimestampMicros2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS, encoding=RLE_DICTIONARY, compression=UNCOMPRESSED"`
	TimestampNanos2   int64               `parquet:"name=TimestampNanos2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=NANOS, encoding=RLE, compression=SNAPPY"`
	Interval          string              `parquet:"name=Interval, type=FIXED_LEN_BYTE_ARRAY, convertedtype=INTERVAL, length=12, encoding=PLAIN, compression=GZIP"`
	Decimal1          int32               `parquet:"name=Decimal1, type=INT32, convertedtype=DECIMAL, scale=2, precision=9, encoding=DELTA_BINARY_PACKED, compression=LZ4_RAW"`
	Decimal2          int64               `parquet:"name=Decimal2, type=INT64, convertedtype=DECIMAL, scale=2, precision=18, encoding=DELTA_BINARY_PACKED, compression=ZSTD"`
	Decimal3          string              `parquet:"name=Decimal3, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12, encoding=PLAIN, compression=BROTLI"`
	Decimal4          string              `parquet:"name=Decimal4, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=20, encoding=DELTA_LENGTH_BYTE_ARRAY, compression=UNCOMPRESSED"`
	Decimal5          int32               `parquet:"name=decimal5, type=INT32, scale=2, precision=9, logicaltype=DECIMAL, logicaltype.precision=9, logicaltype.scale=2, encoding=RLE_DICTIONARY, compression=SNAPPY"`
	DecimalPointer    *string             `parquet:"name=DecimalPointer, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12, repetitiontype=OPTIONAL, encoding=RLE_DICTIONARY, compression=GZIP"`
	Map               map[string]int32    `parquet:"name=Map, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32, keyencoding=RLE_DICTIONARY, valueencoding=BIT_PACKED, keycompression=LZ4_RAW, valuecompression=ZSTD"`
	List              []string            `parquet:"name=List, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8, valuecompression=ZSTD"`
	Repeated          []int32             `parquet:"name=Repeated, type=INT32, repetitiontype=REPEATED, encoding=DELTA_BINARY_PACKED, compression=BROTLI"`
	NestedMap         map[string]InnerMap `parquet:"name=NestedMap, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, keycompression=UNCOMPRESSED, valuetype=STRUCT"`
	NestedList        []InnerMap          `parquet:"name=NestedList, type=LIST, valuetype=STRUCT"`

	// New geospatial logical types
	Geometry  string `parquet:"name=Geometry, type=BYTE_ARRAY, logicaltype=GEOMETRY, logicaltype.crs=OGC:CRS84"`
	Geography string `parquet:"name=Geography, type=BYTE_ARRAY, logicaltype=GEOGRAPHY, logicaltype.crs=OGC:CRS84, logicaltype.algorithm=SPHERICAL"`

	// RFC 7946 geometry type fields (GEOMETRY)
	GeometryPoint              string `parquet:"name=GeometryPoint, type=BYTE_ARRAY, logicaltype=GEOMETRY, logicaltype.crs=OGC:CRS84"`
	GeometryMultiPoint         string `parquet:"name=GeometryMultiPoint, type=BYTE_ARRAY, logicaltype=GEOMETRY, logicaltype.crs=OGC:CRS84"`
	GeometryLineString         string `parquet:"name=GeometryLineString, type=BYTE_ARRAY, logicaltype=GEOMETRY, logicaltype.crs=OGC:CRS84"`
	GeometryMultiLineString    string `parquet:"name=GeometryMultiLineString, type=BYTE_ARRAY, logicaltype=GEOMETRY, logicaltype.crs=OGC:CRS84"`
	GeometryPolygon            string `parquet:"name=GeometryPolygon, type=BYTE_ARRAY, logicaltype=GEOMETRY, logicaltype.crs=OGC:CRS84"`
	GeometryMultiPolygon       string `parquet:"name=GeometryMultiPolygon, type=BYTE_ARRAY, logicaltype=GEOMETRY, logicaltype.crs=OGC:CRS84"`
	GeometryGeometryCollection string `parquet:"name=GeometryGeometryCollection, type=BYTE_ARRAY, logicaltype=GEOMETRY, logicaltype.crs=OGC:CRS84"`

	// Note: VARIANT is a GROUP type with metadata and value binary fields per Parquet spec
	Variant VariantType `parquet:"name=Variant, type=VARIANT, logicaltype=VARIANT, logicaltype.specification_version=1"`
}

// uuidStringToBytes converts a UUID string to 16-byte binary representation
func uuidStringToBytes(uuidStr string) string {
	// Remove dashes from UUID string
	hexStr := strings.ReplaceAll(uuidStr, "-", "")

	// Convert hex string to bytes
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		// If conversion fails, return a default 16-byte UUID
		return string(make([]byte, 16))
	}

	// Ensure we have exactly 16 bytes
	if len(bytes) != 16 {
		// Pad or truncate to 16 bytes
		result := make([]byte, 16)
		copy(result, bytes)
		return string(result)
	}

	return string(bytes)
}

// float32ToFloat16 converts a float32 into IEEE 754 binary16 (little-endian 2 bytes)
func float32ToFloat16(f float32) string {
	// Handle NaN/Inf explicitly
	if f != f { // NaN
		return string([]byte{0x00, 0x7e})
	}
	if f > 65504 { // max half
		return string([]byte{0x00, 0x7c})
	}
	if f < -65504 {
		return string([]byte{0x00, 0xfc})
	}
	bits := math.Float32bits(f)
	sign := uint16((bits >> 31) & 0x1)
	exp := int32((bits>>23)&0xff) - 127 + 15 // re-bias
	mant := uint32(bits & 0x7fffff)

	var half uint16
	if exp <= 0 {
		// subnormal or zero in half
		if exp < -10 {
			half = sign << 15 // zero
		} else {
			// add implicit leading 1 to mantissa
			mant = (mant | 0x800000) >> uint32(1-exp)
			// round to nearest
			mant = (mant + 0x1000) >> 13
			half = (sign << 15) | uint16(mant)
		}
	} else if exp >= 31 {
		// overflow -> inf
		half = (sign << 15) | (0x1f << 10)
	} else {
		// normalized
		// round mantissa
		mant = mant + 0x1000
		if mant&0x800000 != 0 {
			// mant overflow -> adjust exp
			mant = 0
			exp++
			if exp >= 31 {
				half = (sign << 15) | (0x1f << 10)
				return string([]byte{byte(half), byte(half >> 8)})
			}
		}
		half = (sign << 15) | (uint16(exp) << 10) | uint16(mant>>13)
	}
	return string([]byte{byte(half), byte(half >> 8)})
}

func main() {
	fw, err := local.NewLocalFileWriter("/tmp/all-types.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	// Output in geojson format
	// types.SetGeometryJSONMode(types.GeospatialModeGeoJSON)
	// types.SetGeographyJSONMode(types.GeospatialModeGeoJSON)

	// output in hybrid format, ie geojson + raw
	types.SetGeometryJSONMode(types.GeospatialModeHybrid)
	types.SetGeographyJSONMode(types.GeospatialModeHybrid)
	// raw data can be hex or base64
	types.SetGeospatialHybridRawBase64(true)
	// Use default coordinate precision (6 decimals) for GeoJSON
	types.SetGeospatialCoordinatePrecision(6)

	// output in raw format in base64 encoding
	// types.SetGeometryJSONMode(types.GeospatialModeBase64)
	// types.SetGeographyJSONMode(types.GeospatialModeBase64)

	pw, err := writer.NewParquetWriter(fw, new(AllTypes), 4)
	if err != nil {
		fmt.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024
	pw.PageSize = 8 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	decimals := []int32{0, 1, 22, 333, 4444, 0, -1, -22, -333, -4444}
	for i := range 10 {
		ts, _ := time.Parse("2006-01-02T15:04:05.000000Z", fmt.Sprintf("2022-01-01T%02d:%02d:%02d.%03d%03dZ", i, i, i, i, i))
		strI := fmt.Sprintf("%d", i)
		interval := make([]byte, 4)
		binary.LittleEndian.PutUint32(interval, uint32(i))
		jsonObj := map[string]int{
			strI: i,
		}
		jsonStr, _ := json.Marshal(jsonObj)
		// try to avoid unnecessary entries in go.mod
		// "go.mongodb.org/mongo-driver/bson"
		// bsonStr, _ := bson.Marshal(jsonObj)
		bsonStr := []byte{0x0c, 0x00, 0x00, 0x00, 0x10, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
		value := AllTypes{
			Bool:              i%2 == 0,
			Int32:             int32(i),
			Int64:             int64(i),
			Int96:             types.TimeToINT96(ts),
			Float:             float32(i) * 0.5,
			Double:            float64(i) * 0.5,
			ByteArray:         fmt.Sprintf("ByteArray-%d", i),
			Enum:              fmt.Sprintf("Enum-%d", i),
			Uuid:              string(bytes.Repeat([]byte{byte(i)}, 16)),
			Float16Val:        float32ToFloat16(float32(i) + 0.5),
			Json:              string(jsonStr),
			Bson:              string(bsonStr),
			Json2:             string(jsonStr),
			Bson2:             string(bsonStr),
			FixedLenByteArray: fmt.Sprintf("Fixed-%04d", i),
			String:            fmt.Sprintf("UTF8-%d", i),
			String2:           fmt.Sprintf("String2-%d", i),
			ConvertedInt8:     int32(i),
			ConvertedInt16:    int32(i),
			ConvertedInt32:    int32(i),
			ConvertedInt64:    int64(i),
			ConvertedUint8:    int32(i),
			ConvertedUint16:   int32(i),
			ConvertedUint32:   int32(i),
			ConvertedUint64:   int64(i),
			Date:              int32(i * -1000),
			Date2:             int32(i * 1000),
			TimeMillis:        int32(i) * 1_001,
			TimeMillis2:       int32(i) * 1_001,
			TimeMicros:        int64(i) * 1_000_001,
			TimeMicros2:       int64(i) * 1_000_001,
			TimeNanos2:        int64(i) * 1_000_000_001,
			TimestampMillis:   int64(i) + 1_640_995_200_000,
			TimestampMillis2:  int64(i) + 1_640_995_200_000,
			TimestampMicros:   int64(i) + 1_640_995_200_000_000,
			TimestampMicros2:  int64(i) + 1_640_995_200_000_000,
			TimestampNanos2:   int64(i) + 1_640_995_200_000_000_000,
			Interval:          string(bytes.Repeat(interval, 3)),
			Int8Logical:       int32(i%128 - 64),
			Uint16Logical:     int32(i * 1000),
			Decimal1:          decimals[i],
			Decimal2:          int64(decimals[i]),
			Decimal3:          types.StrIntToBinary(fmt.Sprintf("%0.2f", float32(decimals[i]/100.0)), "BigEndian", 12, true),
			Decimal4:          types.StrIntToBinary(fmt.Sprintf("%0.2f", float32(decimals[i]/100.0)), "BigEndian", 0, true),
			Decimal5:          decimals[i],
			DecimalPointer:    nil,
			Map:               map[string]int32{},
			List:              []string{},
			Repeated:          []int32{},
			NestedMap:         map[string]InnerMap{},
			NestedList:        []InnerMap{},
			Geometry:          sampleGeometry(i),
			Geography:         sampleGeography(i),
			// Populate all RFC 7946 geometry type fields
			GeometryPoint:           wkbPoint(float64(i), float64(i)+0.5),
			GeometryMultiPoint:      wkbMultiPoint([][2]float64{{float64(i), float64(i) + 0.1}, {float64(i) + 1, float64(i) + 1}}),
			GeometryLineString:      wkbLineString([][2]float64{{0, 0}, {float64(i), float64(i)}}),
			GeometryMultiLineString: wkbMultiLineString([][][2]float64{{{0, 0}, {1, 1}}, {{2, 2}, {3, 3}}}),
			GeometryPolygon:         wkbPolygon([][][2]float64{{{0, 0}, {float64(i), 0}, {float64(i), float64(i)}, {0, float64(i)}, {0, 0}}}),
			GeometryMultiPolygon: wkbMultiPolygon([][][][2]float64{
				{{{0, 0}, {2, 0}, {2, 2}, {0, 2}, {0, 0}}},
				{{{3, 3}, {4, 3}, {4, 4}, {3, 4}, {3, 3}}},
			}),
			GeometryGeometryCollection: wkbGeometryCollection([]string{
				wkbPoint(float64(i), float64(i)+0.25),
				wkbLineString([][2]float64{{0, 0}, {1, 1}}),
			}),
			Variant: createSampleVariant(i),
		}
		if i%2 == 0 {
			value.DecimalPointer = nil
		} else {
			value.DecimalPointer = &value.Decimal3
		}
		for j := range i {
			key := fmt.Sprintf("Composite-%d", j)
			value.Map[key] = int32(j)
			value.List = append(value.List, key)
			value.Repeated = append(value.Repeated, int32(j))
			nested := InnerMap{
				Map:  map[string]int32{},
				List: []string{},
			}
			for k := range j {
				key := fmt.Sprintf("Embedded-%d", k)
				nested.Map[key] = int32(k)
				nested.List = append(nested.List, types.StrIntToBinary(fmt.Sprintf("%0.2f", float32(decimals[k]/100.0)), "BigEndian", 12, true))
			}
			value.NestedMap[key] = nested
			value.NestedList = append(value.NestedList, nested)
		}

		if err = pw.Write(value); err != nil {
			fmt.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		fmt.Println("WriteStop error", err)
		return
	}
	_ = fw.Close()

	///read
	fr, err := local.NewLocalFileReader("/tmp/all-types.parquet")
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	num := int(pr.GetNumRows())
	res, err := pr.ReadByNumber(num)
	if err != nil {
		log.Println("Can't read", err)
		return
	}

	jsonFriendly, err := marshal.ConvertToJSONFriendly(res, pr.SchemaHandler)
	if err != nil {
		log.Println("Can't to json", err)
		return
	}

	jsonBs, _ := json.Marshal(jsonFriendly)
	log.Println(string(jsonBs))

	pr.ReadStop()
	_ = fr.Close()
}

// wkbPoint builds a little-endian WKB for 2D Point(x,y)
func wkbPoint(x, y float64) string {
	buf := make([]byte, 0, 1+4+16)
	// little-endian byte order marker
	buf = append(buf, 1)
	// geometry type = 1 (Point)
	tmp4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp4, 1)
	buf = append(buf, tmp4...)
	// coordinates
	tmp8 := make([]byte, 8)
	binary.LittleEndian.PutUint64(tmp8, math.Float64bits(x))
	buf = append(buf, tmp8...)
	binary.LittleEndian.PutUint64(tmp8, math.Float64bits(y))
	buf = append(buf, tmp8...)
	return string(buf)
}

// wkbLineString builds a little-endian WKB for 2D LineString
func wkbLineString(coords [][2]float64) string {
	buf := make([]byte, 0, 1+4+4+len(coords)*16)
	buf = append(buf, 1) // LE
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 2)
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, uint32(len(coords)))
	buf = append(buf, n...)
	tmp := make([]byte, 8)
	for _, c := range coords {
		binary.LittleEndian.PutUint64(tmp, math.Float64bits(c[0]))
		buf = append(buf, tmp...)
		binary.LittleEndian.PutUint64(tmp, math.Float64bits(c[1]))
		buf = append(buf, tmp...)
	}
	return string(buf)
}

// wkbPolygon builds a little-endian WKB for 2D Polygon with one or more rings
func wkbPolygon(rings [][][2]float64) string {
	buf := make([]byte, 0)
	buf = append(buf, 1)
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 3)
	buf = append(buf, t...)
	rn := make([]byte, 4)
	binary.LittleEndian.PutUint32(rn, uint32(len(rings)))
	buf = append(buf, rn...)
	tmp4 := make([]byte, 4)
	tmp8 := make([]byte, 8)
	for _, ring := range rings {
		binary.LittleEndian.PutUint32(tmp4, uint32(len(ring)))
		buf = append(buf, tmp4...)
		for _, c := range ring {
			binary.LittleEndian.PutUint64(tmp8, math.Float64bits(c[0]))
			buf = append(buf, tmp8...)
			binary.LittleEndian.PutUint64(tmp8, math.Float64bits(c[1]))
			buf = append(buf, tmp8...)
		}
	}
	return string(buf)
}

// wkbMultiPoint builds a little-endian WKB for MultiPoint from a list of points
func wkbMultiPoint(points [][2]float64) string {
	buf := make([]byte, 0)
	buf = append(buf, 1)
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 4)
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, uint32(len(points)))
	buf = append(buf, n...)
	for _, p := range points {
		// Each point is its own WKB geometry
		buf = append(buf, []byte(wkbPoint(p[0], p[1]))...)
	}
	return string(buf)
}

// wkbMultiLineString builds a little-endian WKB for MultiLineString
func wkbMultiLineString(lines [][][2]float64) string {
	buf := make([]byte, 0)
	buf = append(buf, 1)
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 5)
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, uint32(len(lines)))
	buf = append(buf, n...)
	for _, ls := range lines {
		buf = append(buf, []byte(wkbLineString(ls))...)
	}
	return string(buf)
}

// wkbMultiPolygon builds a little-endian WKB for MultiPolygon
func wkbMultiPolygon(polys [][][][2]float64) string {
	buf := make([]byte, 0)
	buf = append(buf, 1)
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 6)
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, uint32(len(polys)))
	buf = append(buf, n...)
	for _, poly := range polys {
		buf = append(buf, []byte(wkbPolygon(poly))...)
	}
	return string(buf)
}

// wkbGeometryCollection builds a little-endian WKB for GeometryCollection
// Each element in geoms should be a complete WKB geometry (as returned by
// helpers like wkbPoint, wkbLineString, etc.)
func wkbGeometryCollection(geoms []string) string {
	buf := make([]byte, 0)
	buf = append(buf, 1) // LE
	t := make([]byte, 4)
	binary.LittleEndian.PutUint32(t, 7) // GeometryCollection type id
	buf = append(buf, t...)
	n := make([]byte, 4)
	binary.LittleEndian.PutUint32(n, uint32(len(geoms)))
	buf = append(buf, n...)
	for _, g := range geoms {
		buf = append(buf, []byte(g)...)
	}
	return string(buf)
}

// sampleGeometry returns varied geometry types across rows
func sampleGeometry(i int) string {
	switch i % 7 {
	case 0:
		return wkbPoint(float64(i), float64(i)+1)
	case 1:
		return wkbLineString([][2]float64{{0, 0}, {float64(i), float64(i)}})
	case 2:
		return wkbPolygon([][][2]float64{{{0, 0}, {float64(i), 0}, {float64(i), float64(i)}, {0, float64(i)}, {0, 0}}})
	case 3:
		return wkbMultiPoint([][2]float64{{float64(i), float64(i) + 0.1}, {float64(i) + 1, float64(i) + 1}})
	case 4:
		return wkbMultiLineString([][][2]float64{{{0, 0}, {1, 1}}, {{2, 2}, {3, 3}}})
	case 5:
		return wkbMultiPolygon([][][][2]float64{
			{{{0, 0}, {2, 0}, {2, 2}, {0, 2}, {0, 0}}},
			{{{3, 3}, {4, 3}, {4, 4}, {3, 4}, {3, 3}}},
		})
	default:
		return wkbGeometryCollection([]string{
			wkbPoint(float64(i), float64(i)+0.5),
			wkbLineString([][2]float64{{0, 0}, {1, 1}}),
		})
	}
}

// createSampleVariant creates a properly encoded Variant value
// representing a JSON object: {"kind": "Example", "number": i}
func createSampleVariant(i int) VariantType {
	// Create metadata with sorted dictionary for optimal lookup performance
	// EncodeVariantMetadataSorted returns the metadata and a map of field name -> field ID
	metadata, fieldIDs := types.EncodeVariantMetadataSorted([]string{"kind", "number"})

	// Create object value with two fields using the field ID map
	value := types.EncodeVariantObject(
		[]int{fieldIDs["kind"], fieldIDs["number"]},
		[][]byte{
			types.EncodeVariantString("Example"),
			types.EncodeVariantInt32(int32(i)),
		},
	)

	return VariantType{
		Metadata: metadata,
		Value:    value,
	}
}

// sampleGeography returns varied geography types across rows
func sampleGeography(i int) string {
	switch i % 7 {
	case 0:
		return wkbPoint(-122.4+float64(i)*0.01, 37.8+float64(i)*0.01)
	case 1:
		return wkbLineString([][2]float64{{-122.4, 37.8}, {-122.41, 37.81}})
	case 2:
		return wkbPolygon([][][2]float64{{{-122.5, 37.8}, {-122.4, 37.8}, {-122.4, 37.9}, {-122.5, 37.9}, {-122.5, 37.8}}})
	case 3:
		return wkbMultiPoint([][2]float64{{-122.4, 37.8}, {-122.41, 37.81}, {-122.42, 37.82}})
	case 4:
		return wkbMultiLineString([][][2]float64{{{-122.4, 37.8}, {-122.41, 37.81}}, {{-122.42, 37.82}, {-122.43, 37.83}}})
	case 5:
		return wkbMultiPolygon([][][][2]float64{
			{{{-122.5, 37.8}, {-122.4, 37.8}, {-122.4, 37.9}, {-122.5, 37.9}, {-122.5, 37.8}}},
			{{{-122.45, 37.85}, {-122.44, 37.85}, {-122.44, 37.86}, {-122.45, 37.86}, {-122.45, 37.85}}},
		})
	default:
		return wkbGeometryCollection([]string{
			wkbPoint(-122.4, 37.8),
			wkbLineString([][2]float64{{-122.41, 37.81}, {-122.42, 37.82}}),
		})
	}
}
