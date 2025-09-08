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

// there is no TIME_NANOS or TIMESTAMP_NANOS
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#deprecated-time-convertedtype
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#deprecated-timestamp-convertedtype
type AllTypes struct {
	Bool              bool                `parquet:"name=bool, type=BOOLEAN"`
	Int32             int32               `parquet:"name=Int32, type=INT32"`
	Int64             int64               `parquet:"name=Int64, type=INT64"`
	Int96             string              `parquet:"name=Int96, type=INT96"`
	Float             float32             `parquet:"name=Float, type=FLOAT"`
	Double            float64             `parquet:"name=double, type=DOUBLE"`
	ByteArray         string              `parquet:"name=byteArray, type=BYTE_ARRAY"`
	Enum              string              `parquet:"name=Enum, type=BYTE_ARRAY, convertedtype=ENUM"`
	Uuid              string              `parquet:"name=Uuid, type=FIXED_LEN_BYTE_ARRAY, length=16, logicaltype=UUID"`
	Float16Val        string              `parquet:"name=Float16Val, type=FIXED_LEN_BYTE_ARRAY, length=2, logicaltype=FLOAT16"`
	Json              string              `parquet:"name=Json, type=BYTE_ARRAY, convertedtype=JSON"`
	Bson              string              `parquet:"name=Bson, type=BYTE_ARRAY, logicaltype=BSON"`
	Json2             string              `parquet:"name=Json2, type=BYTE_ARRAY, logicaltype=JSON"`
	Bson2             string              `parquet:"name=Bson2, type=BYTE_ARRAY, logicaltype=BSON"`
	FixedLenByteArray string              `parquet:"name=fixedLenByteArray, type=FIXED_LEN_BYTE_ARRAY, length=10"`
	Utf8              string              `parquet:"name=Utf8, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	String2           string              `parquet:"name=String2, type=BYTE_ARRAY, logicaltype=STRING"`
	ConvertedInt8     int32               `parquet:"name=Int_8, type=INT32, convertedtype=INT32, convertedtype=INT_8"`
	ConvertedInt16    int32               `parquet:"name=Int_16, type=INT32, convertedtype=INT_16"`
	ConvertedInt32    int32               `parquet:"name=Int_32, type=INT32, convertedtype=INT_32"`
	ConvertedInt64    int64               `parquet:"name=Int_64, type=INT64, convertedtype=INT_64"`
	ConvertedUint8    int32               `parquet:"name=Uint_8, type=INT32, convertedtype=UINT_8"`
	ConvertedUint16   int32               `parquet:"name=Uint_16, type=INT32, convertedtype=UINT_16"`
	ConvertedUint32   int32               `parquet:"name=Uint_32, type=INT32, convertedtype=UINT_32"`
	ConvertedUint64   int64               `parquet:"name=Uint_64, type=INT64, convertedtype=UINT_64"`
	Date              int32               `parquet:"name=Date, type=INT32, convertedtype=DATE"`
	Date2             int32               `parquet:"name=Date2, type=INT32, logicaltype=DATE"`
	TimeMillis        int32               `parquet:"name=TimeMillis, type=INT32, convertedtype=TIME_MILLIS"`
	TimeMillis2       int32               `parquet:"name=TimeMillis2, type=INT32, logicaltype=TIME, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	TimeMicros        int64               `parquet:"name=TimeMicros, type=INT64, convertedtype=TIME_MICROS"`
	TimeMicros2       int64               `parquet:"name=TimeMicros2, type=INT64, logicaltype=TIME, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS"`
	TimeNanos2        int64               `parquet:"name=TimeNanos2, type=INT64, logicaltype=TIME, logicaltype.isadjustedtoutc=false, logicaltype.unit=NANOS"`
	TimestampMillis   int64               `parquet:"name=TimestampMillis, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	TimestampMillis2  int64               `parquet:"name=TimestampMillis2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	TimestampMicros   int64               `parquet:"name=TimestampMicros, type=INT64, convertedtype=TIMESTAMP_MICROS"`
	TimestampMicros2  int64               `parquet:"name=TimestampMicros2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS"`
	TimestampNanos2   int64               `parquet:"name=TimestampNanos2, type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=NANOS"`
	Interval          string              `parquet:"name=Interval, type=FIXED_LEN_BYTE_ARRAY, convertedtype=INTERVAL, length=12"`
	Int8Logical       int32               `parquet:"name=Int8Logical, type=INT32, logicaltype=INTEGER, logicaltype.bitwidth=8, logicaltype.issigned=true"`
	Uint16Logical     int32               `parquet:"name=Uint16Logical, type=INT32, logicaltype=INTEGER, logicaltype.bitwidth=16, logicaltype.issigned=false"`
	Decimal1          int32               `parquet:"name=Decimal1, type=INT32, convertedtype=DECIMAL, scale=2, precision=9"`
	Decimal2          int64               `parquet:"name=Decimal2, type=INT64, convertedtype=DECIMAL, scale=2, precision=18"`
	Decimal3          string              `parquet:"name=Decimal3, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12"`
	Decimal4          string              `parquet:"name=Decimal4, type=BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=20"`
	Decimal5          int32               `parquet:"name=decimal5, type=INT32, scale=2, precision=9, logicaltype=DECIMAL, logicaltype.precision=9, logicaltype.scale=2"`
	DecimalPointer    *string             `parquet:"name=DecimalPointer, type=FIXED_LEN_BYTE_ARRAY, convertedtype=DECIMAL, scale=2, precision=10, length=12, repetitiontype=OPTIONAL"`
	Map               map[string]int32    `parquet:"name=Map, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=INT32"`
	List              []string            `parquet:"name=List, type=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	Repeated          []int32             `parquet:"name=Repeated, type=INT32, repetitiontype=REPEATED"`
	NestedMap         map[string]InnerMap `parquet:"name=NestedMap, type=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=STRUCT"`
	NestedList        []InnerMap          `parquet:"name=NestedList, type=LIST, valuetype=STRUCT"`
	// New geospatial logical types
	Geometry  string `parquet:"name=Geometry, type=BYTE_ARRAY, logicaltype=GEOMETRY, logicaltype.crs=OGC:CRS84"`
	Geography string `parquet:"name=Geography, type=BYTE_ARRAY, logicaltype=GEOGRAPHY, logicaltype.crs=OGC:CRS84, logicaltype.algorithm=SPHERICAL"`
	// Note: VARIANT is not widely supported by Apache tooling; use JSON for compatibility
	Variant string `parquet:"name=Variant, type=BYTE_ARRAY, logicaltype=VARIANT, logicaltype.specification_version=1"`
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

// float32ToFloat16 converts a float32 into IEEE 754 binary16 (big-endian 2 bytes)
func float32ToFloat16(f float32) string {
	// Handle NaN/Inf explicitly
	if f != f { // NaN
		return string([]byte{0x7e, 0x00})
	}
	if f > 65504 { // max half
		return string([]byte{0x7c, 0x00})
	}
	if f < -65504 {
		return string([]byte{0xfc, 0x00})
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
				return string([]byte{byte(half >> 8), byte(half)})
			}
		}
		half = (sign << 15) | (uint16(exp) << 10) | uint16(mant>>13)
	}
	return string([]byte{byte(half >> 8), byte(half)})
}

func main() {
	fw, err := local.NewLocalFileWriter("/tmp/all-types.parquet")
	if err != nil {
		fmt.Println("Can't create local file", err)
		return
	}

	// Decode GEOMETRY to GeoJSON and include raw WKB hex in JSON output
	types.SetGeometryJSONMode(types.GeospatialModeHybrid)
	types.SetGeospatialHybridRawBase64(false)
	// Use default coordinate precision (6 decimals) for GeoJSON
	// types.SetGeospatialCoordinatePrecision(6)

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
			Utf8:              fmt.Sprintf("UTF8-%d", i),
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
			Variant:           `{"type":"Example","value":` + strI + `}`,
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

// sampleGeometry returns varied geometry types across rows
func sampleGeometry(i int) string {
	switch i % 5 {
	case 0:
		return wkbPoint(float64(i), float64(i)+1)
	case 1:
		return wkbLineString([][2]float64{{0, 0}, {float64(i), float64(i)}})
	case 2:
		return wkbPolygon([][][2]float64{{{0, 0}, {float64(i), 0}, {float64(i), float64(i)}, {0, float64(i)}, {0, 0}}})
	case 3:
		return wkbMultiPoint([][2]float64{{float64(i), float64(i) + 0.1}, {float64(i) + 1, float64(i) + 1}})
	default:
		return wkbMultiLineString([][][2]float64{{{0, 0}, {1, 1}}, {{2, 2}, {3, 3}}})
	}
}

// sampleGeography returns varied geography types across rows
func sampleGeography(i int) string {
	switch i % 5 {
	case 0:
		return wkbPoint(-122.4+float64(i)*0.01, 37.8+float64(i)*0.01)
	case 1:
		return wkbLineString([][2]float64{{-122.4, 37.8}, {-122.41, 37.81}})
	case 2:
		return wkbPolygon([][][2]float64{{{-122.5, 37.8}, {-122.4, 37.8}, {-122.4, 37.9}, {-122.5, 37.9}, {-122.5, 37.8}}})
	case 3:
		return wkbMultiPoint([][2]float64{{-122.4, 37.8}, {-122.41, 37.81}, {-122.42, 37.82}})
	default:
		return wkbMultiPolygon([][][][2]float64{
			{{{-122.5, 37.8}, {-122.4, 37.8}, {-122.4, 37.9}, {-122.5, 37.9}, {-122.5, 37.8}}},
			{{{-122.45, 37.85}, {-122.44, 37.85}, {-122.44, 37.86}, {-122.45, 37.86}, {-122.45, 37.85}}},
		})
	}
}
