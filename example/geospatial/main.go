//go:build example

package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"

	"github.com/hangxie/parquet-go/v2/marshal"
	"github.com/hangxie/parquet-go/v2/parquet"
	"github.com/hangxie/parquet-go/v2/reader"
	"github.com/hangxie/parquet-go/v2/source/local"
	"github.com/hangxie/parquet-go/v2/types"
	"github.com/hangxie/parquet-go/v2/writer"
)

type Row struct {
	// Geometry (planar) in EPSG:3857 using WKB bytes
	Geom string `parquet:"name=Geom, type=BYTE_ARRAY, logicaltype=GEOMETRY"`
	// Geography (spherical) with interpolation algorithm
	Geog string `parquet:"name=Geog, type=BYTE_ARRAY, logicaltype=GEOGRAPHY"`
	// Additional geometry types for comprehensive testing
	GeomType string `parquet:"name=GeomType, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func main() {
	// Configure JSON output: GeoJSON + include hex alongside for Geometry
	types.SetGeometryJSONMode(types.GeospatialModeHybrid)
	types.SetGeographyJSONMode(types.GeospatialModeGeoJSON)
	// Example reprojection hook: (no-op) here for demo; plug in your own
	types.SetGeospatialReprojector(func(crs string, gj map[string]any) (map[string]any, bool) {
		// Implement CRS->CRS84 reprojection here if needed
		return nil, false
	})

	// Write a few rows
	fw, err := local.NewLocalFileWriter("/tmp/geospatial.parquet")
	if err != nil {
		log.Fatal(err)
	}
	pw, err := writer.NewParquetWriter(fw, new(Row), 1)
	if err != nil {
		log.Fatal(err)
	}
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	rows := []Row{
		{Geom: wkbPoint(1, 2), Geog: wkbLineString([][2]float64{{-122.4, 37.8}, {-122.41, 37.81}}), GeomType: "Point"},
		{Geom: wkbPolygon([][][2]float64{{{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0}}}), Geog: wkbPoint(-0.1276, 51.5074), GeomType: "Polygon"},
		{Geom: wkbMultiPoint([][]float64{{10, 20}, {30, 40}}), Geog: wkbMultiLineString([][][2]float64{{{0, 0}, {1, 1}}, {{2, 2}, {3, 3}}}), GeomType: "MultiPoint"},
		{Geom: wkbMultiLineString([][][2]float64{{{0, 0}, {1, 1}, {2, 2}}, {{10, 10}, {11, 11}}}), Geog: wkbMultiPolygon([][][][2]float64{{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}}, {{{2, 2}, {3, 2}, {3, 3}, {2, 3}, {2, 2}}}}), GeomType: "MultiLineString"},
		{Geom: wkbMultiPolygon([][][][2]float64{{{{0, 0}, {5, 0}, {5, 5}, {0, 5}, {0, 0}}}, {{{10, 10}, {15, 10}, {15, 15}, {10, 15}, {10, 10}}}}), Geog: wkbGeometryCollection([]string{wkbPoint(100, 200), wkbLineString([][2]float64{{300, 400}, {500, 600}})}), GeomType: "MultiPolygon"},
		{Geom: wkbGeometryCollection([]string{wkbPoint(0, 0), wkbLineString([][2]float64{{1, 1}, {2, 2}}), wkbPolygon([][][2]float64{{{3, 3}, {4, 3}, {4, 4}, {3, 4}, {3, 3}}})}), Geog: wkbPoint(0, 0), GeomType: "GeometryCollection"},
	}
	for _, r := range rows {
		if err := pw.Write(r); err != nil {
			log.Fatal(err)
		}
	}
	if err := pw.WriteStop(); err != nil {
		log.Fatal(err)
	}
	_ = fw.Close()

	// Read and convert to JSON-friendly (applies our geospatial JSON mode)
	fr, err := local.NewLocalFileReader("/tmp/geospatial.parquet")
	if err != nil {
		log.Fatal(err)
	}
	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		log.Fatal(err)
	}
	n := int(pr.GetNumRows())
	data, err := pr.ReadByNumber(n)
	if err != nil {
		log.Fatal(err)
	}
	out, err := marshal.ConvertToJSONFriendly(data, pr.SchemaHandler)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%v\n", out)

	// Display geospatial statistics for both geometry columns
	fmt.Println("\nGeospatial Statistics:")
	for i, col := range pr.Footer.RowGroups[0].Columns {
		colName := pr.SchemaHandler.SchemaElements[i+1].Name // +1 to skip root element
		if col.MetaData.GeospatialStatistics != nil {
			geoStats := col.MetaData.GeospatialStatistics
			fmt.Printf("Column '%s':\n", colName)
			if geoStats.Bbox != nil {
				fmt.Printf("  Bounding Box: X[%.2f, %.2f] Y[%.2f, %.2f]\n",
					geoStats.Bbox.Xmin, geoStats.Bbox.Xmax,
					geoStats.Bbox.Ymin, geoStats.Bbox.Ymax)
			}
			if len(geoStats.GeospatialTypes) > 0 {
				fmt.Printf("  Geometry Types: %v\n", geoStats.GeospatialTypes)
			}
		} else {
			fmt.Printf("Column '%s': No geospatial statistics\n", colName)
		}
	}
	pr.ReadStop()
	_ = fr.Close()
}

// Helpers to build minimal WKB (little-endian) for Point/LineString/Polygon
func wkbPrefix(buf []byte, typ uint32) []byte {
	buf = append(buf, 1) // little-endian
	tmp := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp, typ)
	return append(buf, tmp...)
}

func wkbPoint(x, y float64) string {
	buf := wkbPrefix(nil, 1)
	tmp := make([]byte, 8)
	binary.LittleEndian.PutUint64(tmp, mathFloat64bits(x))
	buf = append(buf, tmp...)
	binary.LittleEndian.PutUint64(tmp, mathFloat64bits(y))
	buf = append(buf, tmp...)
	return string(buf)
}

func wkbLineString(coords [][2]float64) string {
	buf := wkbPrefix(nil, 2)
	tmp4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp4, uint32(len(coords)))
	buf = append(buf, tmp4...)
	tmp8 := make([]byte, 8)
	for _, c := range coords {
		binary.LittleEndian.PutUint64(tmp8, mathFloat64bits(c[0]))
		buf = append(buf, tmp8...)
		binary.LittleEndian.PutUint64(tmp8, mathFloat64bits(c[1]))
		buf = append(buf, tmp8...)
	}
	return string(buf)
}

func wkbPolygon(rings [][][2]float64) string {
	buf := wkbPrefix(nil, 3)
	tmp4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp4, uint32(len(rings)))
	buf = append(buf, tmp4...)
	tmp8 := make([]byte, 8)
	for _, ring := range rings {
		binary.LittleEndian.PutUint32(tmp4, uint32(len(ring)))
		buf = append(buf, tmp4...)
		for _, c := range ring {
			binary.LittleEndian.PutUint64(tmp8, mathFloat64bits(c[0]))
			buf = append(buf, tmp8...)
			binary.LittleEndian.PutUint64(tmp8, mathFloat64bits(c[1]))
			buf = append(buf, tmp8...)
		}
	}
	return string(buf)
}

func wkbMultiPoint(points [][]float64) string {
	buf := wkbPrefix(nil, 4)
	tmp4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp4, uint32(len(points)))
	buf = append(buf, tmp4...)
	for _, p := range points {
		buf = append(buf, wkbPoint(p[0], p[1])...)
	}
	return string(buf)
}

func wkbMultiLineString(lines [][][2]float64) string {
	buf := wkbPrefix(nil, 5)
	tmp4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp4, uint32(len(lines)))
	buf = append(buf, tmp4...)
	for _, line := range lines {
		buf = append(buf, wkbLineString(line)...)
	}
	return string(buf)
}

func wkbMultiPolygon(polygons [][][][2]float64) string {
	buf := wkbPrefix(nil, 6)
	tmp4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp4, uint32(len(polygons)))
	buf = append(buf, tmp4...)
	for _, poly := range polygons {
		buf = append(buf, wkbPolygon(poly)...)
	}
	return string(buf)
}

func wkbGeometryCollection(geometries []string) string {
	buf := wkbPrefix(nil, 7)
	tmp4 := make([]byte, 4)
	binary.LittleEndian.PutUint32(tmp4, uint32(len(geometries)))
	buf = append(buf, tmp4...)
	for _, geom := range geometries {
		buf = append(buf, geom...)
	}
	return string(buf)
}

func mathFloat64bits(f float64) uint64 { return mathFloat64bitsStd(f) }

// keep math import localized
func mathFloat64bitsStd(f float64) uint64 { return math.Float64bits(f) }
