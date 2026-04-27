package types

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestConvertGeometryLogicalValue_Nil(t *testing.T) {
	cfg := DefaultGeospatialConfig()
	result := ConvertGeometryLogicalValue(nil, nil, cfg)
	require.Nil(t, result)
}

func TestConvertGeometryLogicalValue_UnsupportedType(t *testing.T) {
	cfg := DefaultGeospatialConfig()
	result := ConvertGeometryLogicalValue(42, nil, cfg)
	require.Equal(t, 42, result)
}

func TestConvertGeometryLogicalValue_StringInput(t *testing.T) {
	cfg := DefaultGeospatialConfig()
	cfg.GeometryJSONMode = GeospatialModeHex
	result := ConvertGeometryLogicalValue("abc", nil, cfg)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	require.Contains(t, m, "wkb_hex")
	require.Equal(t, "OGC:CRS84", m["crs"])
}

func TestConvertGeometryLogicalValue_BytesInput_HexMode(t *testing.T) {
	cfg := DefaultGeospatialConfig()
	cfg.GeometryJSONMode = GeospatialModeHex
	wkb := []byte{0x01, 0x02, 0x03}
	result := ConvertGeometryLogicalValue(wkb, nil, cfg)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	require.Contains(t, m, "wkb_hex")
}

func TestConvertGeometryLogicalValue_Base64Mode(t *testing.T) {
	cfg := DefaultGeospatialConfig()
	cfg.GeometryJSONMode = GeospatialModeBase64
	wkb := []byte{0x01, 0x02, 0x03}
	result := ConvertGeometryLogicalValue(wkb, nil, cfg)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	require.Contains(t, m, "wkb_b64")
	require.Equal(t, "OGC:CRS84", m["crs"])
}

func TestConvertGeometryLogicalValue_HybridMode_InvalidWKB(t *testing.T) {
	cfg := DefaultGeospatialConfig()
	cfg.GeometryJSONMode = GeospatialModeHybrid
	wkb := []byte{0xFF, 0xFE} // invalid WKB, hybrid falls back to hex
	result := ConvertGeometryLogicalValue(wkb, nil, cfg)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	require.Contains(t, m, "wkb_hex")
}

func TestConvertGeometryLogicalValue_GeoJSONMode_InvalidWKB(t *testing.T) {
	cfg := DefaultGeospatialConfig()
	cfg.GeometryJSONMode = GeospatialModeGeoJSON
	wkb := []byte{0xFF} // invalid WKB, fallback to hex
	result := ConvertGeometryLogicalValue(wkb, nil, cfg)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	require.Contains(t, m, "wkb_hex")
}

func TestConvertGeometryLogicalValue_CustomCRS(t *testing.T) {
	cfg := DefaultGeospatialConfig()
	cfg.GeometryJSONMode = GeospatialModeHex
	crsStr := "EPSG:4326"
	geom := &parquet.GeometryType{CRS: &crsStr}
	result := ConvertGeometryLogicalValue([]byte{0x01}, geom, cfg)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "EPSG:4326", m["crs"])
}

func TestConvertGeometryLogicalValue_GeoJSONMode_AsFeature(t *testing.T) {
	// Use a valid WKB point: Little Endian, type=1 (Point), x=0.0, y=0.0
	wkb := []byte{
		0x01,                   // byte order: little endian
		0x01, 0x00, 0x00, 0x00, // geometry type: Point (1)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 0.0
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 0.0
	}
	cfg := DefaultGeospatialConfig()
	cfg.GeometryJSONMode = GeospatialModeGeoJSON
	cfg.GeoJSONAsFeature = true
	result := ConvertGeometryLogicalValue(wkb, nil, cfg)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "Feature", m["type"])
}

func TestConvertGeometryLogicalValue_GeoJSONMode_WithReprojector(t *testing.T) {
	// Valid WKB point
	wkb := []byte{
		0x01,
		0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	crsStr := "EPSG:4326"
	geom := &parquet.GeometryType{CRS: &crsStr}
	reprojected := map[string]any{"type": "Point", "coordinates": []float64{1.0, 2.0}}
	cfg := DefaultGeospatialConfig()
	cfg.GeometryJSONMode = GeospatialModeGeoJSON
	cfg.GeoJSONAsFeature = false // disable feature wrapping for direct comparison
	cfg.Reprojector = func(crs string, gj map[string]any) (map[string]any, bool) {
		return reprojected, true
	}
	result := ConvertGeometryLogicalValue(wkb, geom, cfg)
	require.Equal(t, reprojected, result)
}
