package types

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hangxie/parquet-go/v3/parquet"
)

func TestConvertGeographyLogicalValue_Nil(t *testing.T) {
	cfg := DefaultGeospatialConfig()
	result := ConvertGeographyLogicalValue(nil, nil, cfg)
	require.Nil(t, result)
}

func TestConvertGeographyLogicalValue_UnsupportedType(t *testing.T) {
	cfg := DefaultGeospatialConfig()
	result := ConvertGeographyLogicalValue(123, nil, cfg)
	require.Equal(t, 123, result)
}

func TestConvertGeographyLogicalValue_HexMode(t *testing.T) {
	cfg := DefaultGeospatialConfig()
	cfg.GeographyJSONMode = GeospatialModeHex
	result := ConvertGeographyLogicalValue([]byte{0x01, 0x02}, nil, cfg)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	require.Contains(t, m, "wkb_hex")
	require.Equal(t, "OGC:CRS84", m["crs"])
	require.Equal(t, "SPHERICAL", m["algorithm"])
}

func TestConvertGeographyLogicalValue_Base64Mode(t *testing.T) {
	cfg := DefaultGeospatialConfig()
	cfg.GeographyJSONMode = GeospatialModeBase64
	result := ConvertGeographyLogicalValue([]byte{0xAA, 0xBB}, nil, cfg)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	require.Contains(t, m, "wkb_b64")
	require.Equal(t, "OGC:CRS84", m["crs"])
	require.Equal(t, "SPHERICAL", m["algorithm"])
}

func TestConvertGeographyLogicalValue_GeoJSONMode_InvalidWKB(t *testing.T) {
	cfg := DefaultGeospatialConfig()
	cfg.GeographyJSONMode = GeospatialModeGeoJSON
	result := ConvertGeographyLogicalValue([]byte{0xFF}, nil, cfg)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	require.Contains(t, m, "wkb_hex")
}

func TestConvertGeographyLogicalValue_HybridMode_InvalidWKB(t *testing.T) {
	cfg := DefaultGeospatialConfig()
	cfg.GeographyJSONMode = GeospatialModeHybrid
	result := ConvertGeographyLogicalValue([]byte{0xDE, 0xAD}, nil, cfg)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	require.Contains(t, m, "wkb_hex")
}

func TestConvertGeographyLogicalValue_CustomCRSAndAlgorithm(t *testing.T) {
	cfg := DefaultGeospatialConfig()
	cfg.GeographyJSONMode = GeospatialModeHex
	crsStr := "EPSG:4979"
	algo := parquet.EdgeInterpolationAlgorithm_VINCENTY
	geo := &parquet.GeographyType{CRS: &crsStr, Algorithm: &algo}
	result := ConvertGeographyLogicalValue([]byte{0x00}, geo, cfg)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "EPSG:4979", m["crs"])
	require.Equal(t, "VINCENTY", m["algorithm"])
}

func TestConvertGeographyLogicalValue_GeoJSONMode_WithReprojector(t *testing.T) {
	wkb := []byte{
		0x01,
		0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	crsStr := "EPSG:4326"
	geo := &parquet.GeographyType{CRS: &crsStr}
	reprojected := map[string]any{"type": "Point", "coordinates": []float64{5.0, 6.0}}
	cfg := DefaultGeospatialConfig()
	cfg.GeographyJSONMode = GeospatialModeGeoJSON
	cfg.GeoJSONAsFeature = false
	cfg.Reprojector = func(crs string, gj map[string]any) (map[string]any, bool) {
		return reprojected, true
	}
	result := ConvertGeographyLogicalValue(wkb, geo, cfg)
	require.Equal(t, reprojected, result)
}

func TestConvertGeographyLogicalValue_GeoJSONAsFeature(t *testing.T) {
	wkb := []byte{
		0x01,
		0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	cfg := DefaultGeospatialConfig()
	cfg.GeographyJSONMode = GeospatialModeGeoJSON
	cfg.GeoJSONAsFeature = true
	result := ConvertGeographyLogicalValue(wkb, nil, cfg)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "Feature", m["type"])
}

func TestConvertGeographyLogicalValue_HybridMode_ValidWKB_WithReprojector(t *testing.T) {
	wkb := []byte{
		0x01,
		0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	crsStr := "EPSG:9999"
	geo := &parquet.GeographyType{CRS: &crsStr}
	reprojected := map[string]any{"type": "Point", "coordinates": []float64{7.0, 8.0}}
	cfg := DefaultGeospatialConfig()
	cfg.GeographyJSONMode = GeospatialModeHybrid
	cfg.Reprojector = func(crs string, gj map[string]any) (map[string]any, bool) {
		return reprojected, true
	}
	result := ConvertGeographyLogicalValue(wkb, geo, cfg)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	require.Contains(t, m, "crs")
}
