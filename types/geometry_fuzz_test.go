package types

import "testing"

// geospatialModes enumerates the rendering modes so the fuzzer can reach each
// branch of the geometry/geography converters.
var geospatialModes = []GeospatialJSONMode{
	GeospatialModeHex,
	GeospatialModeBase64,
	GeospatialModeGeoJSON,
	GeospatialModeHybrid,
}

func FuzzConvertGeometryLogicalValue(f *testing.F) {
	f.Add([]byte{}, 0)
	f.Add([]byte{0x01, 0x01, 0x00}, 2) // truncated point, GeoJSON mode
	// LE point at (1,2)
	f.Add([]byte{
		0x01,
		0x01, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
	}, 2)

	f.Fuzz(func(t *testing.T, b []byte, modeSel int) {
		if modeSel < 0 {
			modeSel = -modeSel
		}
		cfg := DefaultGeospatialConfig()
		cfg.GeometryJSONMode = geospatialModes[modeSel%len(geospatialModes)]
		_ = ConvertGeometryLogicalValue(b, nil, cfg)
	})
}
