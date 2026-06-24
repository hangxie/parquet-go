package types

import "testing"

func FuzzConvertGeographyLogicalValue(f *testing.F) {
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
		cfg.GeographyJSONMode = geospatialModes[modeSel%len(geospatialModes)]
		_ = ConvertGeographyLogicalValue(b, nil, cfg)
	})
}
