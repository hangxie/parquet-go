package encryption

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAAD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		moduleType     ModuleType
		rowGroup       int16
		column         int16
		page           int16
		wantHexEncoded string
	}{
		{
			name:           "footer",
			moduleType:     ModuleFooter,
			wantHexEncoded: "70726566697866696c652d696400",
		},
		{
			name:           "column index",
			moduleType:     ModuleColumnIndex,
			rowGroup:       2,
			column:         3,
			wantHexEncoded: "70726566697866696c652d69640602000300",
		},
		{
			name:           "data page",
			moduleType:     ModuleDataPage,
			rowGroup:       2,
			column:         3,
			page:           4,
			wantHexEncoded: "70726566697866696c652d696402020003000400",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := AAD([]byte("prefix"), []byte("file-id"), tt.moduleType, tt.rowGroup, tt.column, tt.page)
			require.Equal(t, tt.wantHexEncoded, hex.EncodeToString(got))
		})
	}
}
