package s3v2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_GetConfig(t *testing.T) {
	// Save original config
	originalCfg := cfg
	defer func() {
		cfg = originalCfg
	}()

	newCfg := getConfig()
	require.Equal(t, newCfg, cfg)
	require.NotEmpty(t, cfg.DefaultsMode)
	require.NotEmpty(t, cfg.AccountIDEndpointMode)
}
