package s3v2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetConfig(t *testing.T) {
	cfg, err := getConfig()
	require.NoError(t, err)
	require.NotEmpty(t, cfg.DefaultsMode)
	require.NotEmpty(t, cfg.AccountIDEndpointMode)
}
