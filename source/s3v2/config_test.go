package s3v2

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// Missing shared files and no selected profile load as an empty shared configuration, so a developer's profiles cannot break config loading.
	for _, name := range []string{"AWS_PROFILE", "AWS_DEFAULT_PROFILE"} {
		if err := os.Unsetenv(name); err != nil {
			panic(err)
		}
	}
	for _, name := range []string{"AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE"} {
		if err := os.Setenv(name, "/nonexistent/parquet-go-test-aws"); err != nil {
			panic(err)
		}
	}
	os.Exit(m.Run())
}

func TestGetConfig(t *testing.T) {
	cfg, err := getConfig()
	require.NoError(t, err)
	require.NotEmpty(t, cfg.DefaultsMode)
	require.NotEmpty(t, cfg.AccountIDEndpointMode)
}
