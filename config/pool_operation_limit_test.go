package config

import (
	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPoolOperationLimitDefaultsAndValidation(t *testing.T) {
	var omitted Default
	require.NoError(t, omitted.Validate())
	require.Equal(t, 10, omitted.PoolOperationLimit())
	for _, text := range []string{"max_concurrent_pool_operations = 0", "max_concurrent_pool_operations = -1"} {
		var d Default
		_, err := toml.Decode(text, &d)
		require.NoError(t, err)
		require.ErrorContains(t, d.Validate(), "must be positive")
	}
	var d Default
	_, err := toml.Decode("max_concurrent_pool_operations = 3", &d)
	require.NoError(t, err)
	require.NoError(t, d.Validate())
	require.Equal(t, 3, d.PoolOperationLimit())
}
