package runner

import (
	"github.com/cloudbase/garm/config"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestControllerSharesPoolOperationLimiter(t *testing.T) {
	limit := 3
	p := &poolManagerCtrl{config: config.Config{Default: config.Default{MaxConcurrentPoolOperations: &limit}}}
	first, err := p.poolOperationLimiter()
	require.NoError(t, err)
	second, err := p.poolOperationLimiter()
	require.NoError(t, err)
	require.Same(t, first, second)
}
