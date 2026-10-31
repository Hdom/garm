package common

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestBoundedChanAppliesPressureAndPreservesOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q := NewBoundedChan[int](ctx, nil, 4)
	for i := 0; i < 4; i++ {
		q.In() <- i
	}
	select {
	case q.In() <- 4:
		t.Fatal("queue exceeded capacity")
	default:
	}
	for i := 0; i < 4; i++ {
		require.Equal(t, i, <-q.Out())
	}
	done := make(chan struct{})
	go func() { defer close(done); q.Process(func(int) {}) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("queue processor ignored cancellation")
	}
}
