package watcher

import (
	"context"
	dc "github.com/cloudbase/garm/database/common"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestBoundedConsumerOverflowRequestsResyncWithoutBlockingPublisher(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &consumer{ctx: ctx, quit: make(chan struct{}), in: make(chan dc.ChangePayload), messages: make(chan dc.ChangePayload), queueLimit: 4}
	go c.dispatch()
	defer c.Close()
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for i := 0; i < 1000; i++ {
			c.Send(dc.ChangePayload{Payload: i})
		}
	}()
	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("slow consumer blocked publisher")
	}
	require.LessOrEqual(t, c.QueueLen(), 4)
	select {
	case event := <-c.Watch():
		require.Equal(t, dc.ResyncEntityType, event.EntityType)
	case <-time.After(time.Second):
		t.Fatal("overflow lost its resync marker")
	}
	// New terminal notifications must still flow after the marker.
	c.Send(dc.ChangePayload{EntityType: dc.InstanceEntityType, Operation: dc.DeleteOperation, Payload: "terminal"})
	select {
	case event := <-c.Watch():
		require.Equal(t, "terminal", event.Payload)
	case <-time.After(time.Second):
		t.Fatal("delivery failed after resync")
	}
}
