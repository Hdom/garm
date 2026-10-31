package scaleset

import (
	"context"
	"fmt"
	"github.com/cloudbase/garm/cache"
	dc "github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/params"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

type toolsStateStore struct {
	dc.Store
	states []string
	fail   bool
}

func (s *toolsStateStore) UpdateEntityScaleSet(_ context.Context, _ params.ForgeEntity, _ uint, p params.UpdateScaleSetParams, _ func(params.ScaleSet, params.ScaleSet) error) (params.ScaleSet, error) {
	if s.fail {
		return params.ScaleSet{}, fmt.Errorf("database unavailable")
	}
	s.states = append(s.states, *p.ExtendedState)
	return params.ScaleSet{ExtendedState: *p.ExtendedState}, nil
}
func TestToolsWaitPublishesVisibleReasonAndRecovers(t *testing.T) {
	entity := params.ForgeEntity{ID: uuid.NewString()}
	store := &toolsStateStore{}
	w := &Worker{ctx: context.Background(), quit: make(chan struct{}), store: store, scaleSet: params.ScaleSet{ID: 7, OrgID: entity.ID, State: params.ScaleSetCreated}}
	has, stopped := w.waitForToolsOrCancel()
	require.False(t, has)
	require.False(t, stopped)
	require.Equal(t, waitingForToolsReason, w.GetScaleSet().ExtendedState)
	require.Equal(t, []string{waitingForToolsReason}, store.states)
	w.setToolsWaiting(true)
	require.Len(t, store.states, 1, "do not write the same status every retry")
	cache.SetGithubToolsCache(entity, nil)
	defer cache.DeleteGithubToolsCache(entity.ID)
	has, stopped = w.waitForToolsOrCancel()
	require.True(t, has)
	require.False(t, stopped)
	require.Empty(t, w.GetScaleSet().ExtendedState)
	require.Equal(t, []string{waitingForToolsReason, ""}, store.states)
	require.Equal(t, params.ScaleSetCreated, w.GetScaleSet().State)
	close(w.quit)
	start := time.Now()
	_, stopped = w.waitForToolsOrCancel()
	require.True(t, stopped)
	require.Less(t, time.Since(start), 500*time.Millisecond)
}
func TestToolsReadinessPreservesOtherErrorsAndRetriesFailedWrites(t *testing.T) {
	store := &toolsStateStore{}
	w := &Worker{ctx: context.Background(), store: store, scaleSet: params.ScaleSet{ID: 7, OrgID: uuid.NewString(), ExtendedState: "unrelated failure"}}
	w.setToolsWaiting(true)
	w.setToolsWaiting(false)
	require.Empty(t, store.states)
	require.Equal(t, "unrelated failure", w.GetScaleSet().ExtendedState)
	w.scaleSet.ExtendedState = ""
	store.fail = true
	w.setToolsWaiting(true)
	require.Empty(t, w.scaleSet.ExtendedState)
	store.fail = false
	w.setToolsWaiting(true)
	require.Equal(t, []string{waitingForToolsReason}, store.states)
}
