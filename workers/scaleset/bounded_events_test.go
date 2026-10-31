//go:build testing

package scaleset

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	cp "github.com/cloudbase/garm-provider-common/params"
	dc "github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/database/watcher"
	"github.com/cloudbase/garm/params"
	wc "github.com/cloudbase/garm/workers/common"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type backlogStore struct {
	dc.Store
	mu         sync.Mutex
	set        params.ScaleSet
	deleted    int
	snapshots  int
	onSnapshot func()
	failures   int
}

type controllerSnapshotStore struct {
	dc.Store
	entity params.ForgeEntity
	set    params.ScaleSet
}

func (s *controllerSnapshotStore) GetForgeEntity(context.Context, params.ForgeEntityType, string) (params.ForgeEntity, error) {
	return s.entity, nil
}
func (s *controllerSnapshotStore) ListEntityScaleSets(context.Context, params.ForgeEntity) ([]params.ScaleSet, error) {
	return []params.ScaleSet{{ID: s.set.ID}}, nil
}
func (s *controllerSnapshotStore) GetScaleSetByID(context.Context, uint) (params.ScaleSet, error) {
	return s.set, nil
}

func TestControllerSnapshotRefreshesConfigurationAndRemovesMissingEntries(t *testing.T) {
	entity := params.ForgeEntity{ID: uuid.NewString(), EntityType: params.ForgeEntityTypeOrganization}
	store := &controllerSnapshotStore{entity: entity, set: params.ScaleSet{ID: 7, OrgID: entity.ID, Name: "updated", ExtraSpecs: []byte(`{"new":true}`), Instances: []params.Instance{{ID: "large-api-snapshot"}}}}
	c := &Controller{ctx: context.Background(), Entity: entity, store: store, running: true}
	c.ScaleSets.Store(uint(7), &scaleSet{scaleSet: params.ScaleSet{ID: 7, Name: "old"}, worker: &Worker{running: true}})
	c.ScaleSets.Store(uint(8), &scaleSet{scaleSet: params.ScaleSet{ID: 8}})
	require.NoError(t, c.reloadSnapshot())
	current, _ := c.ScaleSets.Load(uint(7))
	entry := current.(*scaleSet)
	require.Equal(t, "updated", entry.scaleSet.Name)
	require.JSONEq(t, `{"new":true}`, string(entry.scaleSet.ExtraSpecs))
	require.Empty(t, entry.scaleSet.Instances)
	_, exists := c.ScaleSets.Load(uint(8))
	require.False(t, exists)
}

func (s *backlogStore) GetScaleSetByID(context.Context, uint) (params.ScaleSet, error) {
	if s.onSnapshot != nil {
		s.onSnapshot()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshots++
	if s.failures > 0 {
		s.failures--
		return params.ScaleSet{}, fmt.Errorf("temporary snapshot error")
	}
	return s.set, nil
}
func (s *backlogStore) DeleteInstanceByName(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted++
	return nil
}

func TestWorkerBacklogOverflowReconcilesTerminalDatabaseRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watcher.InitWatcher(ctx)
	defer watcher.CloseWatcher()
	id := uuid.NewString()
	store := &backlogStore{set: params.ScaleSet{ID: 7, OrgID: id, Name: "bounded", ProviderName: "mock"}}
	consumer, err := watcher.RegisterReconciledConsumer(ctx, "bounded-worker")
	require.NoError(t, err)
	defer consumer.Close()
	producer, err := watcher.RegisterProducer(ctx, "bounded-producer")
	require.NoError(t, err)
	defer producer.Close()
	quit := make(chan struct{})
	w := &Worker{ctx: ctx, store: store, consumer: consumer, quit: quit, scaleSet: store.set, runners: map[string]params.Instance{}, eventQueue: wc.NewBoundedChan[dc.ChangePayload](ctx, quit, 4)}
	processed, drained := make(chan struct{}), make(chan struct{})
	w.mux.Lock()
	go func() { defer close(processed); w.eventQueue.Process(w.handleEvent) }()
	go func() { defer close(drained); w.loop() }()
	pending := params.Instance{ID: "runner", Name: "runner", ScaleSetID: 7, Status: cp.InstancePendingDelete, UpdatedAt: time.Now().Add(-time.Minute)}
	// Drive the actual watcher and worker drain loops beyond both queue bounds
	// while handler progress is blocked. Authoritative state changes separately.
	for i := 0; i < 1200; i++ {
		require.NoError(t, producer.Notify(dc.ChangePayload{EntityType: dc.InstanceEntityType, Operation: dc.UpdateOperation, Payload: pending}))
	}
	deleted := pending
	deleted.Status = cp.InstanceDeleted
	deleted.UpdatedAt = time.Now()
	store.mu.Lock()
	store.set.Instances = []params.Instance{deleted}
	store.mu.Unlock()
	require.NoError(t, producer.Notify(dc.ChangePayload{EntityType: dc.InstanceEntityType, Operation: dc.UpdateOperation, Payload: deleted}))
	require.LessOrEqual(t, len(w.eventQueue.In()), 4)
	require.LessOrEqual(t, watcher.GetWatcher().Metrics().ConsumerQueueDepths["bounded-worker"], 256)
	w.mux.Unlock()
	require.Eventually(t, func() bool { store.mu.Lock(); defer store.mu.Unlock(); return store.snapshots > 0 && store.deleted > 0 }, 5*time.Second, time.Millisecond)
	require.Eventually(t, func() bool { w.mux.Lock(); defer w.mux.Unlock(); _, exists := w.runners["runner"]; return !exists }, time.Second, time.Millisecond)
	w.mux.Lock()
	require.Empty(t, w.scaleSet.Instances)
	w.mux.Unlock()
	close(quit)
	<-processed
	<-drained
}

func TestSnapshotRejectsOlderQueuedInstanceUpdates(t *testing.T) {
	now := time.Now()
	w := &Worker{ctx: context.Background(), snapshotStarted: now, runners: map[string]params.Instance{}}
	w.handleInstanceEntityEvent(dc.ChangePayload{Operation: dc.UpdateOperation, Payload: params.Instance{ID: "gone", Status: cp.InstanceRunning, UpdatedAt: now.Add(-time.Second)}})
	require.Empty(t, w.runners, "a pre-snapshot notification must not resurrect an absent row")
}

func TestSnapshotPreservesCreatesCompletedDuringDatabaseRead(t *testing.T) {
	store := &backlogStore{set: params.ScaleSet{ID: 7, Enabled: true, MaxRunners: 10}}
	w := &Worker{ctx: context.Background(), store: store, scaleSet: store.set, runners: map[string]params.Instance{}}
	store.onSnapshot = func() {
		w.mux.Lock()
		w.runners["new"] = params.Instance{ID: "new", Status: cp.InstancePendingCreate, UpdatedAt: time.Now()}
		w.mux.Unlock()
	}
	require.NoError(t, w.reloadSnapshot())
	require.Contains(t, w.runners, "new")
}

func TestResyncRetriesUntilTerminalStateIsRecovered(t *testing.T) {
	store := &backlogStore{failures: 1, set: params.ScaleSet{ID: 7, Instances: []params.Instance{{ID: "deleted", Name: "deleted", Status: cp.InstanceDeleted}}}}
	w := &Worker{ctx: context.Background(), quit: make(chan struct{}), store: store, scaleSet: store.set, runners: map[string]params.Instance{}}
	w.reconcileSnapshot()
	require.Equal(t, 2, store.snapshots)
	require.Equal(t, 1, store.deleted)
	require.Empty(t, w.runners)
}
