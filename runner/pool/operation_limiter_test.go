//go:build testing

package pool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cp "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/auth"
	dc "github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/locking"
	"github.com/cloudbase/garm/params"
	rc "github.com/cloudbase/garm/runner/common"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type limitedStore struct {
	dc.Store
	mu          sync.Mutex
	instances   map[string]params.Instance
	pool        params.Pool
	deletedJobs int
}

func (s *limitedStore) ListEntityInstances(context.Context, params.ForgeEntity) ([]params.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ret := []params.Instance{}
	for _, i := range s.instances {
		ret = append(ret, i)
	}
	return ret, nil
}
func (s *limitedStore) GetEntityPool(context.Context, params.ForgeEntity, string) (params.Pool, error) {
	return s.pool, nil
}
func (s *limitedStore) UpdateInstance(_ context.Context, name string, p params.UpdateInstanceParams) (params.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.instances[name]
	i.Status = p.Status
	s.instances[name] = i
	return i, nil
}
func (s *limitedStore) ForceUpdateInstance(ctx context.Context, name string, p params.UpdateInstanceParams) (params.Instance, error) {
	return s.UpdateInstance(ctx, name, p)
}
func (s *limitedStore) DeleteInstance(_ context.Context, _ string, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.instances, name)
	return nil
}
func (s *limitedStore) GetInstance(_ context.Context, name string) (params.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.instances[name], nil
}
func (s *limitedStore) DeleteJob(context.Context, int64) error { s.deletedJobs++; return nil }

type limitedToken struct{ auth.InstanceTokenGetter }

func (limitedToken) NewInstanceJWTToken(params.Instance, params.ForgeEntity, uint) (string, error) {
	return "local-fake-token", nil
}

// A blocking in-memory provider records simultaneous operations. No executable
// provider, runner registration, or compute resources are involved.
type limitedProvider struct {
	rc.Provider
	active  atomic.Int64
	peak    atomic.Int64
	entered chan string
	release chan struct{}
}

func (p *limitedProvider) block(ctx context.Context, operation string) error {
	active := p.active.Add(1)
	defer p.active.Add(-1)
	for {
		peak := p.peak.Load()
		if peak >= active || p.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	p.entered <- operation
	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (p *limitedProvider) CreateInstance(ctx context.Context, b cp.BootstrapInstance, _ rc.CreateInstanceParams) (cp.ProviderInstance, error) {
	err := p.block(ctx, "create")
	return cp.ProviderInstance{Name: b.Name, ProviderID: b.Name, Status: cp.InstanceRunning}, err
}
func (p *limitedProvider) DeleteInstance(ctx context.Context, _ string, _ rc.DeleteInstanceParams) error {
	return p.block(ctx, "delete")
}

func TestPoolOperationLimitSharedAcrossCreatesDeletesAndManagers(t *testing.T) {
	locker, _ := locking.NewLocalLocker(context.Background(), nil)
	_ = locking.RegisterLocker(locker)
	limiter, err := NewInstanceOperationLimiter(4)
	require.NoError(t, err)
	provider := &limitedProvider{entered: make(chan string, 16), release: make(chan struct{})}
	defer close(provider.release)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	makeManager := func(op string, n int) *basePoolManager {
		pool := params.Pool{ID: uuid.NewString(), ProviderName: "mock", TemplateID: 1, ExtraSpecs: []byte(`{"runner_install_template":"ZWNobyBsb2NhbA=="}`)}
		store := &limitedStore{pool: pool, instances: map[string]params.Instance{}}
		for i := 0; i < n; i++ {
			name := fmt.Sprintf("%s-%d", pool.ID, i)
			status := cp.InstancePendingCreate
			if op == "delete" {
				status = cp.InstancePendingDelete
			}
			store.instances[name] = params.Instance{ID: name, Name: name, PoolID: pool.ID, ProviderName: "mock", Status: status}
		}
		backoff, e := locking.NewInstanceDeleteBackoff(ctx)
		require.NoError(t, e)
		return &basePoolManager{ctx: ctx, consumerID: uuid.NewString(), entity: params.ForgeEntity{ID: uuid.NewString(), EntityType: params.ForgeEntityTypeOrganization}, store: store, providers: map[string]rc.Provider{"mock": provider}, instanceTokenGetter: limitedToken{}, quit: make(chan struct{}), backoff: backoff, instanceOperations: limiter}
	}
	creates := makeManager("create", 2)
	deletes := makeManager("delete", 12)
	require.NoError(t, creates.addPendingInstances())
	require.NoError(t, deletes.deletePendingInstances())
	seen := map[string]int{}
	for i := 0; i < 4; i++ {
		select {
		case op := <-provider.entered:
			seen[op]++
		case <-time.After(5 * time.Second):
			t.Fatal("admitted operation never entered provider")
		}
	}
	require.Equal(t, 2, seen["create"])
	require.Equal(t, 2, seen["delete"])
	require.EqualValues(t, 4, provider.peak.Load())
	require.NoError(t, deletes.deletePendingInstances())
	require.Equal(t, 4, len(limiter.slots))
	ds := deletes.store.(*limitedStore)
	ds.mu.Lock()
	pending := 0
	for _, i := range ds.instances {
		if i.Status == cp.InstancePendingDelete {
			pending++
		}
	}
	ds.mu.Unlock()
	require.Equal(t, 10, pending, "unadmitted deletes must stay pending")
	cancel()
	require.Eventually(t, func() bool { return len(limiter.slots) == 0 && provider.active.Load() == 0 }, 5*time.Second, time.Millisecond)
}

func TestPoolOperationLeaseReleasedOnTransitionFailure(t *testing.T) {
	locker, _ := locking.NewLocalLocker(context.Background(), nil)
	_ = locking.RegisterLocker(locker)
	limiter, err := NewInstanceOperationLimiter(1)
	require.NoError(t, err)
	store := &failedTransitionStore{limitedStore: limitedStore{instances: map[string]params.Instance{"new": {Name: uuid.NewString(), Status: cp.InstancePendingCreate}}}}
	r := &basePoolManager{ctx: context.Background(), consumerID: uuid.NewString(), store: store, instanceOperations: limiter}
	require.NoError(t, r.addPendingInstances())
	require.Empty(t, limiter.slots)
	for _, i := range store.instances {
		_, locked := locking.LockedBy(i.Name)
		require.False(t, locked)
	}
}

type failedTransitionStore struct{ limitedStore }

func (*failedTransitionStore) UpdateInstance(context.Context, string, params.UpdateInstanceParams) (params.Instance, error) {
	return params.Instance{}, fmt.Errorf("transition refused")
}

func TestPoolOperationLimiterRejectsInvalidLimits(t *testing.T) {
	for _, n := range []int{0, -1} {
		_, err := NewInstanceOperationLimiter(n)
		require.Error(t, err)
	}
}
