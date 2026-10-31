//go:build testing

package scaleset

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cp "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/cache"
	dc "github.com/cloudbase/garm/database/common"
	gt "github.com/cloudbase/garm/internal/testing"
	"github.com/cloudbase/garm/locking"
	"github.com/cloudbase/garm/params"
	"github.com/cloudbase/garm/runner/common/mocks"
	"github.com/google/go-github/v84/github"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Embedded interfaces deliberately panic on any unplanned remote/store call.
type capacityStore struct {
	dc.Store
	mu      sync.Mutex
	row     params.Instance
	listed  int
	deletes int
	creates int
	updates []string
}

func (s *capacityStore) ControllerInfo(context.Context) (params.ControllerInfo, error) {
	return params.ControllerInfo{}, nil
}
func (s *capacityStore) ListScaleSetInstances(context.Context, uint, bool) ([]params.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listed++
	return []params.Instance{s.row}, nil
}
func (s *capacityStore) DeleteInstanceByName(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes++
	return nil
}
func (s *capacityStore) UpdateInstance(_ context.Context, name string, p params.UpdateInstanceParams) (params.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates = append(s.updates, name)
	return params.Instance{ID: name, Name: name, Status: p.Status}, nil
}
func (s *capacityStore) CreateScaleSetInstance(_ context.Context, _ uint, p params.CreateInstanceParams) (params.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.creates++
	return params.Instance{ID: p.Name, Name: p.Name, Status: p.Status}, nil
}

// A synchronous watcher makes the startup ordering deterministic: no replay is
// performed, and a notification before registration has no recipient.
type capacityWatcher struct {
	gt.MockWatcher
	registered bool
	delivered  int
	consumer   *capacityConsumer
}
type capacityConsumer struct {
	gt.MockConsumer
	ch chan dc.ChangePayload
}

func (c *capacityConsumer) Watch() <-chan dc.ChangePayload { return c.ch }
func (v *capacityWatcher) RegisterConsumer(context.Context, string, ...dc.PayloadFilterFunc) (dc.Consumer, error) {
	v.registered = true
	return v.consumer, nil
}
func (v *capacityWatcher) emit(e dc.ChangePayload) {
	if v.registered {
		v.consumer.ch <- e
		v.delivered++
	}
}

func capacityWorker(t *testing.T) (*Worker, *capacityStore) {
	t.Helper()
	locker, _ := locking.NewLocalLocker(context.Background(), nil)
	_ = locking.RegisterLocker(locker)
	entity := params.ForgeEntity{ID: uuid.NewString(), EntityType: params.ForgeEntityTypeOrganization, Owner: "local-only", Name: "local-only"}
	s := &capacityStore{}
	w := &Worker{ctx: context.Background(), store: s, entity: entity, consumerID: uuid.NewString(), quit: make(chan struct{}), runners: map[string]params.Instance{}, scaleSet: params.ScaleSet{ID: 7, ScaleSetID: 70, Name: "verify", OrgID: entity.ID, Enabled: true, MaxRunners: 10, DesiredRunnerCount: 1}}
	return w, s
}

// All scale-set API traffic goes to httptest; GitHub is never contacted.
func capacityForge(t *testing.T, w *Worker, beforeLookup func(), onDelete func(int64) int) *atomic.Int64 {
	t.Helper()
	jitCalls := &atomic.Int64{}
	var server *httptest.Server
	jwt := "eyJhbGciOiJIUzI1NiJ9." + base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d}`, time.Now().Add(time.Hour).Unix()))) + ".c2ln"
	server = httptest.NewServer(http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		resp.Header().Set("Content-Type", "application/json")
		switch {
		case req.URL.Path == "/actions/runner-registration":
			json.NewEncoder(resp).Encode(map[string]string{"url": server.URL, "token": jwt})
		case strings.HasSuffix(req.URL.Path, "/generatejitconfig"):
			jitCalls.Add(1)
			json.NewEncoder(resp).Encode(params.RunnerScaleSetJitRunnerConfig{Runner: &params.RunnerReference{ID: 99}, EncodedJITConfig: base64.StdEncoding.EncodeToString([]byte(`{"test":"fake-jit"}`))})
		case req.Method == http.MethodDelete:
			var id int64
			fmt.Sscanf(req.URL.Path, "/_apis/distributedtask/pools/0/agents/%d", &id)
			status := http.StatusNoContent
			if onDelete != nil {
				status = onDelete(id)
			}
			resp.WriteHeader(status)
		case strings.HasSuffix(req.URL.Path, "/runnerscalesets"):
			json.NewEncoder(resp).Encode(map[string]any{"count": 1, "value": []params.RunnerScaleSet{{ID: w.scaleSet.ScaleSetID, Name: w.scaleSet.Name}}})
		default:
			t.Errorf("unexpected local API request %s %s", req.Method, req.URL.Path)
			http.Error(resp, "unexpected request", 500)
		}
	}))
	t.Cleanup(server.Close)
	cli := mocks.NewGithubClient(t)
	entity := w.entity
	entity.Credentials.BaseURL = server.URL
	cli.On("GetEntity").Return(entity).Maybe()
	base, _ := url.Parse(server.URL)
	cli.On("GithubBaseURL").Return(base).Maybe()
	cli.On("CreateEntityRegistrationToken", mock.Anything).Return(&github.RegistrationToken{Token: github.Ptr("local-token"), ExpiresAt: &github.Timestamp{Time: time.Now().Add(time.Hour)}}, (*github.Response)(nil), nil).Maybe()
	cli.On("GetEntityRunnerGroupIDByName", mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		if beforeLookup != nil {
			beforeLookup()
		}
	}).Return(int64(1), nil).Maybe()
	cache.SetGithubClient(w.entity.ID, cli)
	t.Cleanup(func() { cache.DeleteGithubClient(w.entity.ID) })
	return jitCalls
}

func TestCleanupStatesAllowReplacementWithinPhysicalMaximum(t *testing.T) {
	for _, state := range []cp.InstanceStatus{cp.InstancePendingDelete, cp.InstancePendingForceDelete, cp.InstanceDeleting, cp.InstanceError, cp.InstanceRunning} {
		t.Run(string(state), func(t *testing.T) {
			w, store := capacityWorker(t)
			calls := capacityForge(t, w, nil, nil)
			w.runners["old"] = params.Instance{ID: "old", Name: "old", Status: state, RunnerStatus: params.RunnerTerminated}
			require.Equal(t, 1, w.runnerCount())
			require.Zero(t, w.runnableRunnerCount())
			w.handleScaleUp()
			require.EqualValues(t, 1, calls.Load())
			require.Equal(t, 1, store.creates)
			require.Equal(t, 2, w.runnerCount())
			require.Equal(t, 1, w.runnableRunnerCount())
			w.handleScaleUp()
			require.EqualValues(t, 1, calls.Load(), "do not replace twice")
		})
	}
}

func TestReplacementWaitsForPhysicalCapacity(t *testing.T) {
	w, store := capacityWorker(t)
	calls := capacityForge(t, w, nil, nil)
	w.scaleSet.MaxRunners = 1
	w.runners["old"] = params.Instance{ID: "old", Status: cp.InstanceDeleting, RunnerStatus: params.RunnerTerminated}
	w.handleScaleUp()
	require.Zero(t, calls.Load())
	require.Zero(t, store.creates)
	w.runners["old"] = params.Instance{ID: "old", Status: cp.InstanceDeleted}
	w.handleScaleUp()
	require.EqualValues(t, 1, calls.Load())
	require.Equal(t, 1, w.runnerCount())
}

func TestProvisioningCountsAsFutureRunnableCapacity(t *testing.T) {
	w, _ := capacityWorker(t)
	for _, state := range []cp.InstanceStatus{cp.InstancePendingCreate, cp.InstanceCreating, cp.InstanceRunning} {
		w.runners = map[string]params.Instance{"new": {Status: state, RunnerStatus: params.RunnerPending}}
		require.Equal(t, 1, w.runnableRunnerCount())
	}
}

func TestScaleDownPreservesBootstrappingWithSeparateCapacityCounts(t *testing.T) {
	for _, alreadyDeleting := range []bool{false, true} {
		t.Run(fmt.Sprint(alreadyDeleting), func(t *testing.T) {
			w, s := capacityWorker(t)
			var removed []int64
			capacityForge(t, w, nil, func(id int64) int { removed = append(removed, id); return http.StatusNoContent })
			w.scaleSet.DesiredRunnerCount = 2
			w.runners = map[string]params.Instance{
				"pending":    {ID: "pending", Name: uuid.NewString(), Status: cp.InstanceRunning, RunnerStatus: params.RunnerPending, AgentID: 1},
				"installing": {ID: "installing", Name: uuid.NewString(), Status: cp.InstanceRunning, RunnerStatus: params.RunnerInstalling, AgentID: 2},
				"idle":       {ID: "idle", Name: uuid.NewString(), Status: cp.InstanceRunning, RunnerStatus: params.RunnerIdle, AgentID: 3},
			}
			if alreadyDeleting {
				w.runners["deleting"] = params.Instance{ID: "deleting", Name: uuid.NewString(), Status: cp.InstanceDeleting}
				w.scaleSet.DesiredRunnerCount = 3
			}
			w.handleScaleDown()
			if alreadyDeleting {
				require.Empty(t, removed)
				require.Empty(t, s.updates)
			} else {
				require.Equal(t, []int64{3}, removed)
				require.Len(t, s.updates, 1)
			}

		})
	}
}
