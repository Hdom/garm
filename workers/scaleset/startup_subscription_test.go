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
	"github.com/cloudbase/garm/database/watcher"
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
type startupStore struct {
	dc.Store
	listErr error
	mu      sync.Mutex
	row     params.Instance
	listed  int
	deletes int
	creates int
	updates []string
}

func (s *startupStore) ControllerInfo(context.Context) (params.ControllerInfo, error) {
	return params.ControllerInfo{}, nil
}
func (s *startupStore) ListScaleSetInstances(context.Context, uint, bool) ([]params.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listed++
	if s.listErr != nil {
		return nil, s.listErr
	}
	return []params.Instance{s.row}, nil
}
func (s *startupStore) DeleteInstanceByName(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes++
	return nil
}
func (s *startupStore) UpdateInstance(_ context.Context, name string, p params.UpdateInstanceParams) (params.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates = append(s.updates, name)
	return params.Instance{ID: name, Name: name, Status: p.Status}, nil
}
func (s *startupStore) CreateScaleSetInstance(_ context.Context, _ uint, p params.CreateInstanceParams) (params.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.creates++
	return params.Instance{ID: p.Name, Name: p.Name, Status: p.Status}, nil
}

// A synchronous watcher makes the startup ordering deterministic: no replay is
// performed, and a notification before registration has no recipient.
type startupWatcher struct {
	gt.MockWatcher
	registered bool
	delivered  int
	consumer   *startupConsumer
}
type startupConsumer struct {
	gt.MockConsumer
	closed bool
	ch     chan dc.ChangePayload
}

func (c *startupConsumer) Close() { c.closed = true }

func (c *startupConsumer) Watch() <-chan dc.ChangePayload { return c.ch }
func (v *startupWatcher) RegisterConsumer(context.Context, string, ...dc.PayloadFilterFunc) (dc.Consumer, error) {
	v.registered = true
	return v.consumer, nil
}
func (v *startupWatcher) emit(e dc.ChangePayload) {
	if v.registered {
		v.consumer.ch <- e
		v.delivered++
	}
}

func startupWorker(t *testing.T) (*Worker, *startupStore) {
	t.Helper()
	locker, _ := locking.NewLocalLocker(context.Background(), nil)
	_ = locking.RegisterLocker(locker)
	entity := params.ForgeEntity{ID: uuid.NewString(), EntityType: params.ForgeEntityTypeOrganization, Owner: "local-only", Name: "local-only"}
	s := &startupStore{}
	w := &Worker{ctx: context.Background(), store: s, entity: entity, consumerID: uuid.NewString(), quit: make(chan struct{}), runners: map[string]params.Instance{}, scaleSet: params.ScaleSet{ID: 7, ScaleSetID: 70, Name: "verify", OrgID: entity.ID, Enabled: true, MaxRunners: 10, DesiredRunnerCount: 1}}
	return w, s
}

// All scale-set API traffic goes to httptest; GitHub is never contacted.
func startupForge(t *testing.T, w *Worker, beforeLookup func(), onDelete func(int64) int) *atomic.Int64 {
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
	cli.On("GetEntity").Return(entity)
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

func TestStartupSubscriptionRetainsConcurrentDeletion(t *testing.T) {
	w, s := startupWorker(t)
	w.scaleSet.Enabled = false
	w.scaleSet.DesiredRunnerCount = 0
	s.row = params.Instance{ID: "startup-row", Name: uuid.NewString(), Status: cp.InstancePendingDelete, ScaleSetID: w.scaleSet.ID}
	vw := &startupWatcher{consumer: &startupConsumer{ch: make(chan dc.ChangePayload, 8)}}
	watcher.SetWatcher(vw)
	defer watcher.SetWatcher(nil)
	startupForge(t, w, func() {
		require.True(t, vw.registered, "subscription must precede the snapshot and forge call")
		s.mu.Lock()
		s.row.Status = cp.InstanceDeleted
		deleted := s.row
		s.mu.Unlock()
		vw.emit(dc.ChangePayload{EntityType: dc.InstanceEntityType, Operation: dc.UpdateOperation, Payload: deleted})
	}, nil)
	require.NoError(t, w.Start())
	defer w.Stop()
	require.Equal(t, 1, vw.delivered)
	require.Eventually(t, func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.deletes == 1 }, time.Second, time.Millisecond)
	w.mux.Lock()
	_, exists := w.runners["startup-row"]
	w.mux.Unlock()
	require.False(t, exists)
}

func TestStartupSubscriptionClosesOnSnapshotFailure(t *testing.T) {
	w, s := startupWorker(t)
	s.listErr = fmt.Errorf("snapshot failed")
	vw := &startupWatcher{consumer: &startupConsumer{ch: make(chan dc.ChangePayload, 8)}}
	watcher.SetWatcher(vw)
	defer watcher.SetWatcher(nil)
	require.ErrorContains(t, w.Start(), "snapshot failed")
	require.True(t, vw.registered)
	require.True(t, vw.consumer.closed)
	require.False(t, w.IsRunning())
}
