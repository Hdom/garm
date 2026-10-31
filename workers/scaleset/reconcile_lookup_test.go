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
type lookupStore struct {
	dc.Store
	mu      sync.Mutex
	row     params.Instance
	listed  int
	deletes int
	creates int
	updates []string
}

func (s *lookupStore) ControllerInfo(context.Context) (params.ControllerInfo, error) {
	return params.ControllerInfo{}, nil
}
func (s *lookupStore) ListScaleSetInstances(context.Context, uint, bool) ([]params.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listed++
	return []params.Instance{s.row}, nil
}
func (s *lookupStore) DeleteInstanceByName(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletes++
	return nil
}
func (s *lookupStore) UpdateInstance(_ context.Context, name string, p params.UpdateInstanceParams) (params.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates = append(s.updates, name)
	return params.Instance{ID: name, Name: name, Status: p.Status}, nil
}
func (s *lookupStore) CreateScaleSetInstance(_ context.Context, _ uint, p params.CreateInstanceParams) (params.Instance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.creates++
	return params.Instance{ID: p.Name, Name: p.Name, Status: p.Status}, nil
}

// A synchronous watcher makes the startup ordering deterministic: no replay is
// performed, and a notification before registration has no recipient.
type lookupWatcher struct {
	gt.MockWatcher
	registered bool
	delivered  int
	consumer   *lookupConsumer
}
type lookupConsumer struct {
	gt.MockConsumer
	ch chan dc.ChangePayload
}

func (c *lookupConsumer) Watch() <-chan dc.ChangePayload { return c.ch }
func (v *lookupWatcher) RegisterConsumer(context.Context, string, ...dc.PayloadFilterFunc) (dc.Consumer, error) {
	v.registered = true
	return v.consumer, nil
}
func (v *lookupWatcher) emit(e dc.ChangePayload) {
	if v.registered {
		v.consumer.ch <- e
		v.delivered++
	}
}

func lookupWorker(t *testing.T) (*Worker, *lookupStore) {
	t.Helper()
	locker, _ := locking.NewLocalLocker(context.Background(), nil)
	_ = locking.RegisterLocker(locker)
	entity := params.ForgeEntity{ID: uuid.NewString(), EntityType: params.ForgeEntityTypeOrganization, Owner: "local-only", Name: "local-only"}
	s := &lookupStore{}
	w := &Worker{ctx: context.Background(), store: s, entity: entity, consumerID: uuid.NewString(), quit: make(chan struct{}), runners: map[string]params.Instance{}, scaleSet: params.ScaleSet{ID: 7, ScaleSetID: 70, Name: "verify", OrgID: entity.ID, Enabled: true, MaxRunners: 10, DesiredRunnerCount: 1}}
	return w, s
}

// All scale-set API traffic goes to httptest; GitHub is never contacted.
func lookupForge(t *testing.T, w *Worker, beforeLookup func(), onDelete func(int64) int, onGet func() (int, params.RunnerReference)) *atomic.Int64 {
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
		case req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/agents/"):
			status, runner := onGet()
			resp.WriteHeader(status)
			if status == http.StatusOK {
				json.NewEncoder(resp).Encode(runner)
			}
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

func TestMissingListEntryRequiresDirectNotFoundBeforeCleanup(t *testing.T) {
	cases := []struct {
		name             string
		lookupStatus     int
		runner           params.RunnerReference
		deleteStatus     int
		deletes, updates int
	}{
		{name: "busy runner", lookupStatus: 200, runner: params.RunnerReference{Status: "online", Busy: true}},
		{name: "idle runner", lookupStatus: 200, runner: params.RunnerReference{Status: "online"}},
		{name: "offline runner still exists", lookupStatus: 200, runner: params.RunnerReference{Status: "offline"}},
		{name: "lookup unauthorized", lookupStatus: 401},
		{name: "lookup transient failure", lookupStatus: 500},
		{name: "confirmed missing", lookupStatus: 404, deleteStatus: 204, deletes: 1, updates: 1},
		{name: "already removed", lookupStatus: 404, deleteStatus: 404, deletes: 1, updates: 1},
		{name: "became busy after lookup", lookupStatus: 404, deleteStatus: 409, deletes: 1},
		{name: "removal unauthorized", lookupStatus: 404, deleteStatus: 401, deletes: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, store := lookupWorker(t)
			name := uuid.NewString()
			w.runners[name] = params.Instance{ID: name, Name: name, AgentID: 11, Status: cp.InstanceRunning, RunnerStatus: params.RunnerIdle, CreatedAt: time.Now().Add(-2 * time.Minute)}
			gets, deletes := 0, 0
			lookupForge(t, w, nil, func(id int64) int { require.EqualValues(t, 11, id); deletes++; return tc.deleteStatus }, func() (int, params.RunnerReference) { gets++; return tc.lookupStatus, tc.runner })
			provider := mocks.NewProvider(t)
			provider.On("ListInstances", mock.Anything, mock.Anything, mock.Anything).Return([]cp.ProviderInstance{{Name: name, Status: cp.InstanceRunning}}, nil)
			w.provider = provider
			w.legacyPoolIDDrained = true
			require.NoError(t, w.consolidateRunnerState(time.Now(), nil))
			require.Equal(t, 1, gets)
			require.Equal(t, tc.deletes, deletes)
			require.Len(t, store.updates, tc.updates)
		})
	}
}
