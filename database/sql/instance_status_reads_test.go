//go:build testing

package sql

import (
	"context"
	"strings"
	"testing"

	cp "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/database/watcher"
	gt "github.com/cloudbase/garm/internal/testing"
	"github.com/cloudbase/garm/params"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/logger"
)

func TestStatusUpdateReadsOnlyRequiredAssociations(t *testing.T) {
	watcher.InitWatcher(context.Background())
	t.Cleanup(func() { watcher.CloseWatcher() })
	store := newTestDB(t)
	db := store.(*sqlDatabase)
	ctx := gt.ImpersonateAdminContext(context.Background(), store, t)
	ep := gt.CreateDefaultGithubEndpoint(ctx, store, t)
	creds := gt.CreateTestGithubCredentials(ctx, "status-read-creds", store, t, ep)
	repo, err := store.CreateRepository(ctx, "status-org", "status-repo", creds, "secret", params.PoolBalancerTypeRoundRobin, false)
	require.NoError(t, err)
	entity, err := repo.GetEntity()
	require.NoError(t, err)
	pool, err := store.CreateEntityPool(ctx, entity, params.CreatePoolParams{ProviderName: "mock", MaxRunners: 5, Image: "img", Flavor: "flv", OSType: cp.Linux, OSArch: cp.Amd64, Tags: []string{"linux"}})
	require.NoError(t, err)
	inst, err := store.CreateInstance(ctx, pool.ID, params.CreateInstanceParams{Name: "status-runner", Status: cp.InstancePendingCreate, RunnerStatus: params.RunnerPending, OSType: cp.Linux, OSArch: cp.Amd64})
	require.NoError(t, err)
	require.NoError(t, db.conn.Create(&InstanceStatusUpdate{InstanceID: uuid.MustParse(inst.ID), Message: "history remains available to API reads"}).Error)
	rec := &stmtRecorder{Interface: logger.Default.LogMode(logger.Silent)}
	db.conn.Logger = rec
	updated, err := store.UpdateInstance(ctx, inst.Name, params.UpdateInstanceParams{Status: cp.InstanceCreating})
	require.NoError(t, err)
	require.Equal(t, cp.InstanceCreating, updated.Status)
	require.Equal(t, "mock", updated.ProviderName)
	for _, q := range rec.stmt {
		require.False(t, strings.Contains(q, "instance_status_updates") || strings.Contains(q, "addresses") || strings.Contains(q, "workflow_jobs"), q)
	}
	// An address update must return the changed addresses, while ordinary status
	// changes need not read them. Full API reads must retain status history.
	updated, err = store.UpdateInstance(ctx, inst.Name, params.UpdateInstanceParams{Addresses: []cp.Address{{Address: "192.0.2.1", Type: cp.AddressType("private")}}})
	require.NoError(t, err)
	require.Len(t, updated.Addresses, 1)
	require.Equal(t, "192.0.2.1", updated.Addresses[0].Address)
	full, err := store.GetInstance(ctx, inst.Name)
	require.NoError(t, err)
	require.Len(t, full.StatusMessages, 1)
	require.Len(t, full.Addresses, 1)
}
