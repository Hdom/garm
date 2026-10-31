//go:build testing

package sql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	cp "github.com/cloudbase/garm-provider-common/params"
	dc "github.com/cloudbase/garm/database/common"
	"github.com/cloudbase/garm/database/watcher"
	gt "github.com/cloudbase/garm/internal/testing"
	"github.com/cloudbase/garm/params"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm/logger"
)

type metadataProducer struct {
	gt.MockProducer
	events []dc.ChangePayload
}

func (p *metadataProducer) Notify(event dc.ChangePayload) error {
	p.events = append(p.events, event)
	return nil
}

func metadataDB(t *testing.T) (*sqlDatabase, context.Context, params.ForgeEntity) {
	t.Helper()
	watcher.InitWatcher(context.Background())
	t.Cleanup(func() { watcher.CloseWatcher() })
	store := newTestDB(t)
	ctx := gt.ImpersonateAdminContext(context.Background(), store, t)
	ep := gt.CreateDefaultGithubEndpoint(ctx, store, t)
	creds := gt.CreateTestGithubCredentials(ctx, "verify-creds", store, t, ep)
	repo, err := store.CreateRepository(ctx, "local-only", "metadata", creds, "secret", params.PoolBalancerTypeRoundRobin, false)
	require.NoError(t, err)
	entity, err := repo.GetEntity()
	require.NoError(t, err)
	return store.(*sqlDatabase), ctx, entity
}

func TestMetadataUpdatesAndCheckpointsExcludeInstanceSnapshots(t *testing.T) {
	db, ctx, entity := metadataDB(t)
	ss, err := db.CreateEntityScaleSet(ctx, entity, params.CreateScaleSetParams{Name: "verify-ss", ProviderName: "mock", MaxRunners: 100, Image: "mock", Flavor: "mock", OSType: cp.Linux, OSArch: cp.Amd64})
	require.NoError(t, err)
	const count = 32
	for i := 0; i < count; i++ {
		_, err = db.CreateScaleSetInstance(ctx, ss.ID, params.CreateInstanceParams{Name: fmt.Sprintf("snapshot-%d", i), Status: cp.InstancePendingCreate, RunnerStatus: params.RunnerPending, OSType: cp.Linux, OSArch: cp.Amd64})
		require.NoError(t, err)
	}
	rec := &stmtRecorder{Interface: logger.Default.LogMode(logger.Silent)}
	db.conn.Logger = rec
	producer := &metadataProducer{}
	db.producer = producer
	oldCount, newCount := 0, 0
	max := uint(101)
	updated, err := db.UpdateEntityScaleSet(ctx, entity, ss.ID, params.UpdateScaleSetParams{MaxRunners: &max}, func(old, new params.ScaleSet) error {
		oldCount = len(old.Instances)
		newCount = len(new.Instances)
		return nil
	})
	require.NoError(t, err)
	require.Zero(t, oldCount)
	require.Zero(t, newCount)
	require.Empty(t, updated.Instances)
	require.Len(t, producer.events, 1)
	require.Equal(t, dc.ScaleSetEntityType, producer.events[0].EntityType)
	require.Empty(t, producer.events[0].Payload.(params.ScaleSet).Instances)
	require.Empty(t, rec.writes("instances"), "write-association fix is independent of the read/payload issue")
	for _, q := range rec.stmt {
		require.False(t, strings.Contains(q, "FROM `instances`") || strings.Contains(q, `FROM "instances"`), q)
	}
	require.NoError(t, db.SetScaleSetLastMessageID(ctx, ss.ID, 42))
	require.NoError(t, db.SetScaleSetDesiredRunnerCount(ctx, ss.ID, 3))
	require.Len(t, producer.events, 3)
	for _, event := range producer.events {
		require.Empty(t, event.Payload.(params.ScaleSet).Instances)
	}
	full, err := db.GetScaleSetByID(ctx, ss.ID)
	require.NoError(t, err)
	require.Len(t, full.Instances, count)
}
