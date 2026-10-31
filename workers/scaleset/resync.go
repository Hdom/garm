package scaleset

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	re "github.com/cloudbase/garm-provider-common/errors"
	cp "github.com/cloudbase/garm-provider-common/params"
	"github.com/cloudbase/garm/params"
)

// A resync marker replaces deltas that overflowed the bounded notification
// queue. Keep retrying a failed snapshot so an isolated terminal event cannot
// be lost merely because the database was unavailable during the first try.
func (w *Worker) reconcileSnapshot() {
	for {
		if err := w.reloadSnapshot(); err == nil {
			return
		} else if errors.Is(err, re.ErrNotFound) {
			_ = w.Stop()
			return
		} else {
			slog.WarnContext(w.ctx, "retrying scale set state snapshot", "error", err)
		}
		if w.sleepWithCancel(time.Second) {
			return
		}
	}
}

func (w *Worker) reloadSnapshot() error {
	// GORM timestamps have millisecond precision. A finer boundary could
	// incorrectly discard a genuine update committed in the same millisecond.
	started := time.Now().Truncate(time.Millisecond)
	w.mux.Lock()
	id := w.scaleSet.ID
	before := make(map[string]struct{}, len(w.runners))
	for id := range w.runners {
		before[id] = struct{}{}
	}
	w.mux.Unlock()
	set, err := w.store.GetScaleSetByID(w.ctx, id)
	if err != nil {
		return err
	}
	runners := make(map[string]params.Instance, len(set.Instances))
	var deleted []params.Instance
	for _, instance := range set.Instances {
		if instance.ProviderName == "" {
			instance.ProviderName = set.ProviderName
		}
		runners[instance.ID] = instance
		if instance.Status == cp.InstanceDeleted {
			deleted = append(deleted, instance)
		}
	}
	set.Instances = nil
	w.mux.Lock()
	// Autoscaling can finish a create while the database snapshot is loading.
	// Retain cache entries created during the read until their queued events
	// arrive, rather than temporarily undercounting provider resources.
	for id, current := range w.runners {
		if _, existed := before[id]; !existed {
			if snapshot, exists := runners[id]; !exists || current.UpdatedAt.After(snapshot.UpdatedAt) {
				runners[id] = current
			}
		}
	}
	stopListener := set.MaxRunners < w.scaleSet.MaxRunners || !set.Enabled
	listener := w.listener
	w.scaleSet = set
	w.runners = runners
	w.snapshotStarted = started
	w.mux.Unlock()
	if stopListener && listener != nil {
		_ = listener.Stop()
	}
	for _, instance := range deleted {
		if err := w.store.DeleteInstanceByName(w.ctx, instance.Name); err != nil && !errors.Is(err, re.ErrNotFound) {
			return fmt.Errorf("cleaning terminal instance %s: %w", instance.Name, err)
		}
		w.mux.Lock()
		delete(w.runners, instance.ID)
		w.mux.Unlock()
	}
	return nil
}

func (c *Controller) reconcileSnapshot() {
	for {
		if err := c.reloadSnapshot(); err == nil {
			return
		} else {
			slog.WarnContext(c.ctx, "retrying scale set controller snapshot", "error", err)
		}
		select {
		case <-c.quit:
			return
		case <-c.ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (c *Controller) reloadSnapshot() error {
	started := time.Now().Truncate(time.Millisecond)
	c.mux.Lock()
	entity := c.Entity
	running := c.running
	c.mux.Unlock()
	if !running {
		return nil
	}
	currentEntity, err := c.store.GetForgeEntity(c.ctx, entity.EntityType, entity.ID)
	if err != nil {
		return err
	}
	sets, err := c.store.ListEntityScaleSets(c.ctx, currentEntity)
	if err != nil {
		return err
	}
	current := make(map[uint]params.ScaleSet, len(sets))
	for _, set := range sets {
		// The list deliberately omits extra specs. Fetch the full configuration
		// for recovery, then discard instances instead of retaining API snapshots.
		full, err := c.store.GetScaleSetByID(c.ctx, set.ID)
		if err != nil {
			return err
		}
		full.Instances = nil
		current[set.ID] = full
	}
	c.mux.Lock()
	defer c.mux.Unlock()
	if !c.running {
		return nil
	}
	c.Entity = currentEntity
	for _, set := range current {
		if err := c.handleScaleSetUpdateOperation(set); err != nil {
			return err
		}
	}
	var stopErr error
	c.ScaleSets.Range(func(key, value any) bool {
		if _, exists := current[key.(uint)]; exists {
			return true
		}
		entry := value.(*scaleSet)
		if err := entry.Stop(); err != nil {
			stopErr = err
			return false
		}
		c.ScaleSets.Delete(key)
		return true
	})
	if stopErr != nil {
		return stopErr
	}
	c.snapshotStarted = started
	return nil
}
