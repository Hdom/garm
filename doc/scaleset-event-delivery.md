# Scale-set event delivery

Scale-set workers and controllers retain at most 256 pending events in their
worker queue. Their watcher subscriptions also use a bounded queue of 256,
alongside the watcher's fixed input/output channel buffers of 128 each.

When a subscription overflows, queued notifications are coalesced into a
resync marker. The consumer reloads authoritative database state, including
terminal instance rows, instead of depending on every intermediate change.
Snapshot failures are retried with cancellation support. A timestamp boundary
rejects notifications older than the refreshed snapshot, and creates that
finish while the snapshot loads remain counted in the worker cache.

This mode is opt-in for state consumers through
`watcher.RegisterReconciledConsumer`. Existing consumers that require each
individual notification keep their current delivery mode. It avoids moving
the scale-set backlog into an unbounded watcher queue or blocking database
writes behind a slow worker. Saturation is logged with the consumer ID.

Scale-set metadata updates and listener checkpoints do not load or notify
with the `Instances` association. Update callbacks receive metadata only;
full API reads and the instance-list endpoint remain available when a caller
needs instance details. Snapshot recovery may perform a full read once, but
discards its instance association after rebuilding worker state.

Listener teardown and snapshot database reads run without the worker state
mutex. Other existing autoscale/consolidation operations may still hold that
mutex across remote calls; their latency no longer permits unbounded event
retention in the scale-set delivery path.
