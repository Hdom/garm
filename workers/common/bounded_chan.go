package common

import "context"

// BoundedChan retains at most capacity queued events. Senders select on their
// context/quit channel when using In, so saturation applies cancellable pressure.
type BoundedChan[T any] struct {
	events chan T
	ctx    context.Context
	quit   <-chan struct{}
}

func NewBoundedChan[T any](ctx context.Context, quit <-chan struct{}, capacity int) *BoundedChan[T] {
	if capacity <= 0 {
		panic("event queue capacity must be positive")
	}
	return &BoundedChan[T]{events: make(chan T, capacity), ctx: ctx, quit: quit}
}
func (q *BoundedChan[T]) In() chan<- T  { return q.events }
func (q *BoundedChan[T]) Out() <-chan T { return q.events }
func (q *BoundedChan[T]) Process(handler func(T)) {
	for {
		select {
		case <-q.ctx.Done():
			return
		case <-q.quit:
			return
		case event := <-q.events:
			handler(event)
		}
	}
}
