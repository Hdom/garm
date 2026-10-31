package pool

import "fmt"

// InstanceOperationLimiter is shared by an installation's entity managers.
// Admission happens before launching an operation goroutine, so both provider
// calls and waiting goroutines are bounded. Saturated work remains pending.
type InstanceOperationLimiter struct{ slots chan struct{} }

func NewInstanceOperationLimiter(limit int) (*InstanceOperationLimiter, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("instance operation limit must be positive")
	}
	return &InstanceOperationLimiter{slots: make(chan struct{}, limit)}, nil
}

type Option func(*basePoolManager) error

func WithInstanceOperationLimiter(limiter *InstanceOperationLimiter) Option {
	return func(r *basePoolManager) error {
		if limiter == nil {
			return fmt.Errorf("instance operation limiter must not be nil")
		}
		r.instanceOperations = limiter
		return nil
	}
}

func (r *basePoolManager) acquireInstanceOperation() bool {
	// Also support internally constructed managers in tests. Public construction
	// installs the shared controller limiter before any worker goroutine starts.
	r.instanceOperationsOnce.Do(func() {
		if r.instanceOperations == nil {
			r.instanceOperations, _ = NewInstanceOperationLimiter(10)
		}
	})
	select {
	case <-r.ctx.Done():
		return false
	case <-r.quit:
		return false
	default:
	}
	select {
	case r.instanceOperations.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (r *basePoolManager) releaseInstanceOperation() { <-r.instanceOperations.slots }
