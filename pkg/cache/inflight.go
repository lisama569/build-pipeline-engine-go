package cache

import (
	"context"
	"sync"
)

// result is the shared conclusion of one in-flight key.
type result struct {
	val any
	err error
}

// InFlight deduplicates identical concurrent work across pipelines: only the
// first caller for a key executes fn; concurrent and later callers await the
// same conclusion and receive identical values. Failures are shared too, so a
// task cannot succeed for one pipeline and fail for another.
type InFlight struct {
	mu sync.Mutex
	m  map[string]*call
}

type call struct {
	done   chan struct{}
	result result
}

// NewInFlight creates a deduper.
func NewInFlight() *InFlight { return &InFlight{m: make(map[string]*call)} }

// Do runs fn exactly once per key currently in flight. The leader executes
// fn with its own ctx so run cancellation propagates to the real work;
// concurrent callers block until the shared conclusion is reached and then
// receive the identical value/error. A waiter whose ctx ends while waiting
// gets ctx.Err() but never triggers a duplicate execution.
func (d *InFlight) Do(ctx context.Context, key string, fn func(leaderCtx context.Context) (any, error)) (any, error) {
	d.mu.Lock()
	if c, ok := d.m[key]; ok {
		d.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.done:
			return c.result.val, c.result.err
		}
	}
	c := &call{done: make(chan struct{})}
	d.m[key] = c
	d.mu.Unlock()

	c.result.val, c.result.err = fn(ctx)
	close(c.done)

	d.mu.Lock()
	delete(d.m, key)
	d.mu.Unlock()

	return c.result.val, c.result.err
}
