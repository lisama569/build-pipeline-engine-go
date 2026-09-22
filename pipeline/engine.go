package pipeline

import (
	"context"
	"os"
	"sync"
)

// Engine orchestrates pipeline runs. A single Engine shares one cache and
// one in-flight de-duplication table across all runs, so identical tasks
// referenced by multiple concurrent pipelines execute at most once and all
// references observe the same result.
type Engine struct {
	cfg   Config
	cache Cache

	// mu guards inFlight and shared when the same fingerprint is requested
	// concurrently from different pipelines.
	mu       sync.Mutex
	inFlight map[string]*flightCall
	shared   map[string]*TaskResult // fingerprints already completed this process
}

// flightCall lets concurrent callers of the same fingerprint share one run.
type flightCall struct {
	done   chan struct{}
	result *TaskResult
}

// NewEngine validates cfg and builds an engine with a persistent (or
// in-memory, when cfg.CacheDir is empty) cache.
func NewEngine(cfg Config) (*Engine, error) {
	vc, err := ValidateConfig(cfg)
	if err != nil {
		return nil, err
	}
	c, err := newCache(vc.CacheDir)
	if err != nil {
		return nil, err
	}
	return &Engine{
		cfg:      vc,
		cache:    c,
		inFlight: map[string]*flightCall{},
		shared:   map[string]*TaskResult{},
	}, nil
}

// Config returns the engine's validated configuration.
func (e *Engine) Config() Config { return e.cfg }

// envForKey returns exactly the allow-listed environment variables. Nothing
// else from the ambient environment can affect a cache key.
func (e *Engine) envForKey() map[string]string {
	out := map[string]string{}
	for _, name := range e.cfg.EnvPassthrough {
		if v, ok := os.LookupEnv(name); ok {
			out[name] = v
		}
	}
	return out
}

// runOrJoin executes the task identified by (g, name, key) exactly once per
// fingerprint, even if several pipelines reference it concurrently: the
// second caller joins the first call and receives the identical result.
func (e *Engine) runOrJoin(ctx context.Context, g *Graph, name, key string, run func(context.Context) *TaskResult) *TaskResult {
	// Only cacheable tasks with a real fingerprint participate in cross-
	// pipeline de-duplication. The empty key must never collide.
	if key == "" {
		return run(ctx)
	}
	e.mu.Lock()
	if r, ok := e.shared[key]; ok {
		e.mu.Unlock()
		clone := *r
		clone.CacheHit = r.CacheHit
		return &clone
	}
	if f, ok := e.inFlight[key]; ok {
		e.mu.Unlock()
		<-f.done
		clone := *f.result
		return &clone
	}
	f := &flightCall{done: make(chan struct{})}
	e.inFlight[key] = f
	e.mu.Unlock()

	res := run(ctx)

	e.mu.Lock()
	delete(e.inFlight, key)
	e.shared[key] = res
	f.result = res
	close(f.done)
	e.mu.Unlock()

	clone := *res
	return &clone
}

// Run parses and executes a single pipeline, returning a deterministic
// structured report. Parse failures are returned as errors; execution
// outcomes (including task failures) are reported in the returned Report.
func (e *Engine) Run(ctx context.Context, p *Pipeline) (*Report, error) {
	g, err := Parse(p, e.cfg)
	if err != nil {
		return nil, err
	}
	return e.Execute(ctx, g), nil
}

// RunMany executes several pipelines concurrently, sharing the cache and
// de-duplicating identical tasks across pipelines. Reports are returned in
// the same order as the inputs; a parse failure rejects that pipeline with
// an error in the matching position and nothing is executed for it.
func (e *Engine) RunMany(ctx context.Context, ps []*Pipeline) ([]*Report, error) {
	reports := make([]*Report, len(ps))
	errs := make([]error, len(ps))
	var wg sync.WaitGroup
	for i, p := range ps {
		wg.Add(1)
		go func(i int, p *Pipeline) {
			defer wg.Done()
			r, err := e.Run(ctx, p)
			reports[i] = r
			errs[i] = err
		}(i, p)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return reports, err
		}
	}
	return reports, nil
}

// Close releases engine resources; no in-flight tasks must remain.
func (e *Engine) Close() error {
	e.mu.Lock()
	n := len(e.inFlight)
	e.mu.Unlock()
	if n != 0 {
		return newErr(ReasonExecError, "close called while tasks are still in flight")
	}
	return nil
}
