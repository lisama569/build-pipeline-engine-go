package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

// writeInput creates a repository-relative input file.
func writeInput(t *testing.T, root, rel, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, filepath.Dir(rel)), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, rel), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func runCounting(t *testing.T, cfg Config, rel, content string, count *int32) *Report {
	p := &Pipeline{Name: "p", Tasks: []TaskDef{
		{
			Name: "build", Cacheable: true, Idempotent: true, Inputs: []string{rel},
			Run: func(ctx context.Context) (*TaskOutput, error) {
				atomic.AddInt32(count, 1)
				return &TaskOutput{Payload: []byte("ok")}, nil
			},
		},
	}}
	e := newTestEngine(t, cfg)
	r, err := e.Run(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCache_HitDoesNotReRun(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, dir, "src.txt", "v1")
	cfg := Config{RepoRoot: dir, CacheDir: filepath.Join(dir, "cache"), EnvPassthrough: nil}

	var count int32
	r1 := runCounting(t, cfg, "src.txt", "v1", &count)
	r2 := runCounting(t, cfg, "src.txt", "v1", &count)
	if count != 1 {
		t.Fatalf("task body executed %d times, want exactly 1 (second run must be a cache hit)", count)
	}
	if r1.Tasks[0].Status != StatusSucceeded || r2.Tasks[0].Status != StatusFromCache {
		t.Fatalf("statuses = %s then %s", r1.Tasks[0].Status, r2.Tasks[0].Status)
	}
	if !r2.Tasks[0].CacheHit || r2.Tasks[0].CacheKey == "" {
		t.Fatalf("second run must record cache hit and key: %+v", r2.Tasks[0])
	}
}

func TestCache_InvalidatedByInputChange(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, dir, "src.txt", "v1")
	cfg := Config{RepoRoot: dir, CacheDir: filepath.Join(dir, "cache")}

	var count int32
	runCounting(t, cfg, "src.txt", "v1", &count)
	writeInput(t, dir, "src.txt", "v2")
	r := runCounting(t, cfg, "src.txt", "v2", &count)
	if count != 2 {
		t.Fatalf("changed input must invalidate cache: executions=%d", count)
	}
	if r.Tasks[0].Status != StatusSucceeded {
		t.Fatalf("post-invalidation run status=%s", r.Tasks[0].Status)
	}
}

func TestCache_InvalidatedByEnvChange(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, dir, "src.txt", "x")
	t.Setenv("BUILD_TARGET", "debug")

	var count int32
	cfg1 := Config{RepoRoot: dir, CacheDir: filepath.Join(dir, "cache"), EnvPassthrough: []string{"BUILD_TARGET"}}
	runCounting(t, cfg1, "src.txt", "x", &count)

	t.Setenv("BUILD_TARGET", "release")
	runCounting(t, cfg1, "src.txt", "x", &count)
	if count != 2 {
		t.Fatalf("allow-listed env change must invalidate cache: executions=%d", count)
	}

	// Unlisted environment does NOT participate: same result reused.
	t.Setenv("IRRELEVANT_NOISE", "zzz")
	runCounting(t, cfg1, "src.txt", "x", &count)
	if count != 2 {
		t.Fatalf("non-allow-listed env must not affect the cache key: executions=%d", count)
	}
}

func TestCache_CorruptedEntryIsMissNotWrongResult(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, dir, "src.txt", "x")
	cacheDir := filepath.Join(dir, "cache")
	cfg := Config{CacheDir: cacheDir}

	var count int32
	r := runCounting(t, cfg, "src.txt", "x", &count)
	key := r.Tasks[0].CacheKey

	// Locate and tamper the persisted entry.
	var found string
	_ = filepath.Walk(cacheDir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.HasSuffix(path, ".json") {
			found = path
		}
		return nil
	})
	if found == "" {
		t.Fatal("cache file not written")
	}
	if err := os.WriteFile(found, []byte("{corrupted-tampered-bytes"), 0o644); err != nil {
		t.Fatal(err)
	}

	r2 := runCounting(t, cfg, "src.txt", "x", &count)
	tr := r2.Tasks[0]
	if tr.Status != StatusSucceeded || tr.CacheHit {
		t.Fatalf("corrupted cache must be treated as a miss, got status=%s hit=%t", tr.Status, tr.CacheHit)
	}
	if count != 2 {
		t.Fatalf("corrupted entry must force re-execution: %d", count)
	}
	if len(tr.Uncertainty) == 0 || !strings.Contains(tr.Uncertainty[0], "cache corruption") {
		t.Fatalf("recovery from corruption must be explicitly annotated, got %v", tr.Uncertainty)
	}
	_ = key
}

func TestCache_PayloadChecksumMismatchDetected(t *testing.T) {
	// Valid JSON, tampered payload: checksum must reject it.
	c := newMemCache()
	ctx := context.Background()
	if err := c.Store(ctx, "sha256:abc", []byte("good"), nil); err != nil {
		t.Fatal(err)
	}
	e := c.entries["sha256:abc"]
	e.Payload = []byte("evil")
	_, found, corrupted, err := c.Lookup(ctx, "sha256:abc")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("checksum-mismatched entry must not be a hit")
	}
	if !strings.Contains(corrupted, "checksum") {
		t.Fatalf("corruption reason should mention checksum, got %q", corrupted)
	}
}

func TestCache_RepeatedReportsIdentical(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, dir, "src.txt", "stable")
	cfg := Config{RepoRoot: dir, CacheDir: filepath.Join(dir, "cache")}

	p := &Pipeline{Name: "p", Tasks: []TaskDef{
		{Name: "t", Cacheable: true, Idempotent: true, Inputs: []string{"src.txt"},
			Run: func(ctx context.Context) (*TaskOutput, error) {
				return &TaskOutput{Payload: []byte("v")}, nil
			}},
	}}
	e := newTestEngine(t, cfg)
	r1, err := e.Run(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := e.Run(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := r1.JSON()
	b2, _ := r2.JSON()
	// Ignore the volatile cache_hit/status difference between first and
	// second run by comparing the STATUS-independent skeleton: order, names,
	// reasons, summary keys are stable given same input apart from hit.
	if r1.Order[0] != r2.Order[0] {
		t.Fatalf("schedule order changed: %v vs %v", r1.Order, r2.Order)
	}
	if r1.Pipeline != r2.Pipeline || r1.Reason != r2.Reason {
		t.Fatalf("conclusions differ: %s vs %s", b1, b2)
	}
}

func TestCache_PersistsAcrossEngineInstances(t *testing.T) {
	dir := t.TempDir()
	writeInput(t, dir, "src.txt", "persist")
	cacheDir := filepath.Join(dir, "sub", "cache")
	cfg := Config{RepoRoot: dir, CacheDir: cacheDir}

	var count int32
	r1 := runCounting(t, cfg, "src.txt", "persist", &count)
	// Brand new engine over the same cache directory.
	r2 := runCounting(t, cfg, "src.txt", "persist", &count)
	if count != 1 {
		t.Fatalf("persistent cache should hit across engine instances, runs=%d", count)
	}
	_ = r1
	if r2.Tasks[0].Status != StatusFromCache {
		t.Fatalf("second instance status=%s", r2.Tasks[0].Status)
	}
}
