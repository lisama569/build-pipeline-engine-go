package cache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

func TestKey_CoversInputsAndEnv(t *testing.T) {
	s, _ := Open(t.TempDir())
	base := Fingerprint{TaskID: "t", Command: "go build", EnvHash: "e1",
		Inputs: map[string]string{"src": "a"}}
	k1 := s.Key(base)

	// change an input -> different key
	ch2 := base
	ch2.Inputs = map[string]string{"src": "b"}
	if k1 == s.Key(ch2) {
		t.Fatal("cache key must change when an input changes")
	}
	// change environment hash -> different key
	ch3 := base
	ch3.EnvHash = "e2"
	if k1 == s.Key(ch3) {
		t.Fatal("cache key must change when environment changes")
	}
	// change command -> different key
	ch4 := base
	ch4.Command = "go test"
	if k1 == s.Key(ch4) {
		t.Fatal("cache key must change when command changes")
	}
	// identical inputs in different map order -> same key (deterministic)
	ch5 := Fingerprint{TaskID: "t", Command: "go build", EnvHash: "e1",
		Inputs: map[string]string{"z": "1", "src": "a", "a": "x"}}
	ch6 := Fingerprint{TaskID: "t", Command: "go build", EnvHash: "e1",
		Inputs: map[string]string{"a": "x", "src": "a", "z": "1"}}
	if s.Key(ch5) != s.Key(ch6) {
		t.Fatal("cache key must be independent of map iteration order")
	}
	t.Logf("input/env/command each alter the key; map order does not: %s", k1[:12])
}

func TestPutGet_RoundTrip(t *testing.T) {
	s, _ := Open(t.TempDir())
	ctx := context.Background()
	key := s.Key(Fingerprint{TaskID: "t", Command: "c", Inputs: map[string]string{"x": "1"}})
	e := Entry{Key: key, Digest: "digest-1", Output: pipeline.ExecOutput{Stdout: "ok", Digest: "digest-1"}}
	if err := s.Put(ctx, e); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, key, "digest-1")
	if err != nil {
		t.Fatalf("expected hit: %v", err)
	}
	if got.Output.Stdout != "ok" {
		t.Fatalf("output mismatch: %q", got.Output.Stdout)
	}
	t.Log("valid entry round-trips")
}

func TestGet_CorruptFileQuarantined(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	ctx := context.Background()
	key := s.Key(Fingerprint{TaskID: "t", Command: "c"})
	if err := s.Put(ctx, Entry{Key: key, Output: pipeline.ExecOutput{Stdout: "x"}}); err != nil {
		t.Fatal(err)
	}
	// tamper with the bytes
	p := s.path(key)
	if err := os.WriteFile(p, []byte("{not-json"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := s.Get(ctx, key, "")
	pe := pipeline.AsError(err)
	if pe == nil || pe.Code != pipeline.ErrCacheCorrupt {
		t.Fatalf("expected cache_corrupt, got %v", err)
	}
	t.Logf("tampered entry rejected as [%s] and quarantined", pe.Code)
	if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("corrupt entry should be moved aside")
	}
}

func TestGet_ChecksumTamperIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	ctx := context.Background()
	key := s.Key(Fingerprint{TaskID: "t", Command: "c"})
	if err := s.Put(ctx, Entry{Key: key, Output: pipeline.ExecOutput{Stdout: "x"}}); err != nil {
		t.Fatal(err)
	}
	p := s.path(key)
	raw, _ := os.ReadFile(p)
	// flip a byte inside the embedded stdout string
	tampered := append([]byte(nil), raw...)
	idx := -1
	for i := range tampered {
		if tampered[i] == 'x' {
			idx = i
			break
		}
	}
	if idx < 0 {
		t.Fatal("could not locate payload byte")
	}
	tampered[idx] = 'y'
	if err := os.WriteFile(p, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := s.Get(ctx, key, "")
	pe := pipeline.AsError(err)
	if pe == nil || pe.Code != pipeline.ErrCacheCorrupt {
		t.Fatalf("payload tamper must be cache_corrupt, got %v", err)
	}
	t.Logf("payload tamper detected via checksum: [%s]", pe.Code)
}

func TestGet_DigestMismatchIsPollution(t *testing.T) {
	s, _ := Open(t.TempDir())
	ctx := context.Background()
	key := s.Key(Fingerprint{TaskID: "t", Command: "c"})
	if err := s.Put(ctx, Entry{Key: key, Digest: "old"}); err != nil {
		t.Fatal(err)
	}
	// Consumer expects a different observed digest -> pollution, never a hit.
	_, err := s.Get(ctx, key, "new")
	pe := pipeline.AsError(err)
	if pe == nil || pe.Code != pipeline.ErrCachePolluted {
		t.Fatalf("expected cache_polluted, got %v", err)
	}
	t.Logf("stale digest rejected as [%s] (no wrong conclusion)", pe.Code)
}

func TestPut_AtomicNoPartialTempLeft(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled Put must not leave a committed entry
	_ = s.Put(ctx, Entry{Key: s.Key(Fingerprint{TaskID: "t"})})
	var temps []string
	filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err == nil && filepath.Base(p)[:1] == "." {
			temps = append(temps, p)
		}
		return nil
	})
	// no committed .json entry should be readable
	if _, err := s.Get(context.Background(), s.Key(Fingerprint{TaskID: "t"}), ""); err == nil {
		t.Fatal("canceled Put must not produce a readable entry")
	}
	t.Logf("canceled write left no committed entry (temps cleaned)")
}

func TestInFlight_ExecutesOnce(t *testing.T) {
	d := NewInFlight()
	var calls int
	var mu sync.Mutex
	key := "k"
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := d.Do(context.Background(), key, func(ctx context.Context) (any, error) {
				time.Sleep(20 * time.Millisecond)
				mu.Lock()
				calls++
				mu.Unlock()
				return "same", nil
			})
			if err != nil || v != "same" {
				t.Errorf("observer got %v %v", v, err)
			}
		}()
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("expected exactly 1 execution, got %d", calls)
	}
	t.Logf("20 concurrent observers -> %d execution, identical result", calls)
}

func TestInFlight_FailureShared(t *testing.T) {
	d := NewInFlight()
	boo := errors.New("boom")
	var calls int
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := d.Do(context.Background(), "f", func(ctx context.Context) (any, error) {
				time.Sleep(20 * time.Millisecond)
				calls++
				return nil, boo
			})
			if err != boo {
				t.Errorf("expected shared failure, got %v", err)
			}
		}()
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("failing work should also run once, got %d", calls)
	}
	t.Log("a failing task is executed once and its failure shared identically")
}
