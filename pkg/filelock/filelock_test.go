package filelock_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pkg/filelock"
)

func TestAcquire_BlocksWhileHeld(t *testing.T) {
	dir := t.TempDir()
	l1, err := filelock.Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan struct{})
	go func() {
		l2, err := filelock.Acquire(filepath.Join(dir)) // same lock target
		if err != nil {
			t.Error(err)
			return
		}
		_ = l2.Release()
		close(got)
	}()
	select {
	case <-got:
		t.Fatal("second acquire returned while first lock still held")
	case <-time.After(50 * time.Millisecond):
	}
	if err := l1.Release(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("second acquire did not proceed after release")
	}
}

func TestRelease_IsIdempotent(t *testing.T) {
	dir := t.TempDir()
	l, err := filelock.Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Release(); err != nil {
		t.Fatal(err)
	}
	if err := l.Release(); err != nil {
		t.Fatalf("second release: %v", err)
	}
}
