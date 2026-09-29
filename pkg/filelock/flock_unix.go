//go:build !windows

package filelock

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

type flockLock struct {
	f *os.File
}

// Acquire blocks until an exclusive lock on <dir>/.lock can be held. The
// lock file is created on demand. The returned lock must be Released.
func Acquire(dir string) (Lock, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("filelock: create %q: %w", dir, err)
	}
	path := filepath.Join(dir, ".lock")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("filelock: open %q: %w", path, err)
	}
	// LOCK_EX blocks until the lock is free; LOCK_ND is deliberately not used
	// because concurrent runs are expected to wait, not fail.
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, fmt.Errorf("filelock: flock %q: %w", path, err)
	}
	return &flockLock{f: f}, nil
}

func (l *flockLock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	// Best-effort unlock; closing the fd also releases the kernel lock.
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	err := l.f.Close()
	l.f = nil
	return err
}
