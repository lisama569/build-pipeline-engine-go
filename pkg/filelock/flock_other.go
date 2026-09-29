//go:build windows

package filelock

import "fmt"

// On platforms without flock the engine is still correct within a process
// (Go-level state stores are used in tests); cross-process exclusion of the
// lock file itself is unsupported outside POSIX.
type noopLock struct{}

func (noopLock) Release() error { return nil }

func Acquire(dir string) (Lock, error) {
	return nil, fmt.Errorf("filelock: cross-process locking is only supported on POSIX")
}
