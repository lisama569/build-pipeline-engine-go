// Package filelock provides an exclusive, advisory on-disk lock used to
// serialize read-modify-write access to per-pipeline state and history, so
// concurrent runs of the same pipeline (including runs in separate OS
// processes) never overwrite or interleave each other's files.
//
// The lock is a POSIX flock on a dedicated lock file inside the pipeline's
// directory: blocking until available, re-entrant-free, and released
// automatically when the process dies (kernel-owned), so a killed run cannot
// leave a permanently stuck lock.
package filelock

// Lock is an acquired exclusive lock; Release frees it.
type Lock interface {
	Release() error
}
