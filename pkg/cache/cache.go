// Package cache implements a content-addressed, tamper-evident local cache
// for task results.
//
// Cache key coverage:
//
//	key = sha256( taskID || canonical(command) || canonical(env overlay)
//	             || sorted(input key/values) )
//
// so any change in inputs, command line or environment produces a different
// key (no stale hit across environments).
//
// Integrity / pollution safety:
//   - Each entry is written to a temp file then fsynced + renamed: a killed
//     run leaves at most an ignored temp file, never a half-written entry.
//   - Each entry stores sha256(payload). Get recomputes it and also verifies
//     the requested key equals the stored key. Mismatch =>
//     ErrCacheCorrupt, the entry is rejected and the caller re-executes.
//   - The observed digest returned by the task is compared with the cached
//     digest on use; a mismatch is ErrCachePolluted.
package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

// Fingerprint is the full set of inputs folded into a cache key.
type Fingerprint struct {
	TaskID  string
	Command string
	EnvHash string
	Inputs  map[string]string
}

// Entry is one cached task result with integrity metadata.
type Entry struct {
	Key    string              `json:"key"`
	Digest string              `json:"digest"`
	Output pipeline.ExecOutput `json:"output"`
}

type envelope struct {
	Key       string          `json:"key"`
	Payload   string          `json:"payload"` // hex(sha256(canonical(Entry)))
	EntryJSON json.RawMessage `json:"entry"`
}

// Store is the on-disk cache.
type Store struct {
	dir string
}

// Open opens (and if necessary initializes) a cache rooted at dir.
// An empty dir yields a disabled no-op store.
func Open(dir string) (*Store, error) {
	if dir == "" {
		return &Store{}, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, pipeline.NewError(pipeline.ErrConfigInvalid,
			"cannot create cache dir %q: %v", dir, err)
	}
	return &Store{dir: dir}, nil
}

// Enabled reports whether the store persists anything.
func (s *Store) Enabled() bool { return s != nil && s.dir != "" }

// Key computes the deterministic cache key for fp.
func (s *Store) Key(fp Fingerprint) string {
	h := sha256.New()
	write := func(prefix, val string) {
		h.Write([]byte(prefix))
		h.Write([]byte{0})
		h.Write([]byte(val))
		h.Write([]byte{0})
	}
	write("task", fp.TaskID)
	write("cmd", fp.Command)
	write("env", fp.EnvHash)
	keys := make([]string, 0, len(fp.Inputs))
	for k := range fp.Inputs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		write("in:"+k, fp.Inputs[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// FingerprintFromTask builds the fingerprint for a task. The env hash covers
// the task overlay exactly; the caller may include a base-env fingerprint.
func FingerprintFromTask(t *pipeline.Task, baseEnvHash string) Fingerprint {
	fp := Fingerprint{
		TaskID:  t.ID,
		Command: canonicalCommand(t.Cmd),
		Inputs:  t.CacheKeyInputs,
	}
	fp.EnvHash = hashEnv(t.Cmd, baseEnvHash)
	return fp
}

func canonicalCommand(c *pipeline.CommandSpec) string {
	if c == nil {
		return "func"
	}
	return c.Name + "|" + strings.Join(c.Args, " ") + "|" + filepath.ToSlash(c.WorkDir)
}

func hashEnv(c *pipeline.CommandSpec, base string) string {
	h := sha256.New()
	h.Write([]byte(base))
	if c != nil {
		keys := make([]string, 0, len(c.Env))
		for k := range c.Env {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			h.Write([]byte(k + "=" + c.Env[k] + "\n"))
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Store) path(key string) string {
	// shard by first 2 hex chars to keep directories bounded
	return filepath.Join(s.dir, key[:2], key+".json")
}

// Get validates and returns the cached entry. Corrupt or polluted entries
// are NEVER returned; the error code distinguishes the two causes.
func (s *Store) Get(ctx context.Context, key, wantDigest string) (*Entry, error) {
	if !s.Enabled() {
		return nil, os.ErrNotExist
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	data, err := os.ReadFile(s.path(key))
	if err != nil {
		return nil, err // os.ErrNotExist propagates as a clean miss
	}
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		s.quarantine(key)
		return nil, pipeline.NewError(pipeline.ErrCacheCorrupt,
			"cache entry %s: cannot decode: %v", short(key), err)
	}
	if env.Key != key {
		return nil, pipeline.NewError(pipeline.ErrCachePolluted,
			"cache entry %s stored under mismatched key %s", short(key), short(env.Key))
	}
	// Recompute over a canonical (compact, whitespace-free) encoding: the
	// envelope is stored pretty-printed, but embedded whitespace must not
	// affect integrity.
	var compact bytes.Buffer
	if err := json.Compact(&compact, env.EntryJSON); err != nil {
		s.quarantine(key)
		return nil, pipeline.NewError(pipeline.ErrCacheCorrupt,
			"cache entry %s: invalid embedded JSON: %v", short(key), err)
	}
	checksum := sha256.Sum256(compact.Bytes())
	if hex.EncodeToString(checksum[:]) != env.Payload {
		s.quarantine(key)
		return nil, pipeline.NewError(pipeline.ErrCacheCorrupt,
			"cache entry %s: checksum mismatch", short(key))
	}
	var e Entry
	if err := json.Unmarshal(env.EntryJSON, &e); err != nil {
		return nil, pipeline.NewError(pipeline.ErrCacheCorrupt,
			"cache entry %s: invalid payload: %v", short(key), err)
	}
	if e.Key != key {
		return nil, pipeline.NewError(pipeline.ErrCachePolluted,
			"cache entry %s: embedded key mismatch", short(key))
	}
	if wantDigest != "" && e.Digest != "" && e.Digest != wantDigest {
		// The producer-observed digest no longer matches what the consumer
		// expects: treat as pollution, never as a valid hit.
		return nil, pipeline.NewError(pipeline.ErrCachePolluted,
			"cache entry %s: digest mismatch %q != %q", short(key), e.Digest, wantDigest)
	}
	return &e, nil
}

// Put writes an entry atomically: temp file in same dir, fsync, rename.
// Partial writes from a killed process are never visible under the key.
func (s *Store) Put(ctx context.Context, e Entry) error {
	if !s.Enabled() {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	raw, err := json.Marshal(&e)
	if err != nil {
		return fmt.Errorf("cache encode: %w", err)
	}
	// raw is the exact byte sequence that will round-trip through the
	// EntryJSON field (compact JSON), so checksum it directly.
	checksum := sha256.Sum256(raw)
	env, err := json.MarshalIndent(&envelope{
		Key:       e.Key,
		Payload:   hex.EncodeToString(checksum[:]),
		EntryJSON: raw,
	}, "", "  ")
	if err != nil {
		return err
	}
	final := s.path(e.Key)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(final), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(env); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, final); err != nil {
		cleanup()
		return err
	}
	return nil
}

// quarantine moves an unreadable entry aside so a corrupt file does not
// permanently mask fresh results.
func (s *Store) quarantine(key string) {
	from := s.path(key)
	to := from + ".bad"
	_ = os.Rename(from, to)
}

func short(key string) string {
	if len(key) <= 12 {
		return key
	}
	return key[:12]
}
