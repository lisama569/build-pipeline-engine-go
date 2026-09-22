package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// cacheEntry is one on-disk or in-memory cache record.
type cacheEntry struct {
	// Key is the fingerprint the entry was stored under.
	Key string `json:"key"`
	// PayloadHash is sha256 of the canonical payload bytes; any mismatch
	// means the entry is corrupted and must not be trusted.
	PayloadHash string `json:"payload_hash"`
	// Payload is the task-specific output payload produced by the action.
	Payload []byte `json:"payload,omitempty"`
	// Value is the in-memory opaque TaskOutput.Value (not persisted).
	Value any `json:"-"`
}

// Cache stores and retrieves task outputs keyed by content fingerprints.
// Implementations must be safe for concurrent use and must never return a
// tampered or partial entry as a hit: corrupted entries produce a miss
// plus an explicit corruption note, never a wrong result.
type Cache interface {
	// Lookup returns the entry for key. found is false on miss or detected
	// corruption; corrupted carries a human-readable explanation that the
	// runner surfaces as an uncertainty annotation.
	Lookup(ctx context.Context, key string) (entry *cacheEntry, found bool, corrupted string, err error)
	// Store writes an entry atomically: readers observe either the complete
	// new value or the previous one, never a half-written file.
	Store(ctx context.Context, key string, payload []byte, value any) error
}

// hashBytes returns the hex sha256 of data.
func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// hashString hashes a canonical string.
func hashString(s string) string { return hashBytes([]byte(s)) }

// Fingerprint computes the cache key for a task. The key covers:
//   - the task name and its action identity (run-time constant per TaskDef
//     identity; command actions contribute program/args/dir/full env),
//   - the declared dependency closure's fingerprints (an upstream output
//     change invalidates downstream cache),
//   - the contents of declared input files relative to RepoRoot,
//   - exactly the allow-listed environment variables.
//
// Two different inputs or environments can therefore never share a key.
// Missing input files hash as an explicit "<missing>" marker, not as empty
// bytes, which could otherwise be confused with an empty existing file.
func Fingerprint(g *Graph, name string, depKeys map[string]string, env map[string]string, repoRoot string) (string, error) {
	t := g.Task(name)
	if t == nil {
		return "", newErr(ReasonMissingDep, "fingerprint requested for unknown task", name)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "task:%s\n", t.Name)

	// Action identity: command tasks carry a stable descriptor set by spec
	// conversion; function tasks use a constant name-derived identity, so
	// cache identity follows spec content rather than pointer allocation.
	identity := t.actionKey
	if identity == "" {
		identity = "func:" + t.Name
	}
	fmt.Fprintf(&b, "action:%s\n", identity)

	// Inputs in sorted order.
	inputs := append([]string(nil), t.Inputs...)
	sort.Strings(inputs)
	for _, in := range inputs {
		base := in
		if filepath.IsAbs(in) {
			base, _ = filepath.Rel(repoRoot, in)
		}
		full := in
		if !filepath.IsAbs(in) {
			full = filepath.Join(repoRoot, in)
		}
		data, err := os.ReadFile(full)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				fmt.Fprintf(&b, "input:%s=<missing>\n", filepath.ToSlash(base))
				continue
			}
			return "", newErr(ReasonExecError, "cannot hash input: "+err.Error(), name, in)
		}
		fmt.Fprintf(&b, "input:%s=%s\n", filepath.ToSlash(base), hashBytes(data))
	}

	// Allow-listed environment, sorted, with name+value hashed.
	envNames := make([]string, 0, len(env))
	for k := range env {
		envNames = append(envNames, k)
	}
	sort.Strings(envNames)
	for _, k := range envNames {
		fmt.Fprintf(&b, "env:%s=%s\n", k, hashString(env[k]))
	}

	// Dependency fingerprints in declaration order (already sorted).
	for _, d := range g.deps[name] {
		dk, ok := depKeys[d]
		if !ok {
			return "", newErr(ReasonUnknown, "dependency fingerprint unavailable", d)
		}
		fmt.Fprintf(&b, "dep:%s=%s\n", d, dk)
	}

	return "sha256:" + hashString(b.String()), nil
}

// ---------------------------------------------------------------------------
// In-memory cache
// ---------------------------------------------------------------------------

type memCache struct {
	entries map[string]*cacheEntry
}

func newMemCache() *memCache {
	return &memCache{entries: map[string]*cacheEntry{}}
}

func (c *memCache) Lookup(ctx context.Context, key string) (*cacheEntry, bool, string, error) {
	e, ok := c.entries[key]
	if !ok {
		return nil, false, "", nil
	}
	if hashBytes(e.Payload) != e.PayloadHash {
		return nil, false, "in-memory payload checksum mismatch", nil
	}
	return e, true, "", nil
}

func (c *memCache) Store(ctx context.Context, key string, payload []byte, value any) error {
	// Defensive copy: callers must not be able to mutate stored payloads.
	stored := append([]byte(nil), payload...)
	c.entries[key] = &cacheEntry{
		Key:         key,
		PayloadHash: hashBytes(stored),
		Payload:     stored,
		Value:       value,
	}
	return nil
}

// ---------------------------------------------------------------------------
// Filesystem cache (atomic writes)
// ---------------------------------------------------------------------------

type fileCache struct {
	dir string
	mem *memCache // value-side mirror; payloads live on disk
}

func newFileCache(dir string) (*fileCache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, newErr(ReasonExecError, "cannot create cache directory: "+err.Error())
	}
	return &fileCache{dir: dir, mem: newMemCache()}, nil
}

func (c *fileCache) path(key string) string {
	sum := strings.TrimPrefix(key, "sha256:")
	// Two-level sharding keeps a single directory from growing unbounded.
	if len(sum) >= 4 {
		return filepath.Join(c.dir, sum[:2], sum[2:4], sum+".json")
	}
	return filepath.Join(c.dir, sum+".json")
}

func (c *fileCache) Lookup(ctx context.Context, key string) (*cacheEntry, bool, string, error) {
	raw, err := os.ReadFile(c.path(key))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, "", nil
		}
		return nil, false, "", newErr(ReasonExecError, "cache read failed: "+err.Error())
	}
	var e cacheEntry
	if err := json.Unmarshal(raw, &e); err != nil {
		return nil, false, "cache entry is not valid JSON: " + err.Error(), nil
	}
	if e.Key != key {
		return nil, false, "cache entry key does not match stored filename", nil
	}
	if hashBytes(e.Payload) != e.PayloadHash {
		return nil, false, "on-disk payload checksum mismatch (cache corrupted)", nil
	}
	if e2, ok := c.mem.entries[key]; ok {
		e.Value = e2.Value
	}
	return &e, true, "", nil
}

func (c *fileCache) Store(ctx context.Context, key string, payload []byte, value any) error {
	e := cacheEntry{
		Key:         key,
		PayloadHash: hashBytes(payload),
		Payload:     append([]byte(nil), payload...),
	}
	raw, err := json.MarshalIndent(&e, "", "  ")
	if err != nil {
		return newErr(ReasonExecError, "cache encode failed: "+err.Error())
	}

	target := c.path(key)
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return newErr(ReasonExecError, "cache mkdir failed: "+err.Error())
	}
	// Write to a sibling temp file then atomically rename: readers never see
	// a partially written entry.
	tmp, err := os.CreateTemp(filepath.Dir(target), ".tmp-*")
	if err != nil {
		return newErr(ReasonExecError, "cache temp create failed: "+err.Error())
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(raw); err != nil {
		_ = tmp.Close()
		cleanup()
		return newErr(ReasonExecError, "cache write failed: "+err.Error())
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return newErr(ReasonExecError, "cache sync failed: "+err.Error())
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return newErr(ReasonExecError, "cache close failed: "+err.Error())
	}
	if err := os.Rename(tmpName, target); err != nil {
		cleanup()
		return newErr(ReasonExecError, "cache rename failed: "+err.Error())
	}
	c.mem.entries[key] = &cacheEntry{Key: key, PayloadHash: e.PayloadHash, Payload: e.Payload, Value: value}
	return nil
}

// newCache constructs the default cache: a file cache rooted at dir when
// dir is non-empty, otherwise a process-local in-memory cache.
func newCache(dir string) (Cache, error) {
	if dir == "" {
		return newMemCache(), nil
	}
	return newFileCache(dir)
}

// encodePayload canonicalizes v to stable JSON for cache persistence.
func encodePayload(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, newErr(ReasonExecError, "payload encode failed: "+err.Error())
	}
	return raw, nil
}

// decodePayload reverses encodePayload into v.
func decodePayload(raw []byte, v any) error {
	if err := json.Unmarshal(raw, v); err != nil {
		return newErr(ReasonExecError, "payload decode failed: "+err.Error())
	}
	return nil
}
