package resume

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

// Store persists resume checkpoints under a state directory.
//
// Layout (shared with the history layer, which uses a sibling subdirectory):
//
//	<root>/checkpoints/<escaped-pipeline-name>.json
//
// Each file is a tamper-evident envelope over the compact checkpoint JSON,
// exactly like the cache layer: sha256 payload, temp-file + fsync + rename.
// A corrupt checkpoint is reported as ErrStateCorrupt and never trusted; the
// caller treats it like "no previous run" (a full, clean re-execution).
type Store struct {
	dir string
}

type envelope struct {
	Pipeline      string          `json:"pipeline"`
	Payload       string          `json:"payload"` // hex(sha256(compact Checkpoint JSON))
	CheckpointRaw json.RawMessage `json:"checkpoint"`
}

// OpenStore initializes (if needed) the checkpoint directory rooted at
// stateDir. An empty stateDir yields a disabled store.
func OpenStore(stateDir string) (*Store, error) {
	if stateDir == "" {
		return &Store{}, nil
	}
	dir := filepath.Join(stateDir, "checkpoints")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, pipeline.NewError(pipeline.ErrConfigInvalid,
			"cannot create state dir %q: %v", stateDir, err)
	}
	return &Store{dir: dir}, nil
}

// Enabled reports whether checkpoints are persisted.
func (s *Store) Enabled() bool { return s != nil && s.dir != "" }

// Dir exposes the checkpoint directory (tests/history use the parent).
func (s *Store) Dir() string { return s.dir }

// pipelineFile is the per-pipeline checkpoint file. Pipeline names are
// user-supplied, so they are flattened to a safe file name; a sha256 suffix
// keeps distinct but colliding names apart.
func (s *Store) pipelineFile(name string) string {
	return filepath.Join(s.dir, EscapeName(name)+".json")
}

// Load reads and validates the checkpoint for pipeline name. A missing file
// returns (nil, nil): a clean first run. A corrupt file returns
// ErrStateCorrupt and is quarantined so it cannot mask later good state.
func (s *Store) Load(pipelineName string) (*Checkpoint, error) {
	if !s.Enabled() {
		return nil, nil
	}
	data, err := os.ReadFile(s.pipelineFile(pipelineName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		s.quarantine(pipelineName)
		return nil, pipeline.NewError(pipeline.ErrStateCorrupt,
			"checkpoint for %q: cannot decode: %v", pipelineName, err)
	}
	if env.Pipeline != pipelineName {
		return nil, pipeline.NewError(pipeline.ErrStateCorrupt,
			"checkpoint for %q is bound to pipeline %q", pipelineName, env.Pipeline)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, env.CheckpointRaw); err != nil {
		s.quarantine(pipelineName)
		return nil, pipeline.NewError(pipeline.ErrStateCorrupt,
			"checkpoint for %q: invalid embedded JSON: %v", pipelineName, err)
	}
	checksum := sha256.Sum256(compact.Bytes())
	if hex.EncodeToString(checksum[:]) != env.Payload {
		s.quarantine(pipelineName)
		return nil, pipeline.NewError(pipeline.ErrStateCorrupt,
			"checkpoint for %q: checksum mismatch", pipelineName)
	}
	var cp Checkpoint
	if err := json.Unmarshal(env.CheckpointRaw, &cp); err != nil {
		return nil, pipeline.NewError(pipeline.ErrStateCorrupt,
			"checkpoint for %q: invalid payload: %v", pipelineName, err)
	}
	if cp.Pipeline != pipelineName {
		return nil, pipeline.NewError(pipeline.ErrStateCorrupt,
			"checkpoint for %q embeds pipeline %q", pipelineName, cp.Pipeline)
	}
	return &cp, nil
}

// Save atomically replaces the checkpoint for cp.Pipeline.
func (s *Store) Save(cp *Checkpoint) error {
	if !s.Enabled() {
		return nil
	}
	if cp == nil || cp.Pipeline == "" {
		return pipeline.NewError(pipeline.ErrConfigInvalid, "refusing to save empty checkpoint")
	}
	raw, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("checkpoint encode: %w", err)
	}
	checksum := sha256.Sum256(raw)
	env, err := json.MarshalIndent(&envelope{
		Pipeline:      cp.Pipeline,
		Payload:       hex.EncodeToString(checksum[:]),
		CheckpointRaw: raw,
	}, "", "  ")
	if err != nil {
		return err
	}
	final := s.pipelineFile(cp.Pipeline)
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

func (s *Store) quarantine(pipelineName string) {
	from := s.pipelineFile(pipelineName)
	_ = os.Rename(from, from+".bad")
}

// escapeName flattens an arbitrary pipeline name into a safe file name. It
// keeps readable characters and appends a short sha256 so names that flatten
// to the same string never overwrite each other.
func EscapeName(name string) string {
	h := sha256.Sum256([]byte(name))
	sum := hex.EncodeToString(h[:8])
	var b bytes.Buffer
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String() + "-" + sum
}
