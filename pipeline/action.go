package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
)

// CommandAction is the repository-native build/verification entry point:
// an external process (e.g. "go", args {"build", "./..."}) executed with a
// fixed working directory and a fixed, explicit environment. It serves as
// the default TaskDef.Run produced by the spec loader.
type CommandAction struct {
	Program string
	Args    []string
	// Dir is repository-relative; empty means RepoRoot.
	Dir string
	// Env is the FULL explicit environment the process gets. The process
	// never inherits ambient variables beyond what is listed here; the
	// engine injects PATH/HOME only when the spec omits them, and the
	// effective environment is always folded into the cache key.
	Env map[string]string
}

// commandResult is the persisted payload for command tasks.
type commandResult struct {
	Program   string            `json:"program"`
	Args      []string          `json:"args"`
	Dir       string            `json:"dir"`
	Env       map[string]string `json:"env"`
	Stdout    string            `json:"stdout"`
	Stderr    string            `json:"stderr"`
	Artifacts map[string]string `json:"artifacts,omitempty"` // rel path -> sha256
}

// stableDescriptor renders the action's identity canonically for hashing.
func (a CommandAction) stableDescriptor() string {
	keys := make([]string, 0, len(a.Env))
	for k := range a.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	fmt.Fprintf(&b, "cmd:%s", a.Program)
	for _, arg := range a.Args {
		fmt.Fprintf(&b, "|%s", arg)
	}
	fmt.Fprintf(&b, "|dir=%s", filepath.ToSlash(a.Dir))
	for _, k := range keys {
		fmt.Fprintf(&b, "|env:%s=%s", k, a.Env[k])
	}
	return b.String()
}

// NewCommandRunner turns a CommandAction into a TaskDef.Run. The process is
// terminated (SIGTERM, then SIGKILL after a bounded WaitDelay) when ctx is
// canceled or the task timeout fires, so no orphan process survives.
func (e *Engine) newCommandRunner(a CommandAction, artifacts []string) func(ctx context.Context) (*TaskOutput, error) {
	return func(ctx context.Context) (*TaskOutput, error) {
		dir := a.Dir
		if dir == "" {
			dir = e.cfg.RepoRoot
		} else if !filepath.IsAbs(dir) {
			dir = filepath.Join(e.cfg.RepoRoot, dir)
		}

		cmd := exec.CommandContext(ctx, a.Program, a.Args...)
		cmd.Dir = dir
		// Explicit, fixed environment — never the ambient one.
		env := make([]string, 0, len(a.Env))
		keys := make([]string, 0, len(a.Env))
		for k := range a.Env {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			env = append(env, k+"="+a.Env[k])
		}
		cmd.Env = env

		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		// Bounded cancellation: SIGTERM on ctx, SIGKILL after the wait
		// delay. Both bounds guarantee the process group converges.
		cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
		cmd.WaitDelay = 2 * killGrace

		if err := cmd.Start(); err != nil {
			return nil, newErr(ReasonExecError, "cannot start command: "+err.Error())
		}
		waitErr := cmd.Wait()

		if ctx.Err() != nil {
			// ctx cancellation/timeout already classifies the attempt; do
			// not masquerade it as an ordinary command failure.
			return nil, ctx.Err()
		}
		if waitErr != nil {
			msg := strings.TrimSpace(stderr.String())
			if msg == "" {
				msg = waitErr.Error()
			}
			return nil, newErr(ReasonFailed, fmt.Sprintf("command %q failed: %s", a.Program, msg))
		}

		res := commandResult{
			Program: a.Program,
			Args:    append([]string(nil), a.Args...),
			Dir:     a.Dir,
			Env:     a.Env,
			Stdout:  stdout.String(),
			Stderr:  stderr.String(),
		}
		if len(artifacts) > 0 {
			res.Artifacts = map[string]string{}
			for _, art := range artifacts {
				full := art
				if !filepath.IsAbs(full) {
					full = filepath.Join(e.cfg.RepoRoot, art)
				}
				data, err := os.ReadFile(full)
				if err != nil {
					return nil, newErr(ReasonExecError, "cannot hash artifact: "+err.Error(), art)
				}
				sum := sha256.Sum256(data)
				res.Artifacts[filepath.ToSlash(art)] = hex.EncodeToString(sum[:])
			}
		}

		payload, err := encodePayload(&res)
		if err != nil {
			return nil, err
		}
		return &TaskOutput{Value: &res, Payload: payload, Artifacts: artifacts}, nil
	}
}
