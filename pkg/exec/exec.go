// Package exec runs a single task attempt: either an in-process ExecFunc or
// a local command (the repository's own build/verification entry points).
//
// Guarantees:
//   - Every attempt is bound to ctx: external cancellation or the attempt
//     deadline cancels/kills the work. Local commands run in their own
//     process group which is killed as a whole, so no child process leaks.
//   - Stdout/stderr are captured as bounded tail buffers, so a chatty task
//     cannot cause unbounded memory growth.
//   - Returned errors are classified into pipeline.ErrTaskTimeout,
//     pipeline.ErrTaskCanceled or pipeline.ErrTaskFailed.
package exec

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

// maxTail bounds captured output per stream (64 KiB).
const maxTail = 64 << 10

// Runner executes task attempts.
type Runner struct {
	rootDir string
	baseEnv []string
}

// NewRunner creates a Runner rooted at rootDir. The process environment at
// construction time forms the base environment; tasks may only overlay it.
func NewRunner(rootDir string) *Runner {
	if rootDir == "" {
		rootDir, _ = os.Getwd()
	}
	return &Runner{rootDir: rootDir, baseEnv: os.Environ()}
}

// Run performs one attempt. It never interprets deadlines itself: the caller
// supplies an already-deadlined ctx and this runner only reports why it ended.
func (r *Runner) Run(ctx context.Context, task *pipeline.Task, in pipeline.ExecInput) (pipeline.ExecOutput, error) {
	if task.Run != nil {
		return r.runFunc(ctx, task, in)
	}
	if task.Cmd != nil {
		return r.runCommand(ctx, task, in)
	}
	return pipeline.ExecOutput{}, pipeline.NewError(pipeline.ErrConfigInvalid,
		"task %q has neither Run nor Cmd", task.ID)
}

func (r *Runner) runFunc(ctx context.Context, task *pipeline.Task, in pipeline.ExecInput) (out pipeline.ExecOutput, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = pipeline.NewError(pipeline.ErrTaskFailed,
				"task %q panicked: %v", task.ID, rec)
			out = pipeline.ExecOutput{}
		}
	}()
	out, runErr := task.Run(ctx, in)
	if runErr == nil {
		return out, nil
	}
	// Classify by context, but only when the context actually ended; a task
	// may return context.DeadlineExceeded coincidentally without a deadline.
	return out, r.classify(ctx, runErr, task.ID)
}

func (r *Runner) runCommand(ctx context.Context, task *pipeline.Task, in pipeline.ExecInput) (pipeline.ExecOutput, error) {
	spec := task.Cmd
	if len(spec.Args) == 0 {
		return pipeline.ExecOutput{}, pipeline.NewError(pipeline.ErrConfigInvalid,
			"task %q command has no arguments", task.ID)
	}
	cmd := exec.CommandContext(ctx, spec.Args[0], spec.Args[1:]...)

	// Own process group; canceled runs kill the whole group, not just the
	// shell that spawned children.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process.Pid > 0 {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return os.ErrProcessDone
	}
	cmd.WaitDelay = 2 * time.Second

	workDir := spec.WorkDir
	if workDir == "" {
		workDir = r.rootDir
	}
	if !filepath.IsAbs(workDir) {
		workDir = filepath.Join(r.rootDir, workDir)
	}
	cmd.Dir = workDir
	cmd.Env = mergeEnv(r.baseEnv, spec.Env)

	outBuf, errBuf := newTailBuffer(maxTail), newTailBuffer(maxTail)
	cmd.Stdout = outBuf
	cmd.Stderr = errBuf

	runErr := cmd.Run()
	out := pipeline.ExecOutput{Stdout: outBuf.String(), Stderr: errBuf.String()}
	if runErr == nil {
		return out, nil
	}
	if errors.Is(runErr, os.ErrProcessDone) || ctx.Err() != nil {
		return out, r.classify(ctx, runErr, task.ID)
	}
	return out, pipeline.NewError(pipeline.ErrTaskFailed,
		"task %q command %q failed: %v", task.ID, strings.Join(spec.Args, " "), runErr)
}

// classify maps a context/error pair onto the distinguishable engine codes.
// Deadline and cancellation are reported separately so timeouts are never
// fed to the retry loop as ordinary failures.
func (r *Runner) classify(ctx context.Context, err error, id string) error {
	if ctx.Err() == context.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded) {
		return pipeline.WrapError(pipeline.ErrTaskTimeout, err, "task %q exceeded its deadline", id)
	}
	if ctx.Err() == context.Canceled || errors.Is(err, context.Canceled) {
		return pipeline.WrapError(pipeline.ErrTaskCanceled, err, "task %q was canceled", id)
	}
	return pipeline.WrapError(pipeline.ErrTaskFailed, err, "task %q failed", id)
}

// mergeEnv overlays overlay onto base; later keys win.
func mergeEnv(base []string, overlay map[string]string) []string {
	if len(overlay) == 0 {
		return base
	}
	seen := make(map[string]bool, len(overlay))
	out := make([]string, 0, len(base)+len(overlay))
	for _, kv := range base {
		k := strings.SplitN(kv, "=", 2)[0]
		if v, ok := overlay[k]; ok {
			out = append(out, k+"="+v)
			seen[k] = true
		} else {
			out = append(out, kv)
		}
	}
	keys := make([]string, 0, len(overlay))
	for k := range overlay {
		if !seen[k] {
			keys = append(keys, k)
		}
	}
	// deterministic extra env order
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	for _, k := range keys {
		out = append(out, k+"="+overlay[k])
	}
	return out
}

// tailBuffer keeps only the last limit bytes, bounded memory.
type tailBuffer struct {
	buf   *bytes.Buffer
	limit int
}

func newTailBuffer(limit int) *tailBuffer {
	return &tailBuffer{buf: new(bytes.Buffer), limit: limit}
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.buf.Write(p)
	if t.buf.Len() > t.limit {
		// discard oldest bytes
		excess := t.buf.Len() - t.limit
		full := t.buf.Bytes()
		t.buf = bytes.NewBuffer(append([]byte(nil), full[excess:]...))
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	return string(t.buf.Bytes())
}

var _ io.Writer = (*tailBuffer)(nil)
var _ = fmt.Sprintf
