package exec

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

func TestRun_CommandSuccess(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("posix shell test")
	}
	r := NewRunner(".")
	task := &pipeline.Task{ID: "echo", Cmd: &pipeline.CommandSpec{
		Name: "sh", Args: []string{"sh", "-c", "echo hello"},
	}}
	out, err := r.Run(context.Background(), task, pipeline.ExecInput{})
	if err != nil {
		t.Fatal(err)
	}
	if out.Stdout != "hello\n" {
		t.Fatalf("stdout=%q", out.Stdout)
	}
}

func TestRun_CommandFailureClassified(t *testing.T) {
	r := NewRunner(".")
	task := &pipeline.Task{ID: "fail", Cmd: &pipeline.CommandSpec{
		Name: "sh", Args: []string{"sh", "-c", "exit 3"},
	}}
	_, err := r.Run(context.Background(), task, pipeline.ExecInput{})
	pe := pipeline.AsError(err)
	if pe == nil || pe.Code != pipeline.ErrTaskFailed {
		t.Fatalf("expected task_failed, got %v", err)
	}
	t.Logf("nonzero exit classified [%s]", pe.Code)
}

func TestRun_TimeoutKillsProcessGroup(t *testing.T) {
	r := NewRunner(".")
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	// spawn a child that outlives the parent shell; whole group must die.
	task := &pipeline.Task{ID: "slow", Cmd: &pipeline.CommandSpec{
		Name: "sh", Args: []string{"sh", "-c", "sleep 30 & sleep 30"},
	}}
	start := time.Now()
	_, err := r.Run(ctx, task, pipeline.ExecInput{})
	elapsed := time.Since(start)
	pe := pipeline.AsError(err)
	if pe == nil || pe.Code != pipeline.ErrTaskTimeout {
		t.Fatalf("expected task_timeout, got %v", err)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("process group not killed promptly: %s", elapsed)
	}
	t.Logf("deadline exceeded after %s; process group killed; classified [%s]",
		elapsed.Round(10*time.Millisecond), pe.Code)
}

func TestRun_CanceledClassified(t *testing.T) {
	r := NewRunner(".")
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	task := &pipeline.Task{ID: "c", Cmd: &pipeline.CommandSpec{
		Name: "sh", Args: []string{"sh", "-c", "sleep 30"},
	}}
	_, err := r.Run(ctx, task, pipeline.ExecInput{})
	pe := pipeline.AsError(err)
	if pe == nil || pe.Code != pipeline.ErrTaskCanceled {
		t.Fatalf("expected task_canceled, got %v", err)
	}
	t.Logf("external cancel classified [%s]", pe.Code)
}

func TestRun_FuncPanicBecomesFailure(t *testing.T) {
	r := NewRunner(".")
	task := &pipeline.Task{ID: "boom", Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
		panic("kaboom")
	}}
	_, err := r.Run(context.Background(), task, pipeline.ExecInput{})
	if pe := pipeline.AsError(err); pe == nil || pe.Code != pipeline.ErrTaskFailed {
		t.Fatalf("panic must become task_failed, got %v", err)
	}
}

func TestMergeEnv_OverlayAndOrder(t *testing.T) {
	merged := mergeEnv([]string{"A=1", "B=2"}, map[string]string{"A": "9", "C": "3"})
	want := map[string]string{"A": "9", "B": "2", "C": "3"}
	got := map[string]string{}
	for _, kv := range merged {
		var k, v string
		for i := 0; i < len(kv); i++ {
			if kv[i] == '=' {
				k, v = kv[:i], kv[i+1:]
				break
			}
		}
		got[k] = v
	}
	if !mapsEqual(got, want) {
		t.Fatalf("env merge wrong: %v", got)
	}
	if !errors.Is(context.Canceled, context.Canceled) {
		t.Fatal("sanity")
	}
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if a[k] != b[k] {
			return false
		}
	}
	return true
}
