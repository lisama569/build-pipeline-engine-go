package engine_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/engine"
	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

func budgetCfg(budget time.Duration, grace time.Duration) pipeline.Config {
	return pipeline.Config{
		MaxParallelism:     2,
		MaxAttempts:        1,
		DefaultTaskTimeout: 30 * time.Second,
		ShutdownGrace:      grace,
		QueuePolicy:        pipeline.QueueFIFO,
		CacheDir:           "",
		RunBudget:          budget,
	}
}

// TestRunBudget_StopsDispatchingAndConverges: once the budget elapses no new
// task launches, running tasks are canceled and Run returns within grace.
func TestRunBudget_StopsDispatchingAndConverges(t *testing.T) {
	var started int32
	release := make(chan struct{})
	tasks := []*pipeline.Task{}
	for i := 0; i < 8; i++ {
		id := fmt.Sprintf("t%d", i)
		tasks = append(tasks, &pipeline.Task{
			ID: id, Timeout: 30 * time.Second,
			Run: func(ctx context.Context, _ pipeline.ExecInput) (pipeline.ExecOutput, error) {
				atomic.AddInt32(&started, 1)
				select {
				case <-ctx.Done():
					return pipeline.ExecOutput{}, ctx.Err()
				case <-release:
					return pipeline.ExecOutput{Digest: "d"}, nil
				}
			},
		})
	}
	eng, err := engine.NewEngine(budgetCfg(80*time.Millisecond, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	start := time.Now()
	rep, err := eng.Run(context.Background(), &pipeline.Pipeline{Name: "budget", Tasks: tasks})
	elapsed := time.Since(start)
	close(release)
	if err != nil {
		t.Fatal(err)
	}

	// Convergence must be bounded by grace (plus scheduling slack): the run
	// must never hang waiting on tasks that ignore cancellation... here they
	// honor it immediately.
	if elapsed > 2*time.Second {
		t.Fatalf("run did not converge in bounded time after budget: %s", elapsed)
	}
	if rep.StopReason != string(pipeline.StopBudgetExhausted) {
		t.Fatalf("stop_reason=%s want budget_exhausted", rep.StopReason)
	}
	if rep.Status != string(pipeline.StopBudgetExhausted) {
		t.Fatalf("status=%s want budget_exhausted", rep.Status)
	}
	if rep.RunBudget != "80ms" {
		t.Fatalf("run_budget reported as %q", rep.RunBudget)
	}
	// With parallelism 2, at most the 2 initially dispatched tasks started;
	// after the budget no new dispatch happened.
	if got := atomic.LoadInt32(&started); got > 2 {
		t.Fatalf("budget must stop new dispatch, %d tasks started", got)
	}
	var budgetReason, canceledReason, scheduled int
	for _, tr := range rep.Tasks {
		switch {
		case tr.Status == pipeline.StatusCanceled && tr.Reason == string(pipeline.ErrRunBudget):
			budgetReason++
		case tr.Status == pipeline.StatusCanceled:
			canceledReason++
		case pipeline.IsSuccess(tr.Status):
			scheduled++
		}
	}
	if budgetReason == 0 {
		t.Fatalf("at least one task must be marked with run_budget_exhausted: %+v", rep.Tasks)
	}
	t.Logf("converged in %s: budget-canceled=%d other-canceled=%d succeeded=%d",
		elapsed, budgetReason, canceledReason, scheduled)
}

// TestRunBudget_DistinctFromTaskTimeout: a task that exceeds its own timeout
// is timed_out; the run completes naturally (no budget stop) when the budget
// is generous.
func TestRunBudget_DistinctFromTaskTimeout(t *testing.T) {
	tasks := []*pipeline.Task{{
		ID: "slow", Timeout: 30 * time.Millisecond,
		Run: func(ctx context.Context, _ pipeline.ExecInput) (pipeline.ExecOutput, error) {
			<-ctx.Done()
			return pipeline.ExecOutput{}, ctx.Err()
		},
	}}
	cfg := budgetCfg(10*time.Second, time.Second)
	eng, _ := engine.NewEngine(cfg)
	defer eng.Close()
	rep, err := eng.Run(context.Background(), &pipeline.Pipeline{Name: "to", Tasks: tasks})
	if err != nil {
		t.Fatal(err)
	}
	tr := rep.Tasks[0]
	if tr.Status != pipeline.StatusTimedOut {
		t.Fatalf("task status=%s want timed_out", tr.Status)
	}
	if tr.Reason != string(pipeline.ErrTaskTimeout) {
		t.Fatalf("task reason=%s want task_timeout", tr.Reason)
	}
	if rep.StopReason != string(pipeline.StopCompleted) {
		t.Fatalf("stop_reason=%s want completed", rep.StopReason)
	}
	if rep.Status != "failed" {
		t.Fatalf("run status=%s want failed (task timeout is a failure, not a budget stop)", rep.Status)
	}
}

// TestRunBudget_DistinctFromManualCancel: canceling the parent context while
// the budget is still open reports "canceled", not budget exhaustion.
func TestRunBudget_DistinctFromManualCancel(t *testing.T) {
	started := make(chan struct{}, 4)
	tasks := []*pipeline.Task{}
	for i := 0; i < 3; i++ {
		tasks = append(tasks, &pipeline.Task{
			ID: fmt.Sprintf("t%d", i), Timeout: 30 * time.Second,
			Run: func(ctx context.Context, _ pipeline.ExecInput) (pipeline.ExecOutput, error) {
				started <- struct{}{}
				<-ctx.Done()
				return pipeline.ExecOutput{}, ctx.Err()
			},
		})
	}
	cfg := budgetCfg(time.Hour, time.Second) // huge budget
	eng, _ := engine.NewEngine(cfg)
	defer eng.Close()
	ctx, cancel := context.WithCancel(context.Background())
	rch := make(chan runSummary, 1)
	go func() {
		r, _ := eng.Run(ctx, &pipeline.Pipeline{Name: "mc", Tasks: tasks})
		rch <- runSummary{stop: r.StopReason, status: r.Status}
	}()
	<-started
	cancel()
	select {
	case got := <-rch:
		if got.stop != string(pipeline.StopCanceled) {
			t.Fatalf("manual cancel stop_reason=%s want canceled", got.stop)
		}
		if got.status != "canceled" {
			t.Fatalf("manual cancel status=%s want canceled", got.status)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("manual cancel did not converge")
	}
}

type runSummary struct {
	stop   string
	status string
}

// TestRunBudget_NaturalCompletionBeforeBudget: everything finishing before
// the budget reports completed, not budget_exhausted.
func TestRunBudget_NaturalCompletionBeforeBudget(t *testing.T) {
	tasks := []*pipeline.Task{{
		ID: "fast", Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
			return pipeline.ExecOutput{Digest: "ok"}, nil
		},
	}}
	cfg := budgetCfg(time.Minute, time.Second)
	eng, _ := engine.NewEngine(cfg)
	defer eng.Close()
	rep, _ := eng.Run(context.Background(), &pipeline.Pipeline{Name: "fast", Tasks: tasks})
	if rep.StopReason != string(pipeline.StopCompleted) {
		t.Fatalf("stop_reason=%s want completed", rep.StopReason)
	}
	if rep.Status != "succeeded" {
		t.Fatalf("status=%s", rep.Status)
	}
}

// TestRunBudget_GraceBackstop: a task that ignores cancellation still lets
// the run converge within ShutdownGrace (force-converge path).
func TestRunBudget_GraceBackstop(t *testing.T) {
	tasks := []*pipeline.Task{{
		ID: "ignores-cancel", Timeout: 30 * time.Second,
		Run: func(context.Context, pipeline.ExecInput) (pipeline.ExecOutput, error) {
			time.Sleep(5 * time.Second) // ignores ctx
			return pipeline.ExecOutput{Digest: "late"}, nil
		},
	}}
	cfg := budgetCfg(30*time.Millisecond, 200*time.Millisecond)
	eng, _ := engine.NewEngine(cfg)
	defer eng.Close()
	start := time.Now()
	rep, _ := eng.Run(context.Background(), &pipeline.Pipeline{Name: "stuck", Tasks: tasks})
	elapsed := time.Since(start)
	if rep.StopReason != string(pipeline.StopBudgetExhausted) {
		t.Fatalf("stop_reason=%s", rep.StopReason)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("force convergence took too long: %s", elapsed)
	}
	t.Logf("ignored-cancel task force-converged after %s", elapsed)
}
