package engine_test

import (
	"testing"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/engine"
	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

func TestConfig_ResumeWithoutStateDirRejected(t *testing.T) {
	_, err := engine.NewEngine(pipeline.Config{
		MaxParallelism: 1, Resume: true, StateDir: "",
	})
	if pe := pipeline.AsError(err); pe == nil || pe.Code != pipeline.ErrConfigInvalid {
		t.Fatalf("want config_invalid, got %v", err)
	}
}

func TestConfig_NegativeBudgetRejected(t *testing.T) {
	_, err := engine.NewEngine(pipeline.Config{
		MaxParallelism: 1, RunBudget: -time.Second,
	})
	if pe := pipeline.AsError(err); pe == nil || pe.Code != pipeline.ErrConfigInvalid {
		t.Fatalf("want config_invalid, got %v", err)
	}
}

func TestConfig_BudgetAndHistoryClamped(t *testing.T) {
	eng, err := engine.NewEngine(pipeline.Config{
		MaxParallelism: 1,
		RunBudget:      1000 * time.Hour, // above 24h hard cap
		StateDir:       t.TempDir(),
		HistoryRuns:    10_000, // above 50 hard cap
	})
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
}
