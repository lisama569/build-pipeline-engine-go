package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/lisama569/build-pipeline-engine-go/pipeline"
)

// Spec is the declarative pipeline definition (JSON; a small, dependency
// free subset sufficient to drive go build / go test locally).
type Spec struct {
	Name  string     `json:"name"`
	Tasks []SpecTask `json:"tasks"`
}

// SpecTask mirrors pipeline.Task for serialization.
type SpecTask struct {
	ID           string            `json:"id"`
	Deps         []string          `json:"deps,omitempty"`
	Command      []string          `json:"command,omitempty"`
	WorkDir      string            `json:"workdir,omitempty"`
	Env          map[string]string `json:"env,omitempty"`
	CacheInputs  map[string]string `json:"cache_inputs,omitempty"`
	Timeout      string            `json:"timeout,omitempty"`
	MaxAttempts  int               `json:"max_attempts,omitempty"`
	Idempotent   bool              `json:"idempotent,omitempty"`
	DisableCache bool              `json:"disable_cache,omitempty"`
}

// LoadSpec reads and converts a spec into a pipeline.
func LoadSpec(path string) (*pipeline.Pipeline, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read spec: %w", err)
	}
	var sp Spec
	if err := json.Unmarshal(data, &sp); err != nil {
		return nil, fmt.Errorf("parse spec: %w", err)
	}
	p := &pipeline.Pipeline{Name: sp.Name}
	for _, st := range sp.Tasks {
		t := &pipeline.Task{
			ID:             st.ID,
			Deps:           st.Deps,
			CacheKeyInputs: st.CacheInputs,
			MaxAttempts:    st.MaxAttempts,
			Idempotent:     st.Idempotent,
			DisableCache:   st.DisableCache,
		}
		if len(st.Command) > 0 {
			t.Cmd = &pipeline.CommandSpec{
				Name:    st.Command[0],
				Args:    st.Command,
				WorkDir: st.WorkDir,
				Env:     st.Env,
			}
		}
		if st.Timeout != "" {
			d, err := time.ParseDuration(st.Timeout)
			if err != nil {
				return nil, pipeline.NewError(pipeline.ErrConfigInvalid,
					"task %q: invalid timeout %q", st.ID, st.Timeout)
			}
			t.Timeout = d
		}
		p.Tasks = append(p.Tasks, t)
	}
	return p, nil
}
