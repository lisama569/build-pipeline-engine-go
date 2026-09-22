package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Spec is the JSON document describing a pipeline and its repository-native
// build/verification commands. JSON (rather than YAML) keeps the module
// dependency-free.
//
// Example:
//
//	{
//	  "name": "build",
//	  "tasks": [
//	    {"name": "vet", "program": "go", "args": ["vet", "./..."],
//	     "inputs": ["go.mod"], "cacheable": true, "idempotent": true}
//	  ]
//	}
type Spec struct {
	Name  string     `json:"name"`
	Tasks []TaskSpec `json:"tasks"`
}

// TaskSpec is the on-disk form of one task.
type TaskSpec struct {
	Name       string            `json:"name"`
	Deps       []string          `json:"deps,omitempty"`
	Program    string            `json:"program"`
	Args       []string          `json:"args,omitempty"`
	Dir        string            `json:"dir,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	Inputs     []string          `json:"inputs,omitempty"`
	Artifacts  []string          `json:"artifacts,omitempty"`
	Cacheable  bool              `json:"cacheable,omitempty"`
	Idempotent bool              `json:"idempotent,omitempty"`
	TimeoutMS  int               `json:"timeout_ms,omitempty"`
	Retries    int               `json:"retries,omitempty"`
}

// LoadSpec reads and decodes a JSON pipeline spec. Malformed documents are
// rejected with ReasonInvalidConfig rather than partially applied.
func LoadSpec(path string) (*Spec, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, newErr(ReasonInvalidConfig, "cannot read spec: "+err.Error())
	}
	var s Spec
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, newErr(ReasonInvalidConfig, "invalid spec JSON: "+err.Error())
	}
	if s.Name == "" {
		return nil, newErr(ReasonInvalidConfig, "spec is missing pipeline name")
	}
	if len(s.Tasks) == 0 {
		return nil, newErr(ReasonEmptyGraph, "spec declares no tasks")
	}
	seen := map[string]bool{}
	for _, t := range s.Tasks {
		if t.Name == "" {
			return nil, newErr(ReasonInvalidConfig, "task with empty name in spec")
		}
		if t.Program == "" {
			return nil, newErr(ReasonInvalidConfig, fmt.Sprintf("task %q is missing program", t.Name))
		}
		if seen[t.Name] {
			return nil, newErr(ReasonDuplicateTask, "task declared twice in spec", t.Name)
		}
		seen[t.Name] = true
		if t.Retries > 0 && !t.Idempotent {
			return nil, newErr(ReasonInvalidConfig,
				fmt.Sprintf("task %q sets retries but is not idempotent", t.Name), t.Name)
		}
	}
	return &s, nil
}

// ToPipeline converts a spec into a Pipeline, wiring each task to a command
// action. The effective environment is the spec env plus, only when the
// spec does not define them, PATH and HOME from the ambient environment so
// locally installed toolchains resolve; both participate in the cache key.
func (e *Engine) ToPipeline(s *Spec) (*Pipeline, error) {
	p := &Pipeline{Name: s.Name, Tasks: make([]TaskDef, 0, len(s.Tasks))}
	for _, ts := range s.Tasks {
		env := map[string]string{}
		for k, v := range ts.Env {
			env[k] = v
		}
		// Inject only PATH/HOME when absent; never the rest of ambient env.
		for _, name := range []string{"PATH", "HOME"} {
			if _, ok := env[name]; !ok {
				if v, present := os.LookupEnv(name); present {
					env[name] = v
				}
			}
		}

		action := CommandAction{
			Program: ts.Program,
			Args:    append([]string(nil), ts.Args...),
			Dir:     ts.Dir,
			Env:     env,
		}

		var timeout time.Duration
		if ts.TimeoutMS > 0 {
			timeout = parseDurationMS(ts.TimeoutMS)
		}

		t := TaskDef{
			Name:       ts.Name,
			Deps:       append([]string(nil), ts.Deps...),
			Run:        e.newCommandRunner(action, ts.Artifacts),
			Cacheable:  ts.Cacheable,
			Inputs:     append([]string(nil), ts.Inputs...),
			Timeout:    timeout,
			Retries:    ts.Retries,
			Idempotent: ts.Idempotent,
			actionKey:  action.stableDescriptor(),
			decode: func(payload []byte) any {
				var cr commandResult
				if err := decodePayload(payload, &cr); err != nil {
					return nil
				}
				return &cr
			},
		}
		p.Tasks = append(p.Tasks, t)
	}
	return p, nil
}

// parseDurationMS converts milliseconds to a duration.
func parseDurationMS(ms int) time.Duration { return time.Duration(ms) * time.Millisecond }
