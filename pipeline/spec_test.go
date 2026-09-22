package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadSpec_RejectsBadDocuments(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]string{
		"not json":        "{",
		"no name":         `{"tasks":[{"name":"a","program":"true"}]}`,
		"no tasks":        `{"name":"p"}`,
		"empty task name": `{"name":"p","tasks":[{"program":"true"}]}`,
		"missing program": `{"name":"p","tasks":[{"name":"a"}]}`,
		"duplicate":       `{"name":"p","tasks":[{"name":"a","program":"true"},{"name":"a","program":"true"}]}`,
		"non-idem retry":  `{"name":"p","tasks":[{"name":"a","program":"true","retries":2}]}`,
	}
	for label, body := range cases {
		path := filepath.Join(dir, strings.ReplaceAll(label, " ", "_")+".json")
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := LoadSpec(path)
		if err == nil {
			t.Fatalf("%s: expected rejection", label)
		}
	}
}

func TestSpecToPipeline_CommandEndToEnd(t *testing.T) {
	// Uses only locally available commands (no network/credentials).
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("/bin/sh not available")
	}
	dir := t.TempDir()
	specPath := filepath.Join(dir, "pipeline.json")
	body := `{
	  "name": "local",
	  "tasks": [
	    {"name":"prepare","program":"/bin/sh","args":["-c","echo ready > out.txt"]},
	    {"name":"check","deps":["prepare"],"program":"/bin/sh","args":["-c","test -f out.txt"]}
	  ]
	}`
	if err := os.WriteFile(specPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	spec, err := LoadSpec(specPath)
	if err != nil {
		t.Fatal(err)
	}
	e := newTestEngine(t, Config{RepoRoot: dir})
	p, err := e.ToPipeline(spec)
	if err != nil {
		t.Fatal(err)
	}
	r, err := e.Run(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != StatusSucceeded {
		for _, x := range r.Tasks {
			t.Logf("%s: %s/%s %s", x.Name, x.Status, x.Reason, x.Message)
		}
		t.Fatalf("e2e pipeline status=%s", r.Status)
	}
	if _, err := os.Stat(filepath.Join(dir, "out.txt")); err != nil {
		t.Fatalf("command side effect missing: %v", err)
	}
}

func TestSpecToPipeline_CommandFailureClassified(t *testing.T) {
	if _, err := os.Stat("/bin/sh"); err != nil {
		t.Skip("/bin/sh not available")
	}
	dir := t.TempDir()
	e := newTestEngine(t, Config{RepoRoot: dir})
	s := &Spec{Name: "f", Tasks: []TaskSpec{
		{Name: "bad", Program: "/bin/sh", Args: []string{"-c", "echo err >&2; exit 7"}},
		{Name: "down", Deps: []string{"bad"}, Program: "/bin/sh", Args: []string{"-c", "echo nope"}},
	}}
	p, err := e.ToPipeline(s)
	if err != nil {
		t.Fatal(err)
	}
	r, perr := e.Run(context.Background(), p)
	if perr != nil {
		t.Fatal(perr)
	}
	by := map[string]*TaskResult{}
	for _, x := range r.Tasks {
		by[x.Name] = x
	}
	if by["bad"].Status != StatusFailed || by["bad"].Reason != ReasonFailed {
		t.Fatalf("bad = %s/%s msg=%s", by["bad"].Status, by["bad"].Reason, by["bad"].Message)
	}
	if !strings.Contains(by["bad"].Message, "err") {
		t.Fatalf("stderr should be surfaced: %s", by["bad"].Message)
	}
	if by["down"].Status != StatusSkipped || by["down"].Reason != ReasonUpstream {
		t.Fatalf("down = %s/%s", by["down"].Status, by["down"].Reason)
	}
}
