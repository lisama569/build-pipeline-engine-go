// Package pipeline is a local build pipeline orchestration and execution
// engine driven by the repository's existing build and verification entry
// points (go build / go test). See docs/ for scheduling, cache and retry
// semantics and local verification steps.
package pipeline
