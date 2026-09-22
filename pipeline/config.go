package pipeline

import (
	"fmt"
	"time"
)

// hard limits enforced by ValidateConfig (also used in tests).
const (
	maxParallelism     = 256
	hardMaxRetries     = 10
	hardMaxTimeout     = 30 * time.Minute
	hardMaxBackoff     = 5 * time.Minute
	hardShutdownGrace  = 2 * time.Minute
	defaultParallelism = 4
	defaultRetries     = 0
	defaultTimeout     = 5 * time.Minute
	defaultBackoff     = 100 * time.Millisecond
	defaultMaxBackoff  = 2 * time.Second
	defaultGrace       = 10 * time.Second
)

// DefaultConfig returns conservative bounded defaults suitable for local
// execution. No external services or credentials are involved.
func DefaultConfig() Config {
	return Config{
		Parallelism:     defaultParallelism,
		DefaultRetries:  defaultRetries,
		MaxRetries:      hardMaxRetries,
		DefaultTimeout:  defaultTimeout,
		MaxTimeout:      hardMaxTimeout,
		RetryBackoff:    defaultBackoff,
		MaxRetryBackoff: defaultMaxBackoff,
		ShutdownGrace:   defaultGrace,
	}
}

// ValidateConfig normalizes and bounds c. It returns ReasonInvalidConfig
// errors when a value is negative or exceeds a hard engine limit, so that
// callers can never configure the engine into unbounded resource use.
func ValidateConfig(c Config) (Config, error) {
	d := DefaultConfig()

	if c.Parallelism == 0 {
		c.Parallelism = d.Parallelism
	}
	if c.Parallelism < 0 {
		return c, newErr(ReasonInvalidConfig, "parallelism must not be negative")
	}
	if c.Parallelism > maxParallelism {
		return c, newErr(ReasonInvalidConfig, fmt.Sprintf("parallelism exceeds engine hard limit %d", maxParallelism))
	}

	if c.DefaultRetries < 0 {
		return c, newErr(ReasonInvalidConfig, "default retries must not be negative")
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = d.MaxRetries
	}
	if c.MaxRetries < 0 || c.MaxRetries > hardMaxRetries {
		return c, newErr(ReasonInvalidConfig, "max retries out of bounded range")
	}
	if c.DefaultRetries > c.MaxRetries {
		return c, newErr(ReasonInvalidConfig, "default retries exceed max retries")
	}

	if c.DefaultTimeout == 0 {
		c.DefaultTimeout = d.DefaultTimeout
	}
	if c.MaxTimeout == 0 {
		c.MaxTimeout = d.MaxTimeout
	}
	if c.DefaultTimeout < 0 || c.MaxTimeout < 0 {
		return c, newErr(ReasonInvalidConfig, "timeouts must not be negative")
	}
	if c.MaxTimeout > hardMaxTimeout || c.DefaultTimeout > hardMaxTimeout {
		return c, newErr(ReasonInvalidConfig, "timeout exceeds engine hard limit")
	}
	if c.DefaultTimeout > c.MaxTimeout {
		return c, newErr(ReasonInvalidConfig, "default timeout exceeds max timeout")
	}

	if c.RetryBackoff == 0 {
		c.RetryBackoff = d.RetryBackoff
	}
	if c.MaxRetryBackoff == 0 {
		c.MaxRetryBackoff = d.MaxRetryBackoff
	}
	if c.RetryBackoff < 0 || c.MaxRetryBackoff < 0 {
		return c, newErr(ReasonInvalidConfig, "backoff must not be negative")
	}
	if c.MaxRetryBackoff > hardMaxBackoff || c.RetryBackoff > hardMaxBackoff {
		return c, newErr(ReasonInvalidConfig, "backoff exceeds engine hard limit")
	}
	if c.RetryBackoff > c.MaxRetryBackoff {
		return c, newErr(ReasonInvalidConfig, "retry backoff exceeds max backoff")
	}

	if c.ShutdownGrace == 0 {
		c.ShutdownGrace = d.ShutdownGrace
	}
	if c.ShutdownGrace < 0 {
		return c, newErr(ReasonInvalidConfig, "shutdown grace must not be negative")
	}
	if c.ShutdownGrace > hardShutdownGrace {
		return c, newErr(ReasonInvalidConfig, "shutdown grace exceeds engine hard limit")
	}

	// EnvPassthrough is sorted on use (Fingerprint); RepoRoot and CacheDir
	// are resolved lazily by callers/engine.
	return c, nil
}

// killGrace is the bounded delay between SIGTERM and SIGKILL when stopping
// a command whose context has expired.
const killGrace = 2 * time.Second
