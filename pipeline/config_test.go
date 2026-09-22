package pipeline

import (
	"testing"
	"time"
)

func TestValidateConfig_BoundsAndDefaults(t *testing.T) {
	c, err := ValidateConfig(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if c.Parallelism != defaultParallelism || c.MaxRetries != hardMaxRetries {
		t.Fatalf("defaults not applied: %+v", c)
	}

	cases := []struct {
		name string
		cfg  Config
		want Reason
	}{
		{"negative parallelism", Config{Parallelism: -1}, ReasonInvalidConfig},
		{"parallelism over cap", Config{Parallelism: maxParallelism + 1}, ReasonInvalidConfig},
		{"negative retries", Config{DefaultRetries: -1}, ReasonInvalidConfig},
		{"retries over hard cap", Config{MaxRetries: hardMaxRetries + 1}, ReasonInvalidConfig},
		{"negative timeout", Config{DefaultTimeout: -time.Second}, ReasonInvalidConfig},
		{"timeout over hard cap", Config{DefaultTimeout: hardMaxTimeout + time.Second}, ReasonInvalidConfig},
		{"negative backoff", Config{RetryBackoff: -time.Millisecond}, ReasonInvalidConfig},
		{"grace over hard cap", Config{ShutdownGrace: hardShutdownGrace + time.Second}, ReasonInvalidConfig},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ValidateConfig(tc.cfg)
			if err == nil || AsError(err).Reason != tc.want {
				t.Fatalf("want %s, got %v", tc.want, err)
			}
		})
	}
}
