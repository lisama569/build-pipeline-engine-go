package pipeline

import "time"

func sleepMs(ms int) { time.Sleep(time.Duration(ms) * time.Millisecond) }
