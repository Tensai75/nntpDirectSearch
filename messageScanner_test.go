package nntpDirectSearch

import (
	"context"
	"testing"
	"time"
)

func TestMaybeRestartOverviewScannerDoesNotBlockWhenAllWorkersTimeOutTogether(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const workers = 4
	ds := &DirectSearch{
		messageScanner: &messageScanner{
			ctx:                    ctx,
			overviewScannerLimiter: make(chan struct{}, workers),
		},
	}

	// Simulate all active overview readers timing out while still holding their
	// limiter slots. Restart scheduling must not block the callers.
	for range workers {
		ds.messageScanner.overviewScannerLimiter <- struct{}{}
	}

	returned := make(chan struct{}, workers)
	for i := range workers {
		go func(offset uint) {
			ds.maybeRestartOverviewScanner(1, 100+offset*10, 200+offset*10, 0)
			returned <- struct{}{}
		}(uint(i))
	}

	deadline := time.After(500 * time.Millisecond)
	for range workers {
		select {
		case <-returned:
		case <-deadline:
			t.Fatal("simultaneous timeout restarts blocked while the limiter was full")
		}
	}

	cancel()
	ds.messageScanner.overviewScannerWG.Wait()
}
