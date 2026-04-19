package nntpDirectSearch

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Tensai75/nntpPool"
)

// This file contains helper functions for the DirectSearch implementation.

// log writes a message to the buffered log channel without blocking.
func (ds *DirectSearch) log(message string) {
	select {
	case ds.Log <- message:
	default:
	}
}

// getConn acquires a pooled connection and selects the current group.
func (ds *DirectSearch) getConn(ctx context.Context) (*nntpPool.NNTPConn, error) {
	conn, err := ds.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	_, _, _, err = conn.Group(ds.group)
	if err != nil {
		ds.pool.Put(conn)
		return nil, err
	}
	return conn, nil
}

// calcMaxBoundariesScannerIterations estimates worst-case boundary scan steps.
func (ds *DirectSearch) calcMaxBoundariesScannerIterations() uint {
	searchSpace := ds.groupLastArticle - ds.groupFirstArticle + 1
	// Binary search worst case is log2(n)
	maxIterations := uint(0)
	for searchSpace > ds.config.BoundariesScannerStep { // The window size
		searchSpace = searchSpace / 2
		maxIterations++
	}
	return 2 * maxIterations
}

// FormatNumberWithApostrophe formats n using apostrophes as thousand separators.
// For example, 12000 becomes "12'000".
func FormatNumberWithApostrophe(n uint) string {
	str := strconv.Itoa(int(n))

	// Handle negative numbers
	negative := ""
	if str[0] == '-' {
		negative = "-"
		str = str[1:]
	}

	// Process from right to left
	var parts []string
	for len(str) > 3 {
		parts = append([]string{str[len(str)-3:]}, parts...)
		str = str[:len(str)-3]
	}
	if len(str) > 0 {
		parts = append([]string{str}, parts...)
	}

	return negative + strings.Join(parts, "'")
}

// GetMD5Hash returns the lowercase hexadecimal MD5 hash of the input text.
func GetMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

// TimeMedian returns the median time from the input slice.
func TimeMedian(times []time.Time) time.Time {
	if len(times) == 0 {
		panic("cannot compute median of empty slice")
	}
	cpy := make([]time.Time, len(times))
	copy(cpy, times)
	sort.Slice(cpy, func(i, j int) bool {
		return cpy[i].Before(cpy[j])
	})
	mid := len(cpy) / 2
	if len(cpy)%2 == 1 {
		return cpy[mid]
	}
	low := cpy[mid-1].UnixNano()
	high := cpy[mid].UnixNano()
	return time.Unix(0, (low+high)/2).UTC()
}

// withinBoundaryTolerance reports whether the target time matches the median
// exactly or lies within the configured tolerance on the expected side.
func withinBoundaryTolerance(forFirst bool, targetDate, meanDate time.Time, toleranceSeconds uint) bool {
	if targetDate.Equal(meanDate) {
		return true
	}

	tolerance := time.Duration(toleranceSeconds) * time.Second
	if forFirst {
		lowerBound := meanDate.Add(-tolerance)
		return targetDate.Before(meanDate) && (targetDate.After(lowerBound) || targetDate.Equal(lowerBound))
	}

	upperBound := meanDate.Add(tolerance)
	return targetDate.After(meanDate) && (targetDate.Before(upperBound) || targetDate.Equal(upperBound))
}

// stopAndDrainTimer safely stops a timer and drains its channel to prevent leaks.
func stopAndDrainTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

// callbackNotifier provides a non-blocking, non-dropping callback queue.
// Producers only increment an atomic counter and send a wake-up signal.
// A single worker drains all pending callbacks serially.
type callbackNotifier struct {
	ctx      context.Context
	callback func()
	signal   chan struct{}
	pending  atomic.Uint64
	wg       sync.WaitGroup
	stopOnce sync.Once
}

// newCallbackNotifier creates and starts a callback notifier worker.
func newCallbackNotifier(ctx context.Context, callback func()) *callbackNotifier {
	if callback == nil {
		callback = func() {}
	}
	n := &callbackNotifier{
		ctx:      ctx,
		callback: callback,
		signal:   make(chan struct{}, 1),
	}
	n.wg.Go(func() {
		n.worker()
	})
	return n
}

// Enqueue schedules one callback execution without blocking the caller.
func (n *callbackNotifier) Enqueue() {
	if n == nil {
		return
	}
	select {
	case <-n.ctx.Done():
		return
	default:
	}
	n.pending.Add(1)
	select {
	case n.signal <- struct{}{}:
	default:
	}
}

// Stop stops the worker and flushes pending callbacks.
func (n *callbackNotifier) Stop() {
	if n == nil {
		return
	}
	n.stopOnce.Do(func() {
		close(n.signal)
		n.wg.Wait()
	})
}

func (n *callbackNotifier) worker() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case _, ok := <-n.signal:
			if !ok {
				n.processPending()
				return
			}
			n.processPending()
		}
	}
}

func (n *callbackNotifier) processPending() {
	for {
		pending := n.pending.Swap(0)
		if pending == 0 {
			return
		}
		for range pending {
			n.callback()
		}
	}
}
