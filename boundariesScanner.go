package nntpDirectSearch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Tensai75/nntp"
	"github.com/Tensai75/nntpPool"
)

// boundariesScanner holds state for concurrent boundary scans.
type boundariesScanner struct {
	ctx                 context.Context
	ctxCancel           context.CancelFunc
	startDate           time.Time
	endDate             time.Time
	firstMessageChannel chan boundariesScannerResult
	lastMessageChannel  chan boundariesScannerResult
	iterationFunc       func()
}

// boundariesScannerResult captures a boundary scan result or error.
type boundariesScannerResult struct {
	messageID uint
	date      time.Time
	err       error
}

// BoundariesScannerResult contains the first and last messages found within the
// specified date range.
type BoundariesScannerResult struct {
	FirstMessage BoundariesScannerResultMessage
	LastMessage  BoundariesScannerResultMessage
}

// BoundariesScannerResultMessage describes a single boundary message result.
type BoundariesScannerResultMessage struct {
	MessageID uint
	Date      time.Time
}

// boundariesScannerStep is the overview window size used by the boundary scan.
const boundariesScannerStep = uint(1000)

var (
	// ErrUnknownError indicates an unexpected internal failure.
	ErrUnknownError = fmt.Errorf("unknown error")
	// ErrInvalidDateRange indicates the start date is after the end date.
	ErrInvalidDateRange = fmt.Errorf("invalid search date range")
	// ErrOldestMessageNewerThanEndDate indicates the group's oldest message is newer than the end date.
	ErrOldestMessageNewerThanEndDate = fmt.Errorf("the oldest message in the group is newer than the specified end date")
	// ErrNewestMessageOlderThanStartDate indicates the group's newest message is older than the start date.
	ErrNewestMessageOlderThanStartDate = fmt.Errorf("the newest message in the group is older than the specified start date")
	// ErrNoMessageFoundAfterStartDate indicates no message exists on or after the start date.
	ErrNoMessageFoundAfterStartDate = fmt.Errorf("no messages found on or after the specified start date")
	// ErrNoMessageFoundBeforeEndDate indicates no message exists on or before the end date.
	ErrNoMessageFoundBeforeEndDate = fmt.Errorf("no messages found on or before the specified end date")
	// ErrNoMessagesFoundWithinRange indicates no messages were found within the search range.
	ErrNoMessagesFoundWithinRange = fmt.Errorf("no messages found within search range")
	// ErrAllRequestsTimedOut indicates all overview requests exceeded the timeout.
	ErrAllRequestsTimedOut = fmt.Errorf("all requests timed out")
	// ErrAllRequestsFailed wraps the last error seen after all overview requests failed.
	ErrAllRequestsFailed = func(err error) error {
		return fmt.Errorf("all requests failed - last error: %v", err)
	}
	// ErrRequestFailed wraps a failed overview request with retry count and range.
	ErrRequestFailed = func(retries, overviewStart, overviewEnd uint, err error) error {
		return fmt.Errorf("request failed after %d attempts for range %d-%d: %w", retries, overviewStart, overviewEnd, err)
	}
)

// BoundariesScanner finds the first and last messages within the given date
// range by performing parallel scans.
// The optional iterationFunc is invoked on each scan iteration and can be used
// to report progress.
// It returns an error if no group is selected, if the date range is invalid, if
// no messages are found within the range, or if there is an issue with NNTP requests.
func (ds *DirectSearch) BoundariesScanner(startDate, endDate time.Time, iterationFunc func()) (BoundariesScannerResult, error) {
	if ds.group == "" {
		return BoundariesScannerResult{}, ErrNoGroupSelected
	}
	if startDate.After(endDate) {
		return BoundariesScannerResult{}, ErrInvalidDateRange
	}

	if iterationFunc == nil {
		iterationFunc = func() {}
	}

	boundariesCtx, boundariesCtxCancel := context.WithCancel(ds.ctx)
	defer boundariesCtxCancel()

	ds.boundariesScanner = &boundariesScanner{
		ctx:                 boundariesCtx,
		ctxCancel:           boundariesCtxCancel,
		startDate:           startDate,
		endDate:             endDate,
		firstMessageChannel: make(chan boundariesScannerResult, 1),
		lastMessageChannel:  make(chan boundariesScannerResult, 1),
		iterationFunc:       iterationFunc,
	}

	// Start scanning for first and last message in parallel
	ds.log("Starting boundaries scanning")
	go ds.getFirstMessageNumber()
	go ds.getLastMessageNumber()

	// Wait for message scan to complete
	firstMessageResult := <-ds.boundariesScanner.firstMessageChannel
	lastMessageResult := <-ds.boundariesScanner.lastMessageChannel
	ds.log("Boundaries scanning completed")

	// Close channels
	close(ds.boundariesScanner.firstMessageChannel)
	close(ds.boundariesScanner.lastMessageChannel)

	// Check for context cancellation
	select {
	case <-boundariesCtx.Done():
		return BoundariesScannerResult{}, boundariesCtx.Err()
	default:
	}

	// Handle errors
	var boundariesScannerError error
	if firstMessageResult.err != nil {
		ds.log(fmt.Sprintf("First message scan failed: %v", firstMessageResult.err))
		boundariesScannerError = firstMessageResult.err
	}
	if lastMessageResult.err != nil {
		ds.log(fmt.Sprintf("Last message scan failed: %v", lastMessageResult.err))
		boundariesScannerError = lastMessageResult.err
	}
	if boundariesScannerError != nil {
		return BoundariesScannerResult{}, boundariesScannerError
	}

	// handle special errors
	if firstMessageResult.messageID >= lastMessageResult.messageID {
		return BoundariesScannerResult{}, ErrNoMessagesFoundWithinRange
	}

	return BoundariesScannerResult{
		FirstMessage: BoundariesScannerResultMessage{
			MessageID: firstMessageResult.messageID,
			Date:      firstMessageResult.date,
		},
		LastMessage: BoundariesScannerResultMessage{
			MessageID: lastMessageResult.messageID,
			Date:      lastMessageResult.date,
		},
	}, nil
}

// getFirstMessageNumber finds the first message at or after the start date.
func (ds *DirectSearch) getFirstMessageNumber() {
	result := ds.findMessageByDate(true)

	// Check for context cancellation
	select {
	case <-ds.boundariesScanner.ctx.Done():
		return
	default:
	}

	if result.err != nil {
		ds.boundariesScanner.firstMessageChannel <- boundariesScannerResult{0, time.Time{}, result.err}
		return
	}
	if result.date.After(ds.boundariesScanner.endDate) {
		ds.boundariesScanner.firstMessageChannel <- boundariesScannerResult{0, time.Time{}, ErrOldestMessageNewerThanEndDate}
		return
	}
	ds.boundariesScanner.firstMessageChannel <- result
}

// getLastMessageNumber finds the last message at or before the end date.
func (ds *DirectSearch) getLastMessageNumber() {
	result := ds.findMessageByDate(false)

	// Check for context cancellation
	select {
	case <-ds.boundariesScanner.ctx.Done():
		return
	default:
	}

	if result.err != nil {
		ds.boundariesScanner.lastMessageChannel <- boundariesScannerResult{0, time.Time{}, result.err}
		return
	}
	if result.date.Before(ds.boundariesScanner.startDate) {
		ds.boundariesScanner.lastMessageChannel <- boundariesScannerResult{0, time.Time{}, ErrNewestMessageOlderThanStartDate}
		return
	}
	ds.boundariesScanner.lastMessageChannel <- result
}

// findMessageByDate performs a binary search for a boundary message by date.
func (ds *DirectSearch) findMessageByDate(searchForFirst bool) boundariesScannerResult {

	low := ds.groupFirstArticle
	high := ds.groupLastArticle

	var direction string
	var targetDate time.Time
	var noResultError error
	var boundariesError error

	if searchForFirst {
		direction = "up"
		targetDate = ds.boundariesScanner.startDate
		noResultError = ErrNoMessageFoundAfterStartDate
		boundariesError = ErrNewestMessageOlderThanStartDate
	} else {
		direction = "down"
		targetDate = ds.boundariesScanner.endDate
		noResultError = ErrNoMessageFoundBeforeEndDate
		boundariesError = ErrOldestMessageNewerThanEndDate
	}

	var lastStep = false
	var lastResult boundariesScannerResult

	// Binary search for the target message
	for low <= high {

		// Check for context cancellation
		select {
		case <-ds.boundariesScanner.ctx.Done():
			return boundariesScannerResult{0, time.Time{}, ds.boundariesScanner.ctx.Err()}
		default:
		}

		// Ensure boundaries are within valid message range
		if low < ds.groupFirstArticle {
			low = ds.groupFirstArticle
		}
		if high > ds.groupLastArticle {
			high = ds.groupLastArticle
		}
		if low > high {
			break
		}

		// Calculate the mid point
		mid := low + (high-low)/2

		// Calculate the overview range as +/- overviewStep/2 around mid
		overviewStart := mid - (boundariesScannerStep / 2)
		overviewEnd := mid + (boundariesScannerStep / 2)

		// Ensure overviewStart and overviewEnd are within boundaries
		if overviewStart <= low {
			overviewStart = low
			lastStep = true
		}
		if overviewEnd >= high {
			overviewEnd = high
			lastStep = true
		}

		// Request overview for the calculated range
		results := []nntp.MessageOverview{}
		var err error
		for range ds.config.OverviewRetries {

			// Check for context cancellation before each retry
			select {
			case <-ds.boundariesScanner.ctx.Done():
				return boundariesScannerResult{0, time.Time{}, ds.boundariesScanner.ctx.Err()}
			default:
			}

			results, err = ds.boundariesScannerOverview(overviewStart, overviewEnd)
			if err == nil {
				break
			}
		}
		if err != nil {
			return boundariesScannerResult{0, time.Time{}, ErrRequestFailed(ds.config.OverviewRetries, overviewStart, overviewEnd, err)}
		}

		// Handle empty results - messages might be deleted in this range
		if len(results) == 0 {
			// No messages available in this range
			// If this was the last step, we can't go further
			if lastStep {
				if (searchForFirst && overviewEnd == high) || (!searchForFirst && overviewStart == low) {
					return boundariesScannerResult{0, time.Time{}, boundariesError}
				} else {
					if lastResult != (boundariesScannerResult{}) {
						return lastResult
					} else {
						return boundariesScannerResult{0, time.Time{}, noResultError}
					}
				}
			}
			// Update search bounds based on direction
			if direction == "up" {
				low = overviewEnd + 1
			} else {
				high = overviewStart - 1
			}
			go ds.boundariesScanner.iterationFunc()
			continue
		}

		// Save appropriate result for potential use in last step
		if searchForFirst {
			lastResult = boundariesScannerResult{
				messageID: uint(results[0].MessageNumber),
				date:      results[0].Date,
				err:       nil,
			}
		} else {
			lastResult = boundariesScannerResult{
				messageID: uint(results[len(results)-1].MessageNumber),
				date:      results[len(results)-1].Date,
				err:       nil,
			}
		}

		// If this is the last step, scan the results directly
		if lastStep {
			result, found := scanResultsForTarget(results, targetDate, searchForFirst)
			if found {
				return boundariesScannerResult{uint(result.MessageNumber), result.Date, nil}
			}

			if (searchForFirst && overviewEnd == high) || (!searchForFirst && overviewStart == low) {
				return boundariesScannerResult{0, time.Time{}, boundariesError}
			} else {
				if searchForFirst {
					return boundariesScannerResult{uint(results[0].MessageNumber), results[0].Date, nil}
				} else {
					return boundariesScannerResult{uint(results[len(results)-1].MessageNumber), results[len(results)-1].Date, nil}
				}
			}
		}

		// Check only first and last message to determine search direction
		firstResult := results[0]
		lastResult := results[len(results)-1]

		// Update search bounds based on comparison with target date
		if searchForFirst {
			if targetDate.Before(firstResult.Date) {
				high = uint(firstResult.MessageNumber) - 1
				direction = "down"
				go ds.boundariesScanner.iterationFunc()
				continue
			}
			if targetDate.After(lastResult.Date) {
				low = uint(lastResult.MessageNumber) + 1
				direction = "up"
				go ds.boundariesScanner.iterationFunc()
				continue
			}
		} else {
			if targetDate.After(lastResult.Date) {
				low = uint(lastResult.MessageNumber) + 1
				direction = "up"
				go ds.boundariesScanner.iterationFunc()
				continue
			}
			if targetDate.Before(firstResult.Date) {
				high = uint(firstResult.MessageNumber) - 1
				direction = "down"
				go ds.boundariesScanner.iterationFunc()
				continue
			}
		}

		// Target date is between first and last message - scan the range directly
		result, found := scanResultsForTarget(results, targetDate, searchForFirst)
		if found {
			return boundariesScannerResult{uint(result.MessageNumber), result.Date, nil}
		}

		// Fallback - continue search
		ds.log(fmt.Sprintf("Unexpected condition encountered: direction %s / low %d / high %d", direction, low, high))
		if direction == "up" {
			low = overviewEnd + 1
		} else {
			high = overviewStart - 1
		}
		go ds.boundariesScanner.iterationFunc()
	}

	return boundariesScannerResult{0, time.Time{}, noResultError}
}

// boundariesScannerOverview fetches message overviews for the given range.
func (ds *DirectSearch) boundariesScannerOverview(begin, end uint) ([]nntp.MessageOverview, error) {
	ctx, cancel := context.WithCancel(ds.boundariesScanner.ctx)
	messagesChannel := make(chan []nntp.MessageOverview, 1)
	overviews := sync.WaitGroup{}
	var lastError atomic.Value
	conns := make([]*nntpPool.NNTPConn, ds.config.OverviewRetries)
	for i := range ds.config.OverviewRetries {
		overviews.Go(func() {
			var err error
			conns[i], err = ds.getConn(ctx)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					lastError.Store(err)
					return
				}
			}
			defer ds.pool.Put(conns[i])
			select {
			case <-ctx.Done():
				return
			default:
			}
			messages, err := conns[i].Overview(int(begin), int(end))
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					lastError.Store(err)
					return
				}
			}
			select {
			case <-ctx.Done():
				return
			case messagesChannel <- messages:
			default:
			}
		})
	}
	go func() {
		overviews.Wait()
		close(messagesChannel)
	}()
	select {
	case messageOverview, ok := <-messagesChannel:
		cancel()
		if !ok {
			err := lastError.Load().(error)
			if err == nil {
				err = ErrUnknownError
			}
			ds.log(fmt.Sprintf("All overview requests failed for range %d - %d: %v", begin, end, err))
			return nil, ErrAllRequestsFailed(err)
		}
		return messageOverview, nil
	case <-time.After(time.Duration(ds.config.OverviewTimeout) * time.Second):
		cancel()
		for i := range ds.config.OverviewRetries {
			if conns[i] != nil {
				conns[i].Close()
			}
		}
		ds.log(fmt.Sprintf("All overview requests timed out for range %d - %d", begin, end))
		return nil, ErrAllRequestsTimedOut
	}
}

// scanResultsForTarget scans a result set for the closest target date match.
func scanResultsForTarget(results []nntp.MessageOverview, targetDate time.Time, searchForFirst bool) (nntp.MessageOverview, bool) {
	if searchForFirst {
		// Scan forward for first message >= targetDate
		for _, result := range results {
			if !result.Date.Before(targetDate) {
				return result, true
			}
		}
	} else {
		// Scan backward for last message <= targetDate
		for i := len(results) - 1; i >= 0; i-- {
			result := results[i]
			if !result.Date.After(targetDate) {
				return result, true
			}
		}
	}
	return nntp.MessageOverview{}, false
}
