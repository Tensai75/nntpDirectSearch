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
	iterationNotifier   *callbackNotifier
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

// boundarySearch holds state for a single boundary search operation.
type boundarySearch struct {
	low           uint
	high          uint
	overviewStart uint
	overviewEnd   uint
	lastStep      bool
	forFirst      bool
	direction     string
	target        string
	targetDate    time.Time
	noResultError error
	lastDate      time.Time
}

var (
	// ErrUnknownError indicates an unexpected internal failure.
	ErrUnknownError = fmt.Errorf("unknown error")
	// ErrUnexpectedError indicates an unexpected error that should be reported to the developer
	ErrUnexpectedError = fmt.Errorf("unexpected error - please report this error on https://github.com/Tensai75/nzb-monkey-go/issues")
	// ErrInvalidDateRange indicates the start date is after the end date.
	ErrInvalidDateRange = fmt.Errorf("invalid search date range")
	// ErrFirstMessageInGroupIsAfterEndDate indicates that the date of the first message in the group is after the end date.
	ErrFirstMessageInGroupIsAfterEndDate = fmt.Errorf("the date of the first message in the group is after the specified end date")
	// ErrLastMessageInGroupIsBeforeStartDate indicates that the date of the last message in the group is before the start date.
	ErrLastMessageInGroupIsBeforeStartDate = fmt.Errorf("the date of the last message in the group is before the specified start date")
	// ErrNoMessageFoundAfterStartDate indicates no message exists after the start date.
	ErrNoMessageFoundAfterStartDate = fmt.Errorf("no messages found after the specified start date")
	// ErrNoMessageFoundBeforeEndDate indicates no message exists before the end date.
	ErrNoMessageFoundBeforeEndDate = fmt.Errorf("no messages found before the specified end date")
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
	if !ds.boundariesScannerRunning.CompareAndSwap(false, true) {
		return BoundariesScannerResult{}, ErrBoundariesScannerAlreadyRunning
	}
	defer ds.boundariesScannerRunning.Store(false)

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
	}
	ds.boundariesScanner.iterationNotifier = newCallbackNotifier(boundariesCtx, iterationFunc)
	defer ds.boundariesScanner.iterationNotifier.Stop()

	// Start scanning for first and last message in parallel
	ds.log("Starting boundaries scanning")
	ds.log(fmt.Sprintf("Boundaries Scanner Step size: %d", ds.config.BoundariesScannerStep))
	ds.log(fmt.Sprintf("Boundaries Scanner Tolerance: %d seconds", ds.config.BoundariesScannerTolerance))

	// Get mean dates for the first messages
	firstProbeEnd := ds.groupLastArticle
	if ds.config.BoundariesScannerStep < (ds.groupLastArticle - ds.groupFirstArticle) {
		firstProbeEnd = ds.groupFirstArticle + ds.config.BoundariesScannerStep
	}
	firstMessages, err := ds.overview(ds.groupFirstArticle, firstProbeEnd)
	if err != nil {
		return BoundariesScannerResult{}, err
	}
	if len(firstMessages) == 0 {
		return BoundariesScannerResult{}, ErrNoMessagesFoundWithinRange
	}
	firstMessagesDates := make([]time.Time, len(firstMessages))
	for i := range firstMessages {
		firstMessagesDates[i] = firstMessages[i].Date
	}
	meanFirstMessagesDate := TimeMedian(firstMessagesDates)
	ds.boundariesScanner.iterationNotifier.Enqueue()

	// Get mean dates for the last messages
	lastProbeStart := ds.groupFirstArticle
	if ds.groupLastArticle > ds.config.BoundariesScannerStep {
		lastProbeStart = ds.groupLastArticle - ds.config.BoundariesScannerStep
	}
	lastMessages, err := ds.overview(lastProbeStart, ds.groupLastArticle)
	if err != nil {
		return BoundariesScannerResult{}, err
	}
	if len(lastMessages) == 0 {
		return BoundariesScannerResult{}, ErrNoMessagesFoundWithinRange
	}
	lastMessagesDates := make([]time.Time, len(lastMessages))
	for i := range lastMessages {
		lastMessagesDates[i] = lastMessages[i].Date
	}
	meanLastMessagesDate := TimeMedian(lastMessagesDates)
	ds.boundariesScanner.iterationNotifier.Enqueue()

	// Check if the date of the last message in the group is before the start date
	if meanLastMessagesDate.Before(ds.boundariesScanner.startDate) {
		ds.log("Mean date of last messages is before start date")
		return BoundariesScannerResult{}, ErrLastMessageInGroupIsBeforeStartDate
	}
	// Check if the date of the first message in the group is after the end date
	if meanFirstMessagesDate.After(ds.boundariesScanner.endDate) {
		ds.log("Mean date of first messages is after end date")
		return BoundariesScannerResult{}, ErrFirstMessageInGroupIsAfterEndDate
	}
	go ds.getFirstMessageNumber(meanFirstMessagesDate)
	go ds.getLastMessageNumber(meanLastMessagesDate)

	// Wait for message scans to complete (cancellation-aware)
	var firstMessageResult boundariesScannerResult
	var lastMessageResult boundariesScannerResult
	gotFirst := false
	gotLast := false
	for !gotFirst || !gotLast {
		select {
		case <-boundariesCtx.Done():
			return BoundariesScannerResult{}, boundariesCtx.Err()
		case result := <-ds.boundariesScanner.firstMessageChannel:
			firstMessageResult = result
			gotFirst = true
		case result := <-ds.boundariesScanner.lastMessageChannel:
			lastMessageResult = result
			gotLast = true
		}
	}
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
func (ds *DirectSearch) getFirstMessageNumber(meanFirstMessagesDate time.Time) {

	// Check if the start date is before the first message in the group
	if ds.boundariesScanner.startDate.Before(meanFirstMessagesDate) {
		ds.log("Start date is before mean date of first articles in group, setting first message number to first article in group")
		ds.boundariesScanner.firstMessageChannel <- boundariesScannerResult{ds.groupFirstArticle, meanFirstMessagesDate, nil}
		return
	}

	// Perform binary search for the first message
	result := ds.findMessageByDate(true)

	// Check for context cancellation
	select {
	case <-ds.boundariesScanner.ctx.Done():
		return
	default:
	}

	ds.boundariesScanner.firstMessageChannel <- result
}

// getLastMessageNumber finds the last message at or before the end date.
func (ds *DirectSearch) getLastMessageNumber(meanLastMessagesDate time.Time) {

	// Check if the end date is after the last message in the group
	if ds.boundariesScanner.endDate.After(meanLastMessagesDate) {
		ds.log("End date is after mean date of last articles in group, setting last message number to last article in group")
		ds.boundariesScanner.lastMessageChannel <- boundariesScannerResult{ds.groupLastArticle, meanLastMessagesDate, nil}
		return
	}

	// Perform binary search for the last message
	result := ds.findMessageByDate(false)

	// Check for context cancellation
	select {
	case <-ds.boundariesScanner.ctx.Done():
		return
	default:
	}

	ds.boundariesScanner.lastMessageChannel <- result
}

// findMessageByDate performs a binary search for a boundary message by date.
func (ds *DirectSearch) findMessageByDate(searchForFirst bool) boundariesScannerResult {

	var search boundarySearch

	if searchForFirst {
		search.forFirst = true
		search.direction = "up"
		search.target = "first"
		search.targetDate = ds.boundariesScanner.startDate
		search.noResultError = ErrNoMessageFoundAfterStartDate
	} else {
		search.forFirst = false
		search.direction = "down"
		search.target = "last"
		search.targetDate = ds.boundariesScanner.endDate
		search.noResultError = ErrNoMessageFoundBeforeEndDate
	}

	search.low = ds.groupFirstArticle
	search.high = ds.groupLastArticle
	search.lastStep = false

	// Binary search for the target message
	step := 0
	for search.low < search.high {
		step++

		// Check for context cancellation
		select {
		case <-ds.boundariesScanner.ctx.Done():
			return boundariesScannerResult{0, time.Time{}, ds.boundariesScanner.ctx.Err()}
		default:
		}

		// Ensure boundaries are within valid message range
		if search.low < ds.groupFirstArticle {
			search.low = ds.groupFirstArticle
		}
		if search.high > ds.groupLastArticle {
			search.high = ds.groupLastArticle
		}
		if search.low > search.high {
			break
		}

		// Calculate the mid point
		mid := search.low + (search.high-search.low)/2

		// Calculate the overview range as +/- overviewStep/2 around mid
		search.overviewStart = mid - (ds.config.BoundariesScannerStep / 2)
		search.overviewEnd = mid + (ds.config.BoundariesScannerStep / 2)

		// Ensure overviewStart and overviewEnd are within boundaries
		if search.overviewStart <= search.low {
			search.overviewStart = search.low
			search.lastStep = true
		}
		if search.overviewEnd >= search.high {
			search.overviewEnd = search.high
			search.lastStep = true
		}

		// Get overview for the calculated range with retries
		results, err := ds.overview(search.overviewStart, search.overviewEnd)
		if err != nil {
			return boundariesScannerResult{0, time.Time{}, err}
		}

		// Handle empty results - messages might be deleted in this range
		if len(results) == 0 {
			// If this is the last step, we cant go further, return result based on overview range and last date
			if search.lastStep {
				ds.log(fmt.Sprintf("Last step reached for %s message search but no messages found, returning result", search.target))
				message := uint(search.overviewEnd)
				if search.forFirst {
					message = uint(search.overviewStart)
				}
				return boundariesScannerResult{message, search.lastDate, nil}
			}
			// Update search bounds based on direction
			if search.direction == "up" {
				search.low = search.overviewEnd + 1
			} else {
				search.high = search.overviewStart - 1
			}
			ds.boundariesScanner.iterationNotifier.Enqueue()
			continue
		}

		// Calculate mean date of the overview results
		dates := make([]time.Time, len(results))
		for i := range results {
			dates[i] = results[i].Date
		}
		meanDate := TimeMedian(dates)

		// Get first and last message
		firstResult := results[0]
		lastResult := results[len(results)-1]

		// If this is the last step, return result
		if search.lastStep {
			ds.log(fmt.Sprintf("Last step reached for %s message search, returning result", search.target))
			if search.forFirst {
				return boundariesScannerResult{uint(firstResult.MessageNumber), meanDate, nil}
			} else {
				return boundariesScannerResult{uint(lastResult.MessageNumber), meanDate, nil}
			}
		}

		// Check if the mean date is close enough to the target date to return result
		if (search.forFirst && (search.targetDate.Before(meanDate) && search.targetDate.After(meanDate.Add(-time.Duration(ds.config.BoundariesScannerTolerance)*time.Second)))) ||
			(!search.forFirst && (search.targetDate.After(meanDate) && search.targetDate.Before(meanDate.Add(time.Duration(ds.config.BoundariesScannerTolerance)*time.Second)))) {
			ds.log(fmt.Sprintf("Target date %s is close to mean date %s for %s message search, returning result", search.targetDate.Format(time.RFC850), meanDate.Format(time.RFC850), search.target))
			if search.forFirst {
				return boundariesScannerResult{uint(firstResult.MessageNumber), meanDate, nil}
			} else {
				return boundariesScannerResult{uint(lastResult.MessageNumber), meanDate, nil}
			}
		}

		// Save last date for further use in case of empty results in next iteration
		search.lastDate = meanDate

		// Update search bounds based on comparison with target date
		if search.forFirst {
			if search.targetDate.Before(meanDate) {
				search.high = uint(firstResult.MessageNumber) - 1
				search.direction = "down"
				ds.boundariesScanner.iterationNotifier.Enqueue()
				continue
			}
			if search.targetDate.After(meanDate) {
				search.low = uint(lastResult.MessageNumber) + 1
				search.direction = "up"
				ds.boundariesScanner.iterationNotifier.Enqueue()
				continue
			}
		} else {
			if search.targetDate.After(meanDate) {
				search.low = uint(lastResult.MessageNumber) + 1
				search.direction = "up"
				ds.boundariesScanner.iterationNotifier.Enqueue()
				continue
			}
			if search.targetDate.Before(meanDate) {
				search.high = uint(firstResult.MessageNumber) - 1
				search.direction = "down"
				ds.boundariesScanner.iterationNotifier.Enqueue()
				continue
			}
		}

		// This should not be reachable, but if it does, log and return an error
		ds.log(fmt.Sprintf("Unexpected error in binary search for %s message: target date %s, mean date %s, first message date %s, last message date %s", search.target, search.targetDate.Format(time.RFC850), meanDate.Format(time.RFC850), firstResult.Date.Format(time.RFC850), lastResult.Date.Format(time.RFC850)))
		return boundariesScannerResult{0, time.Time{}, ErrUnexpectedError}
	}

	return boundariesScannerResult{0, time.Time{}, search.noResultError}
}

// overview fetches message overviews for the given range with retries and context cancellation support.
func (ds *DirectSearch) overview(overviewStart, overviewEnd uint) (results []nntp.MessageOverview, err error) {
	for range ds.config.OverviewRetries {
		// Check for context cancellation before each retry
		select {
		case <-ds.boundariesScanner.ctx.Done():
			return nil, ds.boundariesScanner.ctx.Err()
		default:
		}
		results, err = ds.boundariesScannerOverview(overviewStart, overviewEnd)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, ErrRequestFailed(ds.config.OverviewRetries, overviewStart, overviewEnd, err)
	}
	return results, nil
}

// boundariesScannerOverview fetches message overviews for the given range.
func (ds *DirectSearch) boundariesScannerOverview(begin, end uint) ([]nntp.MessageOverview, error) {
	ctx, cancel := context.WithCancel(ds.boundariesScanner.ctx)
	messagesChannel := make(chan []nntp.MessageOverview, 1)
	overviews := sync.WaitGroup{}
	var lastError atomic.Value
	conns := make([]*nntpPool.NNTPConn, ds.config.OverviewRetries)
	for i := range ds.config.OverviewRetries {
		idx := i
		overviews.Go(func() {
			var err error
			conns[idx], err = ds.getConn(ctx)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					lastError.Store(err.Error())
					return
				}
			}
			defer ds.pool.Put(conns[idx])
			select {
			case <-ctx.Done():
				return
			default:
			}
			messages, err := conns[idx].Overview(int(begin), int(end))
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					lastError.Store(err.Error())
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
			var err error
			loadedError := lastError.Load()
			if loadedError == nil {
				err = ErrUnknownError
			} else {
				err = fmt.Errorf("%v", loadedError)
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
