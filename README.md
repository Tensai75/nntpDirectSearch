# nntpDirectSearch

A high-performance Go library for efficient NNTP (Network News Transfer Protocol) group scanning and searching. It provides utilities to locate message boundaries by date and build NZB files from message subject parsing and matching using concurrent workers and connection pooling.

## Features

- **Concurrent Message Scanning**: Uses multiple concurrent workers to efficiently scan large NNTP groups
- **Connection Pooling**: Manages a shared pool of NNTP connections for optimal resource utilization
- **Date-Based Boundary Detection**: Find the first and last messages within a specific date range using binary search over overview windows
- **NZB FIle Generation**: Build NZB files from message subject parsing and matching
- **Progress Tracking**: Built-in metrics for monitoring lines read and bytes processed
- **Configurable Timeouts & Retries**: Tune overview request timeouts and retry strategies
- **Context Support**: Full support for cancellable operations via Go contexts
- **Debug Logging**: Non-blocking debug logging via a buffered channel

## Installation

```bash
go get github.com/Tensai75/nntpDirectSearch
```

## Dependencies

This library requires:

- `github.com/Tensai75/nntpPool` - NNTP connection pooling
- `github.com/Tensai75/nntp` - NNTP protocol implementation
- `github.com/Tensai75/nzbparser` - NZB file parsing and generation
- `github.com/Tensai75/subjectparser` - Subject line parsing for NZB metadata

## Usage

### Basic Setup

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/Tensai75/nntpDirectSearch"
	"github.com/Tensai75/nntpPool"
)

func main() {
	// Create a connection pool
	pool := nntpPool.NewPool("news.server.com:119", 10, time.Minute)

	// Create a DirectSearch instance
	ds, err := nntpDirectSearch.New(pool, context.Background())
	if err != nil {
		log.Fatal(err)
	}

	// Optionally configure scanning behavior
	config := nntpDirectSearch.DirectSearchConfig{
		Connections:                20,    // Number of concurrent connections
		Step:                       20000, // MessageScanner range step size
		OverviewTimeout:            5,     // Timeout in seconds
		OverviewRetries:            3,     // Number of retries
		BoundariesScannerStep:      1000,  // Overview window size used by BoundariesScanner
		BoundariesScannerTolerance: 15,    // Date tolerance in seconds for boundary convergence
	}
	if err := ds.SetConfig(config); err != nil {
		log.Fatal(err)
	}

	// Select a newsgroup
	if err := ds.SwitchToGroup("alt.binaries.example"); err != nil {
		log.Fatal(err)
	}

	// Read debug logs (optional)
	go func() {
		for msg := range ds.Log {
			log.Println("[DEBUG]", msg)
		}
	}()

	// Use the scanners...
}
```

### Finding Message Boundaries by Date

Use `BoundariesScanner` to locate the first and last messages within a specific date range:

`BoundariesScanner` does not compare only single-message timestamps. It fetches overview windows and uses the **median timestamp** of each window as the representative "average" date while converging on first/last boundaries.

Using a median over a window is more robust than using a single article date, because occasional outlier timestamps do not significantly skew the boundary decision.

```go
startDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
endDate := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC)

// Progress callback (optional)
iteration := 0
progressFunc := func() {
	log.Printf("Progress: %d/%d iterations\n",
		iteration++,
		ds.MaxBoundariesScannerIterations)
}

result, err := ds.BoundariesScanner(startDate, endDate, progressFunc)
if err != nil {
	log.Fatal(err)
}

log.Printf("First message: ID=%d, Date=%v\n",
	result.FirstMessage.MessageID,
	result.FirstMessage.Date)
log.Printf("Last message: ID=%d, Date=%v\n",
	result.LastMessage.MessageID,
	result.LastMessage.Date)
```

### Scanning Messages for Content

Use `MessageScanner` to search for messages containing a specific header and build NZB files:

```go
// Scan messages from 100000 to 200000 for header "Subject:"
linesToRead := 20000 - 10000 + 1
progressFunc := func() {
	linesRead := ds.GetLinesRead()
	bytesRead := ds.GetBytesRead()
	log.Printf("Scanned: %d/%d lines, %d bytes\n", linesRead, linesToRead, bytesRead)
}

nzbResults, err := ds.MessageScanner("Subject:", 100000, 200000, progressFunc)
if err != nil {
	log.Fatal(err)
}

for _, nzb := range nzbResults {
	log.Printf("Found NZB: %s\n", nzb.Head)
}
```

## Configuration

The `DirectSearchConfig` struct controls scanning behavior:

```go
type DirectSearchConfig struct {
	Connections                uint // Concurrent connections (must be > 0)
	Step                       uint // MessageScanner range step size (must be > 0)
	OverviewTimeout            uint // Overview request timeout in seconds (must be > 0)
	OverviewRetries            uint // Number of retries (must be > 0)
	BoundariesScannerStep      uint // Overview window size for BoundariesScanner (must be > 0)
	BoundariesScannerTolerance uint // Date tolerance in seconds for BoundariesScanner (must be > 0)
}
```

All fields are required and must be greater than zero. Validation errors are returned via `SetConfig()`.

## Error Handling

Common error scenarios:

- `ErrNoGroupSelected` - A scan was attempted without selecting a group first
- `ErrGroupHasNoArticles` - The selected group contains no usable articles
- `ErrInvalidMessageRange` - Invalid message range (e.g. start/end is 0, or start > end)
- `ErrInvalidDateRange` - Start date is after end date
- `ErrNoMessageFoundAfterStartDate` - No messages exist on or after the start date
- `ErrNoMessageFoundBeforeEndDate` - No messages exist on or before the end date
- `ErrFirstMessageInGroupIsAfterEndDate` - Group's first-message median is after search end date
- `ErrLastMessageInGroupIsBeforeStartDate` - Group's last-message median is before search start date
