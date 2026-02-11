package nntpDirectSearch

import (
	"bufio"
	"context"
	"fmt"
	"html"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Tensai75/nntp"
	"github.com/Tensai75/nntpPool"
	"github.com/Tensai75/nzbparser"
	"github.com/Tensai75/subjectparser"
)

// messageScanner holds state for concurrent overview and line scanning.
type messageScanner struct {
	header                 string
	ctx                    context.Context
	ctxCancel              context.CancelFunc
	firstMessage           uint
	lastMessage            uint
	overviewScannerWG      sync.WaitGroup
	overviewScannerLimiter chan struct{}
	linesScannerWG         sync.WaitGroup
	linesScannerChannel    chan string
	errorChannel           chan error
	results                map[string]map[string]nzbparser.NzbFile
	resultsMutex           sync.Mutex
	iterationFunc          func()
	lastError              atomic.Value
	messagesRead           atomic.Uint64
	bytesRead              atomic.Uint64
}

var (
	// ErrInvalidMessageRange indicates the provided message range is invalid.
	ErrInvalidMessageRange = fmt.Errorf("invalid message range")
	// ErrMessageScannerCancelled indicates the message scan was cancelled via context.
	ErrMessageScannerCancelled = fmt.Errorf("message scanner cancelled")
	// ErrOverviewReaderFailed indicates the overview reader exceeded retry limits.
	ErrOverviewReaderFailed = func(retries, first, last uint) error {
		return fmt.Errorf("overview reader failed after %d retries for range %d-%d", retries, first, last)
	}
	// ErrRetrievingMessageOverview wraps errors encountered while requesting overviews.
	ErrRetrievingMessageOverview = func(first, last uint, err error) error {
		return fmt.Errorf("retrieving message overview failed for range %d-%d: %v", first, last, err)
	}
)

// MessageScanner scans message overviews for a header substring and builds NZB
// results from matching subjects.
// The optional iterationFunc is invoked as messages are processed and can be
// used for progress reporting.
// It returns an error if no group is selected, if the message range is invalid, if
// no messages are found, or if there is an issue with NNTP requests.
func (ds *DirectSearch) MessageScanner(header string, firstMessage, lastMessage uint, iterationFunc func()) ([]*nzbparser.Nzb, error) {
	if ds.group == "" {
		return nil, ErrNoGroupSelected
	}
	if firstMessage >= lastMessage {
		return nil, ErrInvalidMessageRange
	}

	if iterationFunc == nil {
		iterationFunc = func() {}
	}

	messageScannerCtx, messageScannerCtxCancel := context.WithCancel(ds.ctx)
	ds.messageScanner = &messageScanner{
		header:                 header,
		ctx:                    messageScannerCtx,
		ctxCancel:              messageScannerCtxCancel,
		firstMessage:           firstMessage,
		lastMessage:            lastMessage,
		overviewScannerLimiter: make(chan struct{}, 2*ds.config.Connections),
		linesScannerChannel:    make(chan string, 10000),
		errorChannel:           make(chan error, 1),
		results:                make(map[string]map[string]nzbparser.NzbFile),
		iterationFunc:          iterationFunc,
	}

	// start line scanners
	ds.log("Starting line scanners")
	for range 5 {
		ds.messageScanner.linesScannerWG.Go(func() {
			ds.lineScanner()
		})
	}

	// start error handler
	go ds.messageScannerErrorHandler()

	// start overview scanners
	ds.log("Starting message overview scanners")
	last := firstMessage - 1
	first := firstMessage
	for last < lastMessage {
		first = last + 1
		last = min(first+ds.config.Step-1, lastMessage)
		select {
		case <-ds.messageScanner.ctx.Done():
			return nil, ErrMessageScannerCancelled
		case ds.messageScanner.overviewScannerLimiter <- struct{}{}:
			f := first
			l := last
			ds.messageScanner.overviewScannerWG.Go(func() {
				defer func() { <-ds.messageScanner.overviewScannerLimiter }()
				ds.overviewScanner(f, l, 0)
			})
		}
	}

	// Wait for all overview scanners to complete
	ds.messageScanner.overviewScannerWG.Wait()
	ds.log("All message overview scanner completed")
	close(ds.messageScanner.linesScannerChannel)
	close(ds.messageScanner.errorChannel)

	// Wait for all line scanners to complete
	ds.messageScanner.linesScannerWG.Wait()
	ds.log("All line scanners completed")

	// Check for errors
	err := ds.messageScanner.lastError.Load()
	if err != nil {
		return nil, err.(error)
	}

	// Collect results
	var nzbFiles []*nzbparser.Nzb
	for _, hit := range ds.messageScanner.results {
		var nzb = &nzbparser.Nzb{}
		for _, files := range hit {
			nzb.Files = append(nzb.Files, files)
		}
		nzbparser.MakeUnique(nzb)
		nzbparser.ScanNzbFile(nzb)
		sort.Sort(nzb.Files)
		for id := range nzb.Files {
			sort.Sort(nzb.Files[id].Segments)
		}
		nzbFiles = append(nzbFiles, nzb)
	}

	return nzbFiles, nil
}

// overviewScanner acquires an overview reader for a message range.
func (ds *DirectSearch) overviewScanner(first, last, restart uint) {
	if restart > ds.config.OverviewRetries {
		ds.messageScanner.errorChannel <- ErrOverviewReaderFailed(ds.config.OverviewRetries, first, last)
		return
	}
	var conn *nntpPool.NNTPConn
	var reader *bufio.Reader
	var err error
	for range ds.config.OverviewRetries {
		c, e := ds.getConn(ds.messageScanner.ctx)
		if e != nil {
			continue
		}
		select {
		case <-ds.messageScanner.ctx.Done():
			return
		default:
		}
		r, e := c.OverviewReader(int(first), int(last))
		if e != nil {
			ds.pool.Put(c)
			continue
		}
		conn = c
		reader = r
		err = e
		break
	}
	select {
	case <-ds.messageScanner.ctx.Done():
		return
	default:
	}
	if err != nil {
		ds.messageScanner.errorChannel <- ErrRetrievingMessageOverview(first, last, err)
		return
	}

	// Wrap the reader with a larger buffer to handle long overview lines
	// Some NNTP servers can return very long overview lines (>4KB default buffer)
	largeReader := bufio.NewReaderSize(reader, 128*1024) // 128KB buffer

	ds.overviewReader(conn, largeReader, first, last, restart)

}

// overviewReader streams overview lines and dispatches matching entries.
func (ds *DirectSearch) overviewReader(conn *nntpPool.NNTPConn, reader *bufio.Reader, first, last, restart uint) {
	for i := uint(1); ; i++ {
		select {
		case <-ds.messageScanner.ctx.Done():
			ds.pool.Put(conn)
			return
		default:
		}
		lineChan := make(chan string, 1)
		errorChan := make(chan error, 1)
		go func(r *bufio.Reader) {
			line, err := r.ReadString('\n')
			ds.messageScanner.bytesRead.Add(uint64(len(line)))
			if err != nil {
				errorChan <- err
				lineChan <- ""
			} else {
				lineChan <- strings.TrimSpace(line)
			}
		}(reader)
		select {
		case <-ds.messageScanner.ctx.Done():
			ds.pool.Put(conn)
			return
		case line := <-lineChan:
			if line == "." || line == "" {
				ds.pool.Put(conn)
				return
			}
			if strings.Contains(strings.ToLower(line), strings.ToLower(ds.messageScanner.header)) {
				ds.messageScanner.linesScannerChannel <- line
			} else {
				ds.messageScanner.messagesRead.Add(1)
				go ds.messageScanner.iterationFunc()
			}
		case <-time.After(time.Duration(ds.config.OverviewTimeout) * time.Second):
			conn.Close()
			ds.pool.Put(conn)
			ds.log(fmt.Sprintf("Overview reader timeout at line %d for range %d - %d", i, first, last))
			ds.maybeRestartOverviewScanner(i, first, last, restart)
			return
		}
	}
}

// maybeRestartOverviewScanner restarts the overview reader after a timeout.
func (ds *DirectSearch) maybeRestartOverviewScanner(lineNumber, first, last, restart uint) {
	if first+lineNumber-1 > last {
		return
	}
	if lineNumber != 1 {
		restart = 0
	} else {
		restart++
	}
	select {
	case <-ds.messageScanner.ctx.Done():
		return
	case ds.messageScanner.overviewScannerLimiter <- struct{}{}:
		f := first + lineNumber - 1
		l := last
		r := restart
		ds.log(fmt.Sprintf("Restarting overview reader for range %d - %d", f, l))
		ds.messageScanner.overviewScannerWG.Go(func() {
			defer func() { <-ds.messageScanner.overviewScannerLimiter }()
			ds.overviewScanner(f, l, r)
		})
	}

}

// lineScanner parses overview lines into NZB structures.
func (ds *DirectSearch) lineScanner() {
	for {
		select {
		case <-ds.messageScanner.ctx.Done():
			return
		case line, ok := <-ds.messageScanner.linesScannerChannel:
			if !ok {
				return
			}
			overview, err := nntp.ParseOverviewLine(line)
			if err != nil {
				ds.log(fmt.Sprintf("Failed to parse message line \"%s\": %v", line, err))
				continue
			}
			subject := html.UnescapeString(strings.ToValidUTF8(overview.Subject, ""))
			if strings.Contains(strings.ToLower(subject), strings.ToLower(ds.messageScanner.header)) {
				if subject, err := subjectparser.Parse(subject); err == nil {
					var date int64
					if date = overview.Date.Unix(); date < 0 {
						date = 0
					}
					poster := strings.ToValidUTF8(overview.From, "")
					// make hashes
					headerHash := GetMD5Hash(subject.Header + poster + strconv.Itoa(subject.TotalFiles))
					fileHash := GetMD5Hash(headerHash + subject.Filename + strconv.Itoa(subject.File) + strconv.Itoa(subject.TotalSegments))
					ds.messageScanner.resultsMutex.Lock()
					if _, ok := ds.messageScanner.results[headerHash]; !ok {
						ds.messageScanner.results[headerHash] = make(map[string]nzbparser.NzbFile)
					}
					if _, ok := ds.messageScanner.results[headerHash][fileHash]; !ok {
						ds.messageScanner.results[headerHash][fileHash] = nzbparser.NzbFile{
							Groups:       []string{ds.group},
							Subject:      subject.Subject,
							Poster:       poster,
							Number:       subject.File,
							Filename:     subject.Filename,
							Basefilename: subject.Basefilename,
						}
					}
					file := ds.messageScanner.results[headerHash][fileHash]
					if file.Groups[len(file.Groups)-1] != ds.group {
						file.Groups = append(file.Groups, html.EscapeString(ds.group))
					}
					if subject.Segment == 1 {
						file.Subject = subject.Subject
					}
					if int(date) > file.Date {
						file.Date = int(date)
					}
					file.Segments = append(file.Segments, nzbparser.NzbSegment{
						Number: subject.Segment,
						Id:     html.EscapeString(strings.Trim(overview.MessageId, "<>")),
						Bytes:  overview.Bytes,
					})
					ds.messageScanner.results[headerHash][fileHash] = file
					ds.messageScanner.resultsMutex.Unlock()
				}
			}
			ds.messageScanner.messagesRead.Add(1)
			go ds.messageScanner.iterationFunc()
		}
	}
}

// messageScannerErrorHandler captures the first scanner error.
func (ds *DirectSearch) messageScannerErrorHandler() {
	for {
		select {
		case <-ds.messageScanner.ctx.Done():
			return
		case err, ok := <-ds.messageScanner.errorChannel:
			if !ok {
				return
			}
			ds.messageScanner.lastError.Store(err)
			//ds.messageScanner.ctxCancel()
			return
		}
	}
}
