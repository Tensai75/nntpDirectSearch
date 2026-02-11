package nntpDirectSearch

import (
	"context"
	"fmt"

	"github.com/Tensai75/nntpPool"
)

// DirectSearch manages NNTP group selection, boundary scanning, and message scanning
// using a shared connection pool and cancellable context.
//
// It holds runtime state for the current group and exposes helpers for progress
// reporting and metrics (lines and bytes read).
type DirectSearch struct {
	Log                            chan string // Buffered channel for logging debug messages from internal operations.
	MaxBoundariesScannerIterations uint        // Calculated maximum iterations for the boundaries scanner based on group size and step.
	ctx                            context.Context
	ctxCancel                      context.CancelFunc
	pool                           nntpPool.ConnectionPool
	config                         DirectSearchConfig
	group                          string
	groupFirstArticle              uint
	groupLastArticle               uint
	boundariesScanner              *boundariesScanner
	messageScanner                 *messageScanner
}

// DirectSearchConfig holds tunable settings for scanning behavior and retry
// strategies used by DirectSearch.
type DirectSearchConfig struct {
	Connections     uint // Number of concurrent connections to use for scanning.
	Step            uint // Step size for scanning message ranges.
	OverviewTimeout uint // Timeout in seconds for overview requests.
	OverviewRetries uint // Number of retries for overview requests.
}

var (
	// ErrPoolIsNil is returned when a nil connection pool is provided to New.
	ErrPoolIsNil = fmt.Errorf("nntpPool cannot be nil")
	// ErrGroupHasNoArticles indicates the selected group has no usable article range.
	ErrGroupHasNoArticles = fmt.Errorf("selected group has no articles")
	// ErrNoGroupSelected indicates a scan was attempted without selecting a group.
	ErrNoGroupSelected = fmt.Errorf("no group selected")
	// ErrConnectionsMustBeGreaterThanZero indicates an invalid connection count in config.
	ErrConnectionsMustBeGreaterThanZero = fmt.Errorf("connections must be greater than zero")
	// ErrStepMustBeGreaterThanZero indicates an invalid step size in config.
	ErrStepMustBeGreaterThanZero = fmt.Errorf("step must be greater than zero")
	// ErrOverviewTimeoutMustBeGreaterThanZero indicates an invalid overview timeout in config.
	ErrOverviewTimeoutMustBeGreaterThanZero = fmt.Errorf("overview timeout must be greater than zero")
	// ErrOverviewRetriesMustBeGreaterThanZero indicates an invalid overview retry count in config.
	ErrOverviewRetriesMustBeGreaterThanZero = fmt.Errorf("overview retries must be greater than zero")
)

// New creates a DirectSearch instance using the provided connection pool and
// context. If ctx is nil, context.Background is used.
// The pool is validated by acquiring and returning a test connection.
// It returns an error if the pool is nil or if there is an issue acquiring a connection.
func New(pool nntpPool.ConnectionPool, ctx context.Context) (*DirectSearch, error) {
	if pool == nil {
		return nil, ErrPoolIsNil
	}
	conn, err := pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	pool.Put(conn)
	if ctx == nil {
		ctx = context.Background()
	}
	directsearchCtx, directsearchCtxCancel := context.WithCancel(ctx)
	return &DirectSearch{
		Log:       make(chan string, 100),
		pool:      pool,
		ctx:       directsearchCtx,
		ctxCancel: directsearchCtxCancel,
		config: DirectSearchConfig{
			Connections:     20,
			Step:            20000,
			OverviewTimeout: 5,
			OverviewRetries: 3,
		},
	}, nil
}

// SetConfig validates and applies the DirectSearchConfig.
// It returns an error when any required field is zero.
func (ds *DirectSearch) SetConfig(config DirectSearchConfig) error {
	if config.Connections == 0 {
		return ErrConnectionsMustBeGreaterThanZero
	}
	if config.Step == 0 {
		return ErrStepMustBeGreaterThanZero
	}
	if config.OverviewTimeout == 0 {
		return ErrOverviewTimeoutMustBeGreaterThanZero
	}
	if config.OverviewRetries == 0 {
		return ErrOverviewRetriesMustBeGreaterThanZero
	}
	ds.config = config
	return nil
}

// GetConfig returns the current DirectSearchConfig.
func (ds *DirectSearch) GetConfig() DirectSearchConfig {
	return ds.config
}

// SwitchToGroup selects a new NNTP group and caches its first and last article
// numbers for subsequent scans. It also calculates the maximum iterations
// for the boundaries scanner based on the group size and configured step.
// It returns an error if the group has no articles or if there is an issue
// retrieving group information from the server.
func (ds *DirectSearch) SwitchToGroup(group string) error {
	conn, err := ds.pool.Get(ds.ctx)
	if err != nil {
		return err
	}
	defer ds.pool.Put(conn)

	_, first, last, err := conn.Group(group)
	if err != nil {
		return err
	}

	if first >= last {
		return ErrGroupHasNoArticles
	}

	ds.group = group
	ds.groupFirstArticle = uint(first)
	ds.groupLastArticle = uint(last)
	ds.MaxBoundariesScannerIterations = ds.calcMaxBoundariesScannerIterations()

	return nil
}

// GetLinesRead returns the total number of overview lines processed by the
// active message scanner.
func (ds *DirectSearch) GetLinesRead() uint64 {
	if ds.messageScanner == nil {
		return 0
	}
	return ds.messageScanner.messagesRead.Load()
}

// GetBytesRead returns the total number of bytes read by the active message
// scanner.
func (ds *DirectSearch) GetBytesRead() uint64 {
	if ds.messageScanner == nil {
		return 0
	}
	return ds.messageScanner.bytesRead.Load()
}
