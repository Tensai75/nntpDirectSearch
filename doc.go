// Package nntpDirectSearch provides high-level NNTP group scanning utilities to
// locate message boundaries by date and to build NZB results from message
// overviews. It coordinates concurrent workers over a shared connection pool
// and exposes progress metrics for long-running scans.
package nntpDirectSearch
