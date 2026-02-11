package nntpDirectSearch

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"strconv"
	"strings"

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
	for searchSpace > boundariesScannerStep { // The window size
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
