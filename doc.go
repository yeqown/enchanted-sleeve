// Package wal implements a write-ahead log for storing data that needs to be
// persisted to disk. The WAL is append-only, and is safe for concurrent
// access. The WAL is optimized for writing, and is not optimized for reading.
//
// Similar to the https://github.com/tidwall/wal package, but with a few differences.
package wal

import (
	"errors"
)

var (
	// ErrSegmentNotFound is returned when a segment is not found.
	ErrSegmentNotFound = errors.New("segment not found")

	// ErrSegmentCorrupted is returned when a segment is corrupted.
	ErrSegmentCorrupted = errors.New("segment corrupted")

	// ErrSegmentInvalidOffset is returned when a segment is invalid offset.
	ErrSegmentInvalidOffset = errors.New("segment invalid offset")

	// ErrEntryNotFound is returned when a entry is not found.
	ErrEntryNotFound = errors.New("entry not found")

	// ErrSegmentFileMess is returned when a segment file could not be parsed successfully.
	ErrSegmentFileMess = errors.New("segment file mess")
)
