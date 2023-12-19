package esl

import (
	"time"

	"github.com/spf13/afero"
)

const (
	maxKeySize   = uint16(1) << 9  // 512B
	maxValueSize = uint16(1) << 15 // 64K

	maxDataFileSize = uint32(100 * 1024 * 1024) // 100MB
)

type options struct {
	// The maximum number of bytes for a single file. The default value is 100MB.
	// When the size of a file exceeds this value, a new file will be created.
	maxFileBytes uint32

	// The maximum number of bytes for a single key. The default value is 512B.
	maxKeyBytes uint16
	// The maximum number of bytes for a single value. The default value is 64KB.
	maxValueBytes uint16

	// The maximum number of files to keep. The default value is 10.
	// When the number of files exceeds this value, the compaction process will be triggered.
	compactThreshold uint32
	// The interval to check whether the compaction process should be triggered.
	// default value is 1 minute.
	compactInterval time.Duration

	// The file system to access. The default file system is implemented by os package.
	fs FileSystem
}

func defaultOptions() *options {
	return &options{
		maxFileBytes:     maxDataFileSize,
		maxKeyBytes:      maxKeySize,
		maxValueBytes:    maxValueSize,
		compactThreshold: 10,
		compactInterval:  time.Minute,
		fs:               afero.NewOsFs(),
	}
}

type Option interface {
	apply(*options)
}

type funcOption struct {
	fn func(*options)
}

func (funcOpt funcOption) apply(o *options) {
	funcOpt.fn(o)
}

func newFuncOption(fn func(*options)) *funcOption {
	return &funcOption{
		fn: fn,
	}
}

// WithMaxFileBytes set the maximum number of bytes for a single file.
func WithMaxFileBytes(maxFileBytes uint32) Option {
	return newFuncOption(func(o *options) {
		o.maxFileBytes = maxFileBytes
	})
}

// WithMaxKeyBytes set the maximum number of bytes for a single key.
func WithMaxKeyBytes(maxKeyBytes uint16) Option {
	return newFuncOption(func(o *options) {
		o.maxKeyBytes = maxKeyBytes
	})
}

// WithMaxValueBytes set the maximum number of bytes for a single value.
func WithMaxValueBytes(maxValueBytes uint16) Option {
	return newFuncOption(func(o *options) {
		o.maxValueBytes = maxValueBytes
	})
}

// WithCompactThreshold set the maximum number of files to keep.
func WithCompactThreshold(compactThreshold uint32) Option {
	return newFuncOption(func(o *options) {
		o.compactThreshold = compactThreshold
	})
}

// WithCompactInterval set the interval to check whether the compaction process should be triggered.
// NOTE: The interval is recommended to be greater than 1 minute. but it depends on the case
// of the application.
func WithCompactInterval(compactInterval time.Duration) Option {
	return newFuncOption(func(o *options) {
		o.compactInterval = compactInterval
	})
}

// WithFileSystem set the file system to access.
func WithFileSystem(fs FileSystem) Option {
	return newFuncOption(func(o *options) {
		o.fs = fs
	})
}
