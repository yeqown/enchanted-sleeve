package esl

import (
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func Test_defaultOptions(t *testing.T) {
	defaultOpt := defaultOptions()

	assert.NotNil(t, defaultOpt)
	assert.Equal(t, defaultOpt.maxFileBytes, maxDataFileSize)
	assert.Equal(t, defaultOpt.maxKeyBytes, maxKeySize)
	assert.Equal(t, defaultOpt.maxValueBytes, maxValueSize)
	assert.Equal(t, defaultOpt.compactThreshold, uint32(10))
	assert.Equal(t, defaultOpt.compactInterval, time.Minute)
}

func Test_WithMaxFileBytes(t *testing.T) {
	opt := defaultOptions()
	WithMaxFileBytes(100).apply(opt)

	assert.Equal(t, opt.maxFileBytes, uint32(100))
}

func Test_WithMaxKeyBytes(t *testing.T) {
	opt := defaultOptions()
	WithMaxKeyBytes(100).apply(opt)

	assert.Equal(t, opt.maxKeyBytes, uint16(100))
}

func Test_WithMaxValueBytes(t *testing.T) {
	opt := defaultOptions()
	WithMaxValueBytes(100).apply(opt)

	assert.Equal(t, opt.maxValueBytes, uint16(100))
}

func Test_WithCompactThreshold(t *testing.T) {
	opt := defaultOptions()
	WithCompactThreshold(100).apply(opt)

	assert.Equal(t, opt.compactThreshold, uint32(100))
}

func Test_WithCompactInterval(t *testing.T) {
	opt := defaultOptions()
	WithCompactInterval(100).apply(opt)

	assert.EqualValues(t, opt.compactInterval, 100)
}

func Test_newFuncOption(t *testing.T) {
	opt := newFuncOption(func(o *options) {
		o.maxFileBytes = 100
	})

	assert.NotNil(t, opt)
	assert.NotNil(t, opt.fn)
}

func Test_WithFileSystem(t *testing.T) {
	opt := defaultOptions()
	WithFileSystem(nil).apply(opt)

	assert.Nil(t, opt.fs)

	WithFileSystem(afero.NewMemMapFs()).apply(opt)
	assert.NotNil(t, opt.fs)
}
