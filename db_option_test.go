package esl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_defaultOptions(t *testing.T) {
	defaultOpt := defaultOptions()

	assert.NotNil(t, defaultOpt)
	assert.Equal(t, defaultOpt.maxFileBytes, maxDataFileSize)
	assert.Equal(t, defaultOpt.maxKeyBytes, maxKeySize)
	assert.Equal(t, defaultOpt.maxValueBytes, maxValueSize)
	assert.Equal(t, defaultOpt.compactThreshold, uint32(10))
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
