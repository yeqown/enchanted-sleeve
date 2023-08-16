package esl

import "github.com/pkg/errors"

var (
	ErrKeyOrValueTooLong = errors.New("key or value is oversize")
	ErrKeyNotFound       = errors.New("key not found")
)
