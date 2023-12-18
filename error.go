package esl

import (
	"github.com/pkg/errors"
)

var (
	ErrKeyOrValueTooLong  = errors.New("key or value is oversize")
	ErrKeyNotFound        = errors.New("key not found")
	ErrInvalidEntryHeader = errors.New("invalid entry header")
	ErrEntryCorrupted     = errors.New("entry corrupted")

	ErrInvalidKeydirData     = errors.New("invalid keydir data")
	ErrInvalidKeydirFileData = errors.New("invalid keydir file data")
)
