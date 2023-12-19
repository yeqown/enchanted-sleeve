package esl

import (
	"github.com/spf13/afero"
)

// FileSystem is the interface that wraps the basic methods for a file
// system, it is used by the file system abstraction layer to access, so that
// the default os file system can be replaced by other implementations.
//
// It's useful for testing, since it can be replaced by a mock file system.
type FileSystem = afero.Fs
