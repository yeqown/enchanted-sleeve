package esl

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/pkg/errors"
)

// DB is a simple key-value store backed by a log file, which is an append-only
// and immutable sequence of key-value pairs. The key-value pairs are stored in
// the log file in the order they are written. The log file is structured as
// follows:
//
// |  crc  |  tstamp  |  key_sz  |  value_sz  |  key  |  value  |
// |  crc  |  tstamp  |  key_sz  |  value_sz  |  key     |  value      |
//
// Since it's append-only, so modification and deletion would also append a new
// entry to overwrite old value.
type DB struct {
	// inArchived to protect DB operations while activeFile is archiving.
	inArchived    atomic.Bool
	activeFile    *os.File
	activeFileOff int64
	activeFileId  uint16

	keyDir *keyDirIndex
}

// Open create or restore from the path.
func Open(path string, options ...Option) (*DB, error) {

	init := false
	_, err := os.Stat(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		init = true
		if err = os.MkdirAll(path, 0666); err != nil {
			return nil, err
		}
	}

	// if the path is empty, init should be true too.
	pattern := filepath.Join(path, "*.esld")
	matched, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	activeFileId := len(matched)

	return newDB(path, init, uint16(activeFileId), options)
}

func newDB(path string, init bool, activeFileId uint16, options []Option) (*DB, error) {
	activeFile, activeFileOff, err := newActiveFile(activeFileId)
	if err != nil {
		return nil, errors.New("newActiveFile failed")
	}

	db := &DB{
		activeFile:    activeFile,
		activeFileOff: activeFileOff,
		activeFileId:  activeFileId, // activeFileId always be 0.

		keyDir: newKeyDir(),
	}

	return db, nil
}

func newActiveFile(fileId uint16) (*os.File, int64, error) {
	activeFilename := fmt.Sprintf("%10d.esld", fileId)

	fd, err := os.OpenFile(activeFilename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	st, err := fd.Stat()
	if err != nil {
		return nil, 0, errors.Wrap(err, "read file stat failed")
	}

	return fd, st.Size(), nil
}

func (db *DB) archive() (err error) {
	if !db.inArchived.CompareAndSwap(false, true) {
		return
	}

	_ = db.activeFile.Sync()
	db.activeFileId++
	if db.activeFile, db.activeFileOff, err = newActiveFile(db.activeFileId); err != nil {
		return errors.Wrap(err, "newActiveFile failed")
	}

	db.inArchived.Store(false)
	return nil
}

const (
	maxKeySize   = 1 << 9  // 512B
	maxValueSize = 1 << 16 // 64K

	maxDataFileSize = 100 * 1024 * 1024 // 100MB
)

func (db *DB) Set(key, value []byte) error {
	if len(key) > maxKeySize || len(value) > maxValueSize {
		return ErrKeyOrValueTooLong
	}

	for db.inArchived.Load() {
		// wait for archive finish
	}

	e := newEntry(key, value)

	// write to activate file
	n, err := db.activeFile.Write(e.bytes())
	if err != nil {
		return errors.Wrap(err, "db.Set could not write to file")
	}

	// and update keyDir index.
	db.keyDir.set(key, &keydirEntry{
		fileId:      db.activeFileId,
		valueSize:   e.valueSize,
		valuePos:    db.activeFileOff + int64(kvEntry_bytes_keyOff+int(e.keySize)),
		tsTimestamp: e.tsTimestamp,
	})

	// update active file offset
	db.activeFileOff += int64(n)

	if db.activeFileOff >= maxDataFileSize {
		if err = db.archive(); err != nil {
			return errors.Wrap(err, "db archive failed")
		}
	}

	return nil
}

func (db *DB) Get(key []byte) (value []byte, err error) {
	clue := db.keyDir.get(key)
	if clue == nil {
		return nil, ErrKeyNotFound
	}

	value = make([]byte, clue.valueSize)
	if clue.fileId == db.activeFileId {
		_, err = db.activeFile.ReadAt(value, clue.valuePos)
	} else {
		err = db.readInactiveFile(value, clue)
	}

	if err != nil {
		return nil, errors.Wrap(err, "read from file failed")
	}

	return value, nil
}

func (db *DB) readInactiveFile(value []byte, clue *keydirEntry) error {
	filename := fmt.Sprintf("%10d", clue.fileId)
	fd, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		return errors.Wrap(err, "open file failed")
	}

	if _, err = fd.ReadAt(value, clue.valuePos); err != nil {
		return errors.Wrap(err, "read file failed")
	}

	return nil
}

func (db *DB) Remove(key []byte) error {
	// TODO:
	return nil
}

func (db *DB) merge() error {
	// TODO:
	return nil
}

func (db *DB) Close() error {
	if err := db.activeFile.Sync(); err != nil {
		return errors.Wrap(err, "could not sync file")
	}

	if err := db.activeFile.Close(); err != nil {
		return errors.Wrap(err, "could not close file")
	}

	// TODO: more operations todo here.
	return nil
}
