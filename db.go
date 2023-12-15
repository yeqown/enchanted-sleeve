package esl

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

const (
	dataFileExt     = ".esld"
	dataFilePattern = "*" + dataFileExt
	hintFileExt     = ".hint"
	hintFilePattern = "*" + hintFileExt
)

// DB is a simple key-value store backed by a log file, which is an append-only
// and immutable sequence of key-value pairs. The key-value pairs are stored in
// the log file in the order they are written. The log file is structured as
// follows:
//
// |  crc  |  tstamp  |  key_sz  |  value_sz  |  key  |  value  |
// |  crc  |  tstamp  |  key_sz  |  value_sz  |  key  |  value  |
//
// Since it's append-only, so modification and deletion would also append a new
// entry to overwrite old value.
type DB struct {
	// inArchived to protect DB operations while activeFile is archiving.
	inArchived atomic.Bool

	// TODO: we need a sema to protect DB status field, such as activeFile, activeFileOff, activeFileId, etc.
	activeFile    *os.File
	activeFileOff int64
	activeFileId  uint16

	// path is the directory where the DB is stored.
	path string

	// keyDir is a key-value index for all key-value pairs.
	keyDir *keyDirIndex

	// compactCommand is a channel to receive compactRoutine command.
	compactCommand chan struct{}
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
	pattern := filepath.Join(path, dataFilePattern)
	matched, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	if len(matched) == 0 {
		init = true
	}
	activeFileId := len(matched)

	return newDB(path, init, uint16(activeFileId), options)
}

func newDB(path string, init bool, activeFileId uint16, options []Option) (*DB, error) {
	if init {
		activeFileId -= 1
	}

	activeFile, activeFileOff, err := openActiveFile(path, activeFileId)
	if err != nil {
		return nil, errors.New("openActiveFile failed")
	}

	keyDir := newKeyDir()
	if !init {
		if err = restoreKeyDir(path, keyDir); err != nil {
			return nil, errors.Wrap(err, "restoreKeyDir")
		}
	}

	db := &DB{
		inArchived: atomic.Bool{},

		// TODO: add a hint file for activeFile to store the keydir index of activeFile,
		//      so that we can quickly restore keyDir from the hint file while db restart or recover from crash.
		activeFile:    activeFile,
		activeFileOff: activeFileOff,
		activeFileId:  activeFileId, // activeFileId always be 0.

		path: path,

		keyDir: keyDir,
	}

	go db.compactRoutine()

	return db, nil
}

// openActiveFile create a new active file with given fileId which should be
// formed as 10 digits, for example: 0000000001.esld
func openActiveFile(path string, fileId uint16) (*os.File, int64, error) {
	activeFilename := fmt.Sprintf("%010d%s", fileId, dataFileExt)
	activeFilename = filepath.Join(path, activeFilename)

	fd, err := os.OpenFile(activeFilename, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	st, err := fd.Stat()
	if err != nil {
		return nil, 0, errors.Wrap(err, "read file stat failed")
	}

	return fd, st.Size(), nil
}

// restoreKeyDir restore keyDir from index file. First of all, the restore process
// will try scan all hint files and merge them into a single keyDir. But if the
// hint files are not found, the restore process will scan all data files and
// merge them into a single keyDir.
func restoreKeyDir(path string, keyDir *keyDirIndex) error {
	pattern := filepath.Join(path, hintFilePattern)
	matched, err := filepath.Glob(pattern)
	if err != nil {
		return errors.Wrap(err, "failed locate hint files")
	}
	if len(matched) != 0 {
		// TODO: restore from hint files.
		println("restore from hint files")
		return nil
	}

	dataFilePattern := filepath.Join(path, dataFilePattern)
	dataFileMatched, err := filepath.Glob(dataFilePattern)
	if err != nil {
		return errors.Wrap(err, "failed locate data files")
	}
	if len(dataFileMatched) == 0 {
		// no data files, no need to restore.
		return nil
	}
	// TODO: restore from data files.
	println("restore from data files")

	return nil
}

func (db *DB) archive() (err error) {
	if !db.inArchived.CompareAndSwap(false, true) {
		return
	}

	_ = db.activeFile.Sync()
	_ = db.activeFile.Close()
	db.activeFile = nil
	db.activeFileId++
	if db.activeFile, db.activeFileOff, err = openActiveFile(db.path, db.activeFileId); err != nil {
		return errors.Wrap(err, "openActiveFile failed")
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

	entry := newEntry(key, value)
	keydir := db.buildKeyDir(entry)

	return db.write(key, entry, keydir)
}

func (db *DB) buildKeyDir(e *kvEntry) *keydirEntry {
	if e == nil {
		return nil
	}

	if db.inArchived.Load() {
		// spin wait for archive finish
		time.Sleep(time.Millisecond)
	}

	return &keydirEntry{
		fileId:      db.activeFileId,
		valueSize:   e.valueSize,
		tsTimestamp: e.tsTimestamp,
		valuePos:    db.activeFileOff + int64(kvEntry_bytes_keyOff+int(e.keySize)),
	}
}

// Remove removes the key from the DB. Note that the key is not actually removed from the DB,
// but marked as deleted, and the key will be removed from the DB when the DB is compacted.
func (db *DB) Remove(key []byte) error {
	if dir := db.keyDir.get(key); dir != nil {
		return nil
	}

	entry := newEntry(key, nil)
	keydir := db.buildKeyDir(entry)

	return db.write(key, entry, keydir)
}

// write to activate file and update keyDir index.
func (db *DB) write(key []byte, e *kvEntry, keydir *keydirEntry) error {
	for db.inArchived.Load() {
		// spin to wait for archiving finish
		time.Sleep(time.Millisecond)
	}

	n, err := db.activeFile.Write(e.bytes())
	if err != nil {
		return errors.Wrap(err, "db.Set could not write to file")
	}

	db.keyDir.set(key, keydir)   // update keyDir index.
	db.activeFileOff += int64(n) // step active file offset

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

	// TODO: check crc32 checksum.

	return value, nil
}

// TODO: add cache pool to reduce file open/close operations.
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

// Compact compacts the DB which is used by developer to reduce disk usage manually.
func (db *DB) Compact() error {
	select {
	case db.compactCommand <- struct{}{}:
	default:
	}
	return nil
}

// compactRoutine is a routine to compacts the older closed datafiles into one or
// many merged files having the same structure as the existing datafiles.
// This way the unused and non-existent keys are ignored from the newer datafiles
// saving a bunch of disk space. Since the record now exists in a different merged datafile
// and at a new offset, its entry in KeyDir needs an atomic update.
func (db *DB) compactRoutine() {
	ticker := time.NewTicker(time.Second * 30)
	needCompact := func() bool {
		// check whether we need to compactRoutine the DB or not.
		// TODO: finish this admission check function.
		return false
	}

	// TODO: compact routine also need to start merge process while disk usage is over than
	//      the threshold that was configured by developer.
	for {
		select {
		case <-ticker.C:
			if needCompact() {
				select {
				case db.compactCommand <- struct{}{}:
				default:
				}
			}
			continue
		case <-db.compactCommand:
		}

		db.merge()
	}
}

func (db *DB) merge() {
	// TODO: achieve merge function.
}

func (db *DB) Close() error {
	if db.activeFile != nil {
		if err := db.activeFile.Sync(); err != nil {
			return errors.Wrap(err, "could not sync file")
		}

		if err := db.activeFile.Close(); err != nil {
			return errors.Wrap(err, "could not close file")
		}
	}

	// TODO: more operations todo here.
	return nil
}
