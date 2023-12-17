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
	// inArchived to protect DB operations while activeDataFile is archiving.
	inArchived atomic.Bool

	// TODO: we need a sema to protect DB status field, such as activeDataFile, activeDataFileOff, activeFileId, etc.
	activeFileId uint16

	activeDataFile    *os.File
	activeDataFileOff uint32
	// The hint file for activeDataFile to store the keydir index of activeDataFile,
	// so that we can quickly restore keyDir from the hint file while db restart or recover from crash.
	activeHintFile *os.File
	activeHintOff  uint32

	// path is the directory where the DB is stored.
	path string

	// keyDir is a key-value index for all key-value pairs.
	keyDir *keyDirIndex

	// compactCommand is a channel to receive startCompactRoutine command.
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

	return newDB(path, init, uint16(activeFileId), options...)
}

func newDB(path string, init bool, activeFileId uint16, options ...Option) (*DB, error) {
	dataFile, dataFileOff, err := openDataFile(path, activeFileId)
	if err != nil {
		return nil, errors.New("openDataFile")
	}

	hintFile, hintFileOff, err2 := openHintFile(path, activeFileId)
	if err2 != nil {
		return nil, errors.New("openHintFile")
	}

	keyDir := newKeyDir()
	if !init {
		activeFileId -= 1
		if err = restoreKeyDir(path, keyDir); err != nil {
			return nil, errors.Wrap(err, "restoreKeyDir")
		}
	}

	db := &DB{
		inArchived: atomic.Bool{},

		activeFileId:      activeFileId,
		activeDataFile:    dataFile,
		activeDataFileOff: dataFileOff,
		activeHintFile:    hintFile,
		activeHintOff:     hintFileOff,

		path: path,

		keyDir: keyDir,

		compactCommand: make(chan struct{}, 1),
	}

	go db.startCompactRoutine()

	return db, nil
}

func (db *DB) Close() error {
	if db.activeDataFile != nil {
		if err := db.activeDataFile.Sync(); err != nil {
			return errors.Wrap(err, "could not sync file")
		}

		if err := db.activeDataFile.Close(); err != nil {
			return errors.Wrap(err, "could not close file")
		}
	}

	if db.activeHintFile != nil {
		if err := db.activeHintFile.Sync(); err != nil {
			return errors.Wrap(err, "could not sync hint file")
		}

		if err := db.activeHintFile.Close(); err != nil {
			return errors.Wrap(err, "could not close hint file")
		}
	}

	return nil
}

// openDataFile open a data file for writing. If the file is not exist, it
// creates a new active file with given fileId which should be formed as 10 digits,
// for example: 0000000001.esld
func openDataFile(path string, fileId uint16) (*os.File, uint32, error) {
	dataFName := dataFilename(path, fileId)

	dataFd, err := os.OpenFile(dataFName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	st, err := dataFd.Stat()
	if err != nil {
		return nil, 0, errors.Wrap(err, "read file stat failed")
	}

	return dataFd, uint32(st.Size()), nil
}

func openHintFile(path string, fileId uint16) (*os.File, uint32, error) {
	hintFName := hintFilename(path, fileId)

	hintFd, err := os.OpenFile(hintFName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, 0, errors.Wrap(err, "open hint file failed")
	}

	st, err := hintFd.Stat()
	if err != nil {
		return nil, 0, errors.Wrap(err, "read file stat failed")
	}

	return hintFd, uint32(st.Size()), nil
}

// restoreKeyDir restore keyDir from index file. First of all, the restore process
// will try scan all hint files and merge them into a single keyDir. But if the
// hint files are not found, the restore process will scan all data files and
// merge them into a single keyDir.
func restoreKeyDir(path string, keyDir *keyDirIndex) error {
	pattern := filepath.Join(path, hintFilePattern)
	hintFiles, err := filepath.Glob(pattern)
	if err != nil {
		return errors.Wrap(err, "failed locate hint files")
	}
	if len(hintFiles) != 0 {
		// TODO: restore from hint files.
		println("restore from hint files")
		return nil
	}

	pattern = filepath.Join(path, dataFilePattern)
	dataFiles, err := filepath.Glob(pattern)
	if err != nil {
		return errors.Wrap(err, "failed locate data files")
	}
	if len(dataFiles) == 0 {
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

	_ = db.activeDataFile.Sync()
	_ = db.activeDataFile.Close()
	db.activeDataFile = nil

	_ = db.activeHintFile.Sync()
	_ = db.activeHintFile.Close()
	db.activeHintFile = nil

	db.activeFileId++
	db.activeDataFile, db.activeDataFileOff, err = openDataFile(db.path, db.activeFileId)
	if err != nil {
		return errors.Wrap(err, "openDataFile failed")
	}
	db.activeHintFile, db.activeHintOff, err = openHintFile(db.path, db.activeFileId)
	if err != nil {
		return errors.Wrap(err, "openHintFile failed")
	}

	db.inArchived.Store(false)
	return nil
}

const (
	maxKeySize   = 1 << 9  // 512B
	maxValueSize = 1 << 16 // 64K

	maxDataFileSize = 100 * 1024 * 1024 // 100MB
)

func (db *DB) Put(key, value []byte) error {
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
		valueOffset: db.activeDataFileOff + uint32(kvEntry_bytes_keyOff+uint32(e.keySize)),
	}
}

// Delete removes the key from the DB. Note that the key is not actually removed from the DB,
// but marked as deleted, and the key will be removed from the DB when the DB is compacted.
func (db *DB) Delete(key []byte) error {
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

	n, err := db.activeDataFile.Write(e.bytes())
	if err != nil {
		return errors.Wrap(err, "db.Put could not write to file")
	}

	db.keyDir.set(key, keydir)        // update keyDir index.
	db.activeDataFileOff += uint32(n) // step active file offset

	if db.activeDataFileOff >= maxDataFileSize {
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
		_, err = db.activeDataFile.ReadAt(value, int64(clue.valueOffset))
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

	if _, err = fd.ReadAt(value, int64(clue.valueOffset)); err != nil {
		return errors.Wrap(err, "read file failed")
	}

	return nil
}

type Key []byte

func (db *DB) ListKeys() []Key {
	keys := make([]Key, 0, len(db.keyDir.hashmap))
	for key := range db.keyDir.hashmap {
		keys = append(keys, Key(key))
	}

	return keys
}

// Merge compacts the DB which is used by developer to reduce disk usage manually.
func (db *DB) Merge() error {
	select {
	case db.compactCommand <- struct{}{}:
	default:
	}
	return nil
}

// Sync force any writes to sync to disk
func (db *DB) Sync() {
	// TODO:
}

func dataFilename(path string, fileId uint16) string {
	return fmt.Sprintf("%010d%s", fileId, dataFileExt)
}

func hintFilename(path string, fileId uint16) string {
	return fmt.Sprintf("%010d%s", fileId, hintFileExt)
}
