package esl

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

const (
	dataFileExt     = ".esld"
	dataFilePattern = "*" + dataFileExt
	hintFileExt     = ".hint"
	hintFilePattern = "*" + hintFileExt

	initDataFileId = uint16(1)
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
	opt *options

	// inArchived to protect DB operations while activeDataFile is archiving.
	inArchived atomic.Bool
	// TODO: use inArchivedCond to replace inArchived flag spin lock.
	// inArchivedCond sync.Cond

	// DONE: we need a sema to protect DB status field,
	// such as activeDataFile, activeDataFileOff, activeFileId, etc.
	activeLock        sync.RWMutex
	activeFileId      uint16
	activeDataFile    afero.File
	activeDataFileOff uint32

	// // The hint file for activeDataFile to store the keydir index of activeDataFile,
	// // so that we can quickly restore keyDir from the hint file while db restart or recover from crash.
	// activeHintFile afero.File
	// activeHintOff  uint32

	// path is the directory where the DB is stored.
	path string

	// keyDir is a key-value index for all key-value pairs.
	keyDir *keydirMemTable

	// inCompaction is a flag to indicate whether the DB is in compaction.
	inCompaction atomic.Bool
	// compactCommand is a channel to receive startCompactRoutine command.
	compactCommand chan struct{}
}

// Open create or restore from the path.
func Open(path string, options ...Option) (*DB, error) {
	dbOpts := defaultOptions()
	for _, opt := range options {
		opt.apply(dbOpts)
	}

	if err := ensurePath(dbOpts.fs, path); err != nil {
		return nil, errors.Wrap(err, "Open ensurePath failed")
	}

	snap, err := takeDBPathSnap(dbOpts.fs, path)
	if err != nil {
		return nil, errors.Wrap(err, "Open takeDBPathSnap")
	}

	return newDB(path, snap, dbOpts)
}

func newDB(path string, snap *dbPathSnap, opts *options) (*DB, error) {

	activeFileId := snap.lastDataFileId
	dataFile, dataFileOff, err := openDataFile(opts.fs, path, activeFileId)
	if err != nil {
		return nil, errors.Wrap(err, "openDataFile")
	}

	keyDir := newKeyDir()
	if !snap.isEmpty() {
		if err = restoreKeydirIndex(opts.fs, snap, keyDir); err != nil {
			return nil, errors.Wrap(err, "restoreKeydirIndex")
		}
	}

	db := &DB{
		opt:        opts,
		inArchived: atomic.Bool{},

		activeLock:        sync.RWMutex{},
		activeFileId:      activeFileId,
		activeDataFile:    dataFile,
		activeDataFileOff: dataFileOff,

		path: path,

		keyDir: keyDir,

		inCompaction:   atomic.Bool{},
		compactCommand: make(chan struct{}, 1),
	}

	db.inArchived.Store(false)
	db.inCompaction.Store(false)

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

	// if db.activeHintFile != nil {
	// 	if err := db.activeHintFile.Sync(); err != nil {
	// 		return errors.Wrap(err, "could not sync hint file")
	// 	}
	//
	// 	if err := db.activeHintFile.Close(); err != nil {
	// 		return errors.Wrap(err, "could not close hint file")
	// 	}
	// }

	return nil
}

func (db *DB) filesystem() FileSystem {
	if db == nil || db.opt == nil || db.opt.fs == nil {
		return afero.NewOsFs()
	}

	return db.opt.fs
}

// openDataFile open a data file for writing. If the file is not exist, it
// creates a new active file with given fileId which should be formed as 10 digits,
// for example: 0000000001.esld
func openDataFile(fs FileSystem, path string, fileId uint16) (afero.File, uint32, error) {
	dataFName := dataFilename(path, fileId)

	dataFd, err := fs.OpenFile(dataFName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, 0, errors.Wrap(err, "open data file failed")
	}
	st, err := dataFd.Stat()
	if err != nil {
		_ = dataFd.Close()
		return nil, 0, errors.Wrap(err, "read file stat failed")
	}

	return dataFd, uint32(st.Size()), nil
}

// func openHintFile(fs FileSystem, path string, fileId uint16) (afero.File, uint32, error) {
// 	hintFName := hintFilename(path, fileId)
//
// 	hintFd, err := fs.OpenFile(hintFName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
// 	if err != nil {
// 		return nil, 0, errors.Wrap(err, "open hint file failed")
// 	}
//
// 	st, err := hintFd.Stat()
// 	if err != nil {
// 		_ = hintFd.Close()
// 		return nil, 0, errors.Wrap(err, "read file stat failed")
// 	}
//
// 	return hintFd, uint32(st.Size()), nil
// }

func (db *DB) archive() (err error) {
	if !db.inArchived.CompareAndSwap(false, true) {
		// has been in archiving, return.
		return
	}

	_ = db.activeDataFile.Sync()
	_ = db.activeDataFile.Close()
	db.activeDataFile = nil

	db.activeFileId++
	db.activeDataFile, db.activeDataFileOff, err = openDataFile(db.filesystem(), db.path, db.activeFileId)
	if err != nil {
		return errors.Wrap(err, "openDataFile failed")
	}

	db.inArchived.Store(false)
	return nil
}

func (db *DB) Put(key, value []byte) error {
	if len(key) > int(db.opt.maxKeyBytes) || len(value) > int(db.opt.maxValueBytes) {
		return ErrKeyOrValueTooLong
	}

	entry := newEntry(key, value)

	return db.write(key, entry)
}

// Delete removes the key from the DB. Note that the key is not actually removed from the DB,
// but marked as deleted, and the key will be removed from the DB when the DB is compacted.
func (db *DB) Delete(key []byte) error {
	if dir := db.keyDir.get(key); dir == nil || dir.valueSize == 0 {
		return nil
	}

	entry := newEntry(key, nil)

	return db.write(key, entry)
}

// write to activate file and update keyDir index.
// TODO: use channel to write to active file in sequence. also can set different channel for diff priority write.
func (db *DB) write(key []byte, e *kvEntry) error {
	for db.inArchived.Load() {
		// spin to wait for archiving finish
		time.Sleep(time.Millisecond)
	}

	// FIXED: maybe deadlock with keyDir.lock? no, since keyDir only called in write method and
	// restoreKeydirIndex method, and restoreKeydirIndex method is called in newDB method which
	// is called only once in Open method.
	db.activeLock.Lock()
	defer db.activeLock.Unlock()

	keydir := &keydirMemEntry{
		fileId:      db.activeFileId,
		valueSize:   e.valueSize,
		entryOffset: db.activeDataFileOff,
		valueOffset: db.activeDataFileOff + kvEntry_fixedBytes + uint32(e.keySize),
	}

	// fmt.Printf("entry(key=%s, value=%s) keydir: %+v\n", key, e.value, keydir)

	n, err := db.activeDataFile.Write(e.bytes())
	if err != nil {
		return errors.Wrap(err, "db.Put could not write to file")
	}

	db.keyDir.set(key, keydir)
	db.activeDataFileOff += uint32(n)

	if db.activeDataFileOff >= db.opt.maxFileBytes {
		if err = db.archive(); err != nil {
			return errors.Wrap(err, "db archive failed")
		}
	}

	return nil
}

func (db *DB) Get(key []byte) (value []byte, err error) {
	entry, err := db.get(key, true)
	if err != nil {
		return nil, err
	}

	return entry.value, nil
}

func (db *DB) get(key []byte, quick bool) (entry *kvEntry, err error) {
	for db.inCompaction.Load() {
		// spin to wait for compaction finish
		time.Sleep(time.Millisecond)
	}

	clue := db.keyDir.get(key)
	if clue == nil || clue.valueSize == 0 {
		return nil, ErrKeyNotFound
	}

	var fd afero.File
	if clue.fileId == db.activeFileId {
		fd = db.activeDataFile
	} else {
		fd, err = db.openInactiveFile(clue)
		if err != nil {
			return nil, errors.Wrap(err, "open inactive file failed")
		}
	}

	// FIXED: activeFile concurrent read/write may cause read entry incorrectly.
	db.activeLock.Lock()
	defer db.activeLock.Unlock()

	if quick {
		entry = new(kvEntry)
		entry.value = make([]byte, clue.valueSize)
		err = readValueOnly(fd, clue, entry.value)
	} else {
		entry, err = readEntryEntire(fd, clue)
	}
	if err != nil {
		return nil, errors.Wrap(err, "read entry failed")
	}

	// fmt.Printf("get key=%s, value=%s, clue: %+v\n", key, entry.value, clue)

	return entry, nil
}

func readEntryEntire(dataFile afero.File, clue *keydirMemEntry) (*kvEntry, error) {
	// TODO: use buffer pool to reduce memory allocation.
	header := make([]byte, kvEntry_fixedBytes)
	n, err := dataFile.ReadAt(header, int64(clue.entryOffset))
	if err != nil || n != kvEntry_fixedBytes {
		return nil, errors.Wrap(err, "read from dataFile failed")
	}

	entry, err := decodeEntryFromHeader(header)
	if err != nil {
		return nil, errors.Wrap(err, "decode entry from header failed")
	}

	// read key.
	n, err = dataFile.ReadAt(entry.key, int64(clue.entryOffset+kvEntry_fixedBytes))
	if err != nil || n != int(entry.keySize) {
		return nil, errors.Wrap(err, "read from dataFile failed")
	}

	// read value.
	n, err = dataFile.ReadAt(entry.value, int64(clue.entryOffset+kvEntry_fixedBytes+uint32(entry.keySize)))
	if err != nil || n != int(entry.valueSize) {
		return nil, errors.Wrap(err, "read from dataFile failed")
	}

	// DONE: check entry crc to ensure the entry is not corrupted.
	if entry.crc != checksum(entry) {
		return nil, ErrEntryCorrupted
	}

	return entry, nil
}

func readValueOnly(dataFile afero.File, clue *keydirMemEntry, value []byte) error {
	n, err := dataFile.ReadAt(value, int64(clue.valueOffset))
	if err != nil || n != int(clue.valueSize) {
		return errors.Wrap(err, "read from dataFile failed")
	}

	return nil
}

// openInactiveFile open inactive file for reading.
// TODO: add cache pool to reduce file open/close operations.
func (db *DB) openInactiveFile(clue *keydirMemEntry) (afero.File, error) {
	filename := dataFilename(db.path, clue.fileId)
	fd, err := db.filesystem().OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		return nil, errors.Wrap(err, "open file failed")
	}

	return fd, nil
}

type Key []byte

func (db *DB) ListKeys() []Key {
	keys := make([]Key, 0, len(db.keyDir.indexes))
	for key, keydir := range db.keyDir.indexes {
		if keydir.valueSize == 0 {
			continue
		}
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
	if db.inArchived.Load() {
		return
	}

	_ = db.activeDataFile.Sync()
}
