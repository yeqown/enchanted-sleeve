package esl

import (
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
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
	opt *options

	// inArchived to protect DB operations while activeDataFile is archiving.
	inArchived atomic.Bool

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

func openHintFile(fs FileSystem, path string, fileId uint16) (afero.File, uint32, error) {
	hintFName := hintFilename(path, fileId)

	hintFd, err := fs.OpenFile(hintFName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, 0, errors.Wrap(err, "open hint file failed")
	}

	st, err := hintFd.Stat()
	if err != nil {
		_ = hintFd.Close()
		return nil, 0, errors.Wrap(err, "read file stat failed")
	}

	return hintFd, uint32(st.Size()), nil
}

// restoreKeydirIndex restore keyDir from index file. First of all, the restore process
// will try scan all hint files and merge them into a single keyDir. But if the
// hint files are not found, the restore process will scan all data files and
// merge them into a single keyDir.
func restoreKeydirIndex(fs FileSystem, snap *dbPathSnap, keyDir *keydirMemTable) error {
	hintFileIds := make(map[uint16]struct{}, len(snap.hintFiles))
	if len(snap.hintFiles) != 0 {
		for _, hintFile := range snap.hintFiles {
			fileId, err := fileIdFromFilename(hintFile)
			if err != nil {
				// skip invalid hint file
				continue
			}
			hintFileIds[fileId] = struct{}{}

			keydirs, err := readHintFile(fs, hintFile)
			if err != nil {
				return errors.Wrap(err, "read hint file failed")
			}

			for _, keydir := range keydirs {
				keyDir.set(keydir.key, &keydir.keydirMemEntry)
			}
		}
		return nil
	}

	if len(snap.dataFiles) != 0 {
		for _, filename := range snap.dataFiles {
			fileId, err := fileIdFromFilename(filename)
			if err != nil {
				println("could not parse data file, ", err.Error())
				continue
			}
			// Range data files and merge them into keyDir. if the data file has related hint file,
			// we can skip the data file.
			if _, exists := hintFileIds[fileId]; exists {
				continue
			}

			kvs, keydirs, err := readDataFile(fs, filename, fileId)
			if err != nil {
				return errors.Wrap(err, "readDataFile "+filename)
			}

			// FIXED: keydirMemEntry should be created while reading data file,
			//  calculate from the offset is not precise and safe.
			off := uint32(0)
			for _, kv := range kvs {
				keyDir.set(kv.key, keydirs[unsafe.String(&kv.key[0], len(kv.key))])
				off += kvEntry_fixedBytes + uint32(kv.keySize) + uint32(kv.valueSize)
			}
		}

		return nil
	}

	return nil
}

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
	keydir := db.buildKeyDir(entry)

	return db.write(key, entry, keydir)
}

func (db *DB) buildKeyDir(e *kvEntry) *keydirMemEntry {
	if e == nil {
		return nil
	}

	if db.inArchived.Load() {
		// spin wait for archive finish
		time.Sleep(time.Millisecond)
	}

	db.activeLock.RLock()
	activeFileId := db.activeFileId
	activeDataFileOff := db.activeDataFileOff
	db.activeLock.RUnlock()

	return &keydirMemEntry{
		fileId:      activeFileId,
		valueSize:   e.valueSize,
		entryOffset: activeDataFileOff,
		valueOffset: activeDataFileOff + kvEntry_keyOff + uint32(e.keySize),
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
func (db *DB) write(key []byte, e *kvEntry, keydir *keydirMemEntry) error {
	for db.inArchived.Load() {
		// spin to wait for archiving finish
		time.Sleep(time.Millisecond)
	}

	// FIXME: maybe deadlock with keyDir.lock?
	db.activeLock.Lock()
	defer db.activeLock.Unlock()

	n, err := db.activeDataFile.Write(e.bytes())
	if err != nil {
		return errors.Wrap(err, "db.Put could not write to file")
	}

	db.keyDir.set(key, keydir)        // update keyDir index.
	db.activeDataFileOff += uint32(n) // step active file offset

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
	clue := db.keyDir.get(key)
	if clue == nil {
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

	if quick {
		entry = new(kvEntry)
		entry.value, err = readValueOnly(fd, clue)
	} else {
		entry, err = readEntryEntire(fd, clue)
	}
	if err != nil {
		return nil, errors.Wrap(err, "read entry failed")
	}

	return entry, nil
}

func readEntryEntire(file afero.File, clue *keydirMemEntry) (*kvEntry, error) {
	// TODO: use buffer pool to reduce memory allocation.
	header := make([]byte, kvEntry_fixedBytes)
	n, err := file.ReadAt(header, int64(clue.entryOffset))
	if err != nil || n != kvEntry_fixedBytes {
		return nil, errors.Wrap(err, "read from file failed")
	}

	entry, err := decodeEntryFromHeader(header)
	if err != nil {
		return nil, errors.Wrap(err, "decode entry from header failed")
	}

	// read key.
	n, err = file.ReadAt(entry.key, int64(clue.entryOffset+kvEntry_fixedBytes))
	if err != nil || n != int(entry.keySize) {
		return nil, errors.Wrap(err, "read from file failed")
	}

	// read value.
	n, err = file.ReadAt(entry.value, int64(clue.entryOffset+kvEntry_fixedBytes+uint32(entry.keySize)))
	if err != nil || n != int(entry.valueSize) {
		return nil, errors.Wrap(err, "read from file failed")
	}

	// DONE: check entry crc to ensure the entry is not corrupted.
	if entry.crc != checksum(entry) {
		return nil, ErrEntryCorrupted
	}

	return entry, nil
}

func readValueOnly(file afero.File, clue *keydirMemEntry) ([]byte, error) {
	value := make([]byte, clue.valueSize)
	n, err := file.ReadAt(value, int64(clue.valueOffset))
	if err != nil || n != int(clue.valueSize) {
		return nil, errors.Wrap(err, "read from file failed")
	}

	return value, nil
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
	for key := range db.keyDir.indexes {
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
