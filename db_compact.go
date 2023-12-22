package esl

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

// startCompactRoutine is a routine to compacts the older closed datafiles into one or
// many merged files having the same structure as the existing datafiles.
func (db *DB) startCompactRoutine() {
	ticker := time.NewTicker(db.opt.compactInterval)
	needCompact := func() bool {
		snap, er := takeDBPathSnap(db.filesystem(), db.path)
		if er != nil {
			fmt.Printf("takeDBPathSnap failed: %v\n", er)
			return false
		}

		// immutable data files are more than compactThreshold, then we need to compact.
		if (len(snap.dataFiles) - 1) >= int(db.opt.compactThreshold) {
			return true
		}

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

		if err := db.merge(); err != nil {
			fmt.Printf("merge failed: %v\n", err)
		}
	}
}

// merge merges prepared datafiles into one or many merged files.
// NOTE: if merge process is running, we should disable the operations
// those are reading data, especially reading immutable data files.
func (db *DB) merge() error {
	if !db.inCompaction.CompareAndSwap(false, true) {
		return nil
	}
	defer func() {
		db.inCompaction.Store(false)
	}()

	oversize := func(off uint32) bool {
		return off >= db.opt.maxFileBytes
	}

	return mergeFiles(db.filesystem(), db.path, db.activeFileId, oversize)
}

// mergeFiles merges the older closed datafiles into one or many merged files
// having the same structure as the existing datafiles.
// This way the unused and non-existent keys are ignored from the newer datafiles
// saving a bunch of disk space. Since the record now exists in a different merged datafile
// and at a new offset, its entry in KeyDir needs an atomic update.
//
// NOTE: mergeFiles is reading all immutable datafiles and writing to a new datafile,
// and it only keeps the "live" or the latest version of the key-value pairs.
func mergeFiles(fs FileSystem, path string, activeFileId uint16, oversize oversizeFunc) error {
	pattern := filepath.Join(path, dataFilePattern)
	matched, err := afero.Glob(fs, pattern)
	if err != nil {
		return err
	}

	orderedFileIds := make([]int, 0, len(matched))
	for _, filename := range matched {
		fileId, err := fileIdFromFilename(filename)
		if err != nil {
			return errors.Wrap(err, "fileIdFromFilename parse data file id")
		}
		orderedFileIds = append(orderedFileIds, int(fileId))
	}
	sort.Sort(sort.Reverse(sort.IntSlice(orderedFileIds)))

	tombstone := make(map[string]struct{}, 1024)
	alive := make(map[string]*kvEntry, 1024)

	// trim the oldest datafile, since it's normally the active datafile.
	fileId := orderedFileIds[0]
	if uint16(fileId) == activeFileId {
		orderedFileIds = orderedFileIds[1:]
	}

	restoreFns := make([]func() error, 0, len(orderedFileIds))
	cleanFns := make([]func() error, 0, len(orderedFileIds))

	// loop datafiles(from the newest to the oldest) to merge.
	for _, fileId = range orderedFileIds {
		filename := dataFilename(path, uint16(fileId))
		kvs, _, err2 := readDataFile(fs, filename, uint16(fileId))
		if err2 != nil {
			return errors.Wrap(err2, "readDataFile "+filename)
		}

		// backup datafile
		restoreFn, cleanFn, err := backupFile(fs, filename)
		if err != nil {
			return errors.Wrap(err, "backupFile "+filename)
		}
		restoreFns = append(restoreFns, restoreFn)
		cleanFns = append(cleanFns, cleanFn)

		for _, kv := range kvs {
			key := unsafe.String(&kv.key[0], int(kv.keySize))
			if _, ignored := tombstone[key]; ignored {
				continue
			}
			if _, exists := alive[key]; exists {
				continue
			}

			if kv.tombstone() {
				tombstone[key] = struct{}{}
			}

			alive[key] = kv
		}
	}

	err = writeMergeFileAndHint(fs, path, activeFileId-1, alive, oversize)
	if err == nil {
		// if merge success and remove all backup datafiles.
		for _, cleanFn := range cleanFns {
			_ = cleanFn()
		}
		return nil
	}

	// if merge failed, restore all backup datafiles.
	for _, restoreFn := range restoreFns {
		_ = restoreFn()
	}

	return err
}

type oversizeFunc func(off uint32) bool

// writeMergeFileAndHint writes the merged datafile and hint file.
// The merged datafile and hint file will be named as the maxFileId, and if the merged
// datafile is too large, it will be split into multiple datafiles. The next fileId
// will be the maxFileId - 1.
// The aliveEntries is a map of key-value pairs that are alive or the latest version
// of the key-value pairs.
// oversize is a function to determine whether the datafile is too large.
//
// TODO: what if the maxFileId is too less which cause the datafile id reverse overflow?
// or we don't split even if the datafile is too large?
func writeMergeFileAndHint(
	fs FileSystem, path string, maxFileId uint16, aliveEntries map[string]*kvEntry, oversize oversizeFunc) (err error) {

	var fileIds = make([]uint16, 0, 8)
	// if any error occurs, we should clean up the datafile and hint file.
	defer func() {
		if err == nil {
			return
		}

		for _, fileId := range fileIds {
			_ = fs.Remove(dataFilename(path, fileId))
			_ = fs.Remove(hintFilename(path, fileId))
		}
	}()

	open := func(fileId uint16) (dataFile, hintFile afero.File, closeFn func(), err error) {
		fileIds = append(fileIds, fileId)

		defer func() {
			if err != nil {
				if dataFile != nil {
					_ = dataFile.Close()
				}
				if hintFile != nil {
					_ = hintFile.Close()
				}
			}
		}()

		dataFName := dataFilename(path, fileId)
		if dataFile, err = fs.OpenFile(dataFName, os.O_CREATE|os.O_RDWR, 0666); err != nil {
			return nil, nil, nil, err
		}
		hintFName := hintFilename(path, fileId)
		if hintFile, err = fs.OpenFile(hintFName, os.O_CREATE|os.O_RDWR, 0666); err != nil {
			return nil, nil, nil, err
		}

		closeFn = func() {
			_ = dataFile.Close()
			_ = hintFile.Close()
		}

		return dataFile, hintFile, closeFn, nil
	}

	dataFile, hintFile, closeFn, err := open(maxFileId)
	if err != nil {
		return err
	}

	valueOff := uint32(0)
	entryOff := uint32(0)
	var (
		keydir *keydirFileEntry
		n      int
	)
	for _, entry := range aliveEntries {
		if n, err = dataFile.Write(entry.bytes()); err != nil {
			return errors.Wrap(err, "writeMergeFileAndHint.writeDataFile")
		}
		valueOff = entryOff + kvEntry_fixedBytes + uint32(entry.keySize)

		keydir = &keydirFileEntry{
			keydirMemEntry: keydirMemEntry{
				fileId:      maxFileId,
				valueSize:   entry.valueSize,
				valueOffset: valueOff,
				entryOffset: entryOff,
			},
			keySize: entry.keySize,
			key:     entry.key,
		}
		if _, err = hintFile.Write(keydir.bytes()); err != nil {
			return errors.Wrap(err, "writeMergeFileAndHint.writeHintFile")
		}

		// open another file if the current file is too large (>= 100MB).
		if oversize(valueOff) {
			closeFn()

			maxFileId--
			valueOff = 0
			entryOff = 0

			dataFile, hintFile, closeFn, err = open(maxFileId)
			if err != nil {
				return err
			}
			continue
		}

		entryOff += uint32(n)
	}

	closeFn()

	return nil
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

func readDataFile(fs FileSystem, filename string, fileId uint16) ([]*kvEntry, map[string]*keydirMemEntry, error) {
	fd, err := fs.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		return nil, nil, err
	}

	// DONE: determine the size of datafile, so we can allocate a buffer to read all data
	//       from datafile at once.
	fi, err := fd.Stat()
	if err != nil {
		return nil, nil, err
	}

	total := fi.Size()
	cur := int64(0)
	n := estimateEntry(total) // estimate the number of entries.

	entries := make([]*kvEntry, 0, n)
	keydires := make(map[string]*keydirMemEntry, n)
	header := make([]byte, kvEntry_fixedBytes)

	for cur < total {
		keydir := &keydirMemEntry{
			fileId:      fileId,
			valueSize:   0,           // set it later
			entryOffset: uint32(cur), //
			valueOffset: 0,           // set it later
		}

		// read fixed entry header.
		n, err2 := fd.ReadAt(header, cur)
		if err != nil || n != kvEntry_fixedBytes {
			return nil, nil, err2
		}

		entry, err3 := decodeEntryFromHeader(header)
		if err3 != nil {
			return nil, nil, err3
		}

		// read key.
		cur += kvEntry_fixedBytes
		n, err2 = fd.ReadAt(entry.key, cur)
		if err != nil || n != int(entry.keySize) {
			return nil, nil, err2
		}

		// read value.
		cur += int64(entry.keySize)
		keydir.valueOffset = uint32(cur)
		keydir.valueSize = entry.valueSize

		n, err2 = fd.ReadAt(entry.value, cur)
		if err != nil || n != int(entry.valueSize) {
			return nil, nil, err2
		}

		if crc := checksum(entry); crc != entry.crc {
			return nil, nil, ErrEntryCorrupted
		}

		entries = append(entries, entry)
		keydires[unsafe.String(&entry.key[0], int(entry.keySize))] = keydir

		// step to next entry.
		cur += int64(entry.valueSize)
	}

	return entries, keydires, nil
}

func readHintFile(fs FileSystem, filename string) ([]*keydirFileEntry, error) {
	fd, err := fs.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	keydirFileEntries := make([]*keydirFileEntry, 0, 1024)
	pos := int64(0)
	header := make([]byte, keydirFile_fixedSize)
	fi, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	for pos < fi.Size() {
		// read fixed keydir header.
		n, err2 := fd.ReadAt(header, pos)
		if err != nil || n != keydirFile_fixedSize {
			return nil, err2
		}

		keydir, err3 := decodeKeydirFileEntry(header)
		if err3 != nil {
			return nil, err3
		}

		// read key.
		pos += keydirFile_fixedSize
		n, err2 = fd.ReadAt(keydir.key, pos)
		if err != nil || n != int(keydir.keySize) {
			return nil, err2
		}

		keydirFileEntries = append(keydirFileEntries, keydir)

		// step to next keydir.
		pos += int64(keydir.keySize)
	}

	return keydirFileEntries, nil
}
