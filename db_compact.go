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
	ticker := time.NewTicker(time.Second * 30)
	needCompact := func() bool {
		// check whether we need to startCompactRoutine the DB or not.
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

		if err := db.mergeFiles(); err != nil {
			fmt.Printf("mergeFiles failed: %v\n", err)
		}
	}
}

// mergeFiles merges the older closed datafiles into one or many merged files
// having the same structure as the existing datafiles.
// This way the unused and non-existent keys are ignored from the newer datafiles
// saving a bunch of disk space. Since the record now exists in a different merged datafile
// and at a new offset, its entry in KeyDir needs an atomic update.
//
// NOTE: mergeFiles is reading all immutable datafiles and writing to a new datafile,
// and it only keeps the "live" or the latest version of the key-value pairs.
func (db *DB) mergeFiles() error {
	pattern := filepath.Join(db.path, dataFilePattern)
	matched, err := afero.Glob(db.filesystem(), pattern)
	if err != nil {
		return err
	}

	orderedFileIds := make([]int, 0, len(matched))
	for _, filename := range matched {
		fileId, err := fileIdFromFilename(filename)
		if err != nil {
			return errors.Wrap(err, "takeDBPathSnap parse data file id")
		}
		orderedFileIds = append(orderedFileIds, int(fileId))
	}
	sort.Sort(sort.Reverse(sort.IntSlice(orderedFileIds)))

	// orderedFilenames := make([]string, 0, len(matched))
	// activeDataFilename := dataFilename(db.path, db.activeFileId)
	// for _, filename := range matched {
	// 	if strings.EqualFold(filename, activeDataFilename) {
	// 		println("skip active datafile")
	// 		continue
	// 	}
	// 	orderedFilenames = append(orderedFilenames, filename)
	// }
	// sort.Sort(sort.Reverse(sort.StringSlice(orderedFilenames)))

	tombstone := make(map[string]struct{}, 1024)
	alive := make(map[string]*kvEntry, 1024)

	// loop datafiles(from the newest to the oldest) to merge.
	for _, fileId := range orderedFileIds {
		if fileId == int(db.activeFileId) {
			continue
		}

		filename := dataFilename(db.path, uint16(fileId))
		kvs, _, err2 := readDataFile(db.filesystem(), filename, uint16(fileId))
		if err2 != nil {
			return errors.Wrap(err2, "readDataFile "+filename)
		}

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

	return db.writeMergeFileAndHint(db.activeFileId+1, alive)
}

func readDataFile(fs FileSystem, filename string, fileId uint16) ([]*kvEntry, map[string]*keydirMemEntry, error) {
	fd, err := fs.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		return nil, nil, err
	}

	// TODO: determine the size of datafile, so we can allocate a buffer to read all data
	//       from datafile at once.
	pos := int64(0)
	entries := make([]*kvEntry, 0, 1024)
	keydirs := make(map[string]*keydirMemEntry, 1024)
	header := make([]byte, kvEntry_fixedBytes)
	fi, err := fd.Stat()
	if err != nil {
		return nil, nil, err
	}

	for pos < fi.Size() {
		keydir := &keydirMemEntry{
			fileId:      fileId,
			valueSize:   0,           // set it later
			entryOffset: uint32(pos), //
			valueOffset: 0,           // set it later
		}

		// read fixed entry header.
		n, err2 := fd.ReadAt(header, pos)
		if err != nil || n != kvEntry_fixedBytes {
			return nil, nil, err2
		}

		entry, err3 := decodeEntryFromHeader(header)
		if err3 != nil {
			return nil, nil, err3
		}

		// read key.
		pos += kvEntry_fixedBytes
		n, err2 = fd.ReadAt(entry.key, pos)
		if err != nil || n != int(entry.keySize) {
			return nil, nil, err2
		}

		// read value.
		pos += int64(entry.keySize)
		keydir.valueOffset = uint32(pos)
		keydir.valueSize = entry.valueSize

		n, err2 = fd.ReadAt(entry.value, pos)
		if err != nil || n != int(entry.valueSize) {
			return nil, nil, err2
		}

		entries = append(entries, entry)
		keydirs[unsafe.String(&entry.key[0], int(entry.keySize))] = keydir

		// step to next entry.
		pos += int64(entry.valueSize)
	}

	return entries, keydirs, nil
}

// DONE: what if the datafile is too large to write into one file
//
//	over the maxDataFileSize(100MB)? split into another file?
func (db *DB) writeMergeFileAndHint(fileId uint16, aliveEntries map[string]*kvEntry) error {
	// open new datafile and hint file.
	open := func(fileId uint16) (dataFile, hintFile afero.File, fn func(), err error) {
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

		dataFName := dataFilename(db.path, fileId)
		if dataFile, err = db.filesystem().OpenFile(dataFName, os.O_CREATE|os.O_RDWR, 0666); err != nil {
			return nil, nil, nil, err
		}

		hintFName := hintFilename(db.path, fileId)
		if hintFile, err = db.filesystem().OpenFile(hintFName, os.O_CREATE|os.O_RDWR, 0666); err != nil {
			return nil, nil, nil, err
		}

		closeFn := func() {
			_ = dataFile.Close()
			_ = hintFile.Close()
		}

		return dataFile, hintFile, closeFn, nil

	}

	dataFile, hintFile, closeFn, err := open(fileId)
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
			// TODO: handle err
			panic(err)
		}
		valueOff = entryOff + kvEntry_fixedBytes + uint32(entry.keySize)

		keydir = &keydirFileEntry{
			keydirMemEntry: keydirMemEntry{
				fileId:      fileId,
				valueSize:   entry.valueSize,
				valueOffset: valueOff,
				entryOffset: entryOff,
			},
			keySize: entry.keySize,
			key:     entry.key,
		}
		if _, err = hintFile.Write(keydir.bytes()); err != nil {
			// TODO: handle err
			panic(err)
		}

		// open another file if the current file is too large (>= 100MB).
		if valueOff >= db.opt.maxFileBytes {
			closeFn()

			fileId++
			valueOff = 0
			entryOff = 0

			dataFile, hintFile, closeFn, err = open(fileId)
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
