package esl

import (
	"fmt"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unsafe"
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
	matched, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	orderedFilenames := make([]string, 0, len(matched))
	activeDataFilename := dataFilename(db.path, db.activeFileId)
	for _, filename := range matched {
		if strings.EqualFold(filename, activeDataFilename) {
			println("skip active datafile")
			continue
		}
		orderedFilenames = append(orderedFilenames, filename)
	}

	sort.Sort(sort.Reverse(sort.StringSlice(orderedFilenames)))

	tombstone := make(map[string]struct{}, 1024)
	alive := make(map[string]*kvEntry, 1024)

	// loop datafiles(from the newest to the oldest) to merge.
	for _, filename := range orderedFilenames {
		kvs, err2 := readDataFile(filename)
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

	// mergedFilename := filepath.Join(db.path, fmt.Sprintf("%10d%s", db.activeFileId+1, dataFileExt))
	return writeMergeFileAndHint(db.path, db.activeFileId+1, alive)
}

func readDataFile(filename string) ([]*kvEntry, error) {
	fd, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	// TODO: determine the size of datafile, so we can allocate a buffer to read all data
	//       from datafile at once.
	entries := make([]*kvEntry, 0, 1024)
	pos := int64(0)
	header := make([]byte, kvEntry_bytes_fixedBytes)
	fi, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	for pos >= fi.Size() {
		// read fixed entry header.
		n, err2 := fd.ReadAt(header, pos)
		if err != nil || n != kvEntry_bytes_fixedBytes {
			return nil, err2
		}

		entry, err3 := decodeEntryFromHeader(header)
		if err3 != nil {
			return nil, err3
		}

		// read key.
		pos += kvEntry_bytes_fixedBytes
		n, err2 = fd.ReadAt(entry.key, pos)
		if err != nil || n != int(entry.keySize) {
			return nil, err2
		}

		// read value.
		pos += int64(entry.keySize)
		n, err2 = fd.ReadAt(entry.value, pos)
		if err != nil || n != int(entry.valueSize) {
			return nil, err2
		}

		entries = append(entries, entry)

		// step to next entry.
		pos += int64(entry.valueSize)
	}

	return entries, nil
}

// DONE: what if the datafile is too large to write into one file
//
//	over the maxDataFileSize(100MB)? split into another file?
func writeMergeFileAndHint(path string, fileId uint16, aliveEntries map[string]*kvEntry) error {
	// open new datafile and hint file.
	open := func(fileId uint16) (dataFile, hintFile *os.File, fn func(), err error) {
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
		if dataFile, err = os.OpenFile(dataFName, os.O_CREATE|os.O_RDWR, 0666); err != nil {
			return nil, nil, nil, err
		}

		hintFName := hintFilename(path, fileId)
		if hintFile, err = os.OpenFile(hintFName, os.O_CREATE|os.O_RDWR, 0666); err != nil {
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
		keydir *keydirEntry
		n      int
	)
	for _, entry := range aliveEntries {
		if n, err = dataFile.Write(entry.bytes()); err != nil {
			// TODO: handle err
			panic(err)
		}
		valueOff = entryOff + kvEntry_bytes_fixedBytes + uint32(entry.keySize)
		keydir = &keydirEntry{
			fileId:      fileId,
			valueSize:   entry.valueSize,
			tsTimestamp: entry.tsTimestamp,
			valueOffset: valueOff,
			entryOffset: entryOff,
		}
		if _, err = hintFile.Write(keydir.bytes()); err != nil {
			// TODO: handle err
			panic(err)
		}

		// open another file if the current file is too large (>= 100MB).
		if valueOff >= maxDataFileSize {
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
