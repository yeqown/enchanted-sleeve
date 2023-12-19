package esl

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

func dataFilename(path string, fileId uint16) string {
	name := fmt.Sprintf("%010d%s", fileId, dataFileExt)
	return filepath.Join(path, name)
}

func hintFilename(path string, fileId uint16) string {
	name := fmt.Sprintf("%010d%s", fileId, hintFileExt)
	return filepath.Join(path, name)
}

// fileIdFromFilename parse file id from filename.
// e.g.
// - 0000000001.esld         -> 1
// - path/to/0000000002.esld -> 2
func fileIdFromFilename(filename string) (uint16, error) {
	_, name := filepath.Split(filename)

	ext := filepath.Ext(name)
	if !strings.EqualFold(ext, dataFileExt) && !strings.EqualFold(ext, hintFileExt) {
		return 0, errors.Errorf("invalid file ext: %s", ext)
	}

	var fileId uint16
	_, err := fmt.Sscanf(name, "%010d", &fileId)
	if err != nil {
		return 0, errors.Wrap(err, "parse file id failed")
	}

	return fileId, nil
}

type dbPathSnap struct {
	path      string
	dataFiles []string
	hintFiles []string

	lastDataFileId uint16
}

func (snap dbPathSnap) lastActiveFile(path string) string {
	return dataFilename(path, snap.lastDataFileId)
}

// isEmpty check whether db path is empty, there has no data file or hint file.
func (snap dbPathSnap) isEmpty() bool {
	return len(snap.dataFiles) == 0 && len(snap.hintFiles) == 0
}

func takeDBPathSnap(fs FileSystem, path string) (snap *dbPathSnap, err error) {
	snap = &dbPathSnap{
		path:           path,
		lastDataFileId: initDataFileId,
	}
	pattern := filepath.Join(path, dataFilePattern)
	if snap.dataFiles, err = afero.Glob(fs, pattern); err != nil {
		return nil, errors.Wrap(err, "takeDBPathSnap glob data files")
	}

	pattern = filepath.Join(path, hintFilePattern)
	if snap.hintFiles, err = afero.Glob(fs, pattern); err != nil {
		return nil, errors.Wrap(err, "takeDBPathSnap glob hint files")
	}

	if len(snap.dataFiles) == 0 && len(snap.hintFiles) == 0 {
		return snap, nil
	}

	if len(snap.dataFiles) != 0 {
		snap.lastDataFileId, err = lastFileIdFromFilenames(snap.dataFiles)
		if err != nil {
			return nil, errors.Wrap(err, "takeDBPathSnap parse data file id")
		}
	}

	if len(snap.hintFiles) != 0 {
		// This case is abnormal, because hint file must be existed with data file.
		// But we still handle it. And notice snap.dataFileId should bigger than the
		// latest hintFileId, so we add 1 to it.
		snap.lastDataFileId, err = lastFileIdFromFilenames(snap.hintFiles)
		snap.lastDataFileId++
	}

	return snap, nil
}

func lastFileIdFromFilenames(filenames []string) (uint16, error) {
	if len(filenames) == 0 {
		return 0, nil
	}
	if len(filenames) == 1 {
		return fileIdFromFilename(filenames[0])
	}

	fileIds := make([]int, 0, 8)
	for _, filename := range filenames {
		fileId, err := fileIdFromFilename(filename)
		if err != nil {
			return 0, errors.Wrap(err, "takeDBPathSnap parse data file id")
		}
		fileIds = append(fileIds, int(fileId))
	}

	sort.Sort(sort.Reverse(sort.IntSlice(fileIds)))

	return uint16(fileIds[0]), nil
}

func ensurePath(fs FileSystem, path string) error {
	exists, err := afero.DirExists(fs, path)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	return fs.MkdirAll(path, 0744)
}

// backupFile rename filename to filename.bak, it will return a restore function
// and a clean function. The restore function will rename filename.bak to filename,
// and the clean function will remove filename.
func backupFile(fs FileSystem, filename string) (restoreFn func() error, cleanFn func() error, err error) {
	oldName := filename
	backupName := filename + ".bak"

	if err = fs.Rename(filename, backupName); err != nil {
		return nil, nil, errors.Wrap(err, "backupFile rename failed")
	}

	restoreFn = func() error {
		return fs.Rename(backupName, oldName)
	}

	cleanFn = func() error {
		return fs.Remove(backupName)
	}

	return restoreFn, cleanFn, nil
}
