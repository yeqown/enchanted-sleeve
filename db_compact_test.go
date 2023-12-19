package esl

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// randomKVEntries generate random kv entries. each entry cost about
// 25 bytes (kvEntry_fixedBytes + len(key) + len(value)), each key and value
// cost about 5 - 10 bytes, it depends on the random number.
func randomKVEntries(n int) map[string]*kvEntry {
	entries := make(map[string]*kvEntry, n)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		ent := &kvEntry{
			tsTimestamp: uint32(i),
			keySize:     uint16(len(key)),
			valueSize:   uint16(len(value)),
			key:         key,
			value:       value,
		}
		ent.fillcrc()
		entries[string(key)] = ent
	}
	return entries
}

func Test_mergeFiles(t *testing.T) {
	fs := afero.NewMemMapFs()
	path := "/tmp/esl"
	actualFileId := uint16(3)

	// 100 entries cost about 25 * 100 = 2.5 KB, avoid merging process produces
	// more than one file, we set the oversize to 1 MB.
	oversize := func(off uint32) bool {
		return off > 1024*1024
	}

	// prepare files, there has 4 files, each file has 100 entries, keys are same in each file.
	// so we should have 100 entries after merge.
	//
	// And the /tmp/esl/0000000004.esld is the active file, it should not be merged.
	entries := randomKVEntries(100)
	for i := 0; i < 4; i++ {
		filename := fmt.Sprintf("/tmp/esl/000000000%d.esld", i)
		for _, ent := range entries {
			_, err := writeEntryIntoFile(fs, uint16(i), filename, ent)
			require.NoError(t, err)
		}
	}

	err := mergeFiles(fs, path, actualFileId, oversize)
	assert.NoError(t, err)

	// expected 2 data files (0000000002.esld, 0000000003.esld) after merge, and a
	// hint file (0000000002.hint) for 0000000002.esld.
	exists, err := afero.Exists(fs, "/tmp/esl/0000000002.esld")
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = afero.Exists(fs, "/tmp/esl/0000000003.esld")
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = afero.Exists(fs, "/tmp/esl/0000000002.hint")
	assert.NoError(t, err)
	assert.True(t, exists)

	snap, err := takeDBPathSnap(fs, path)
	assert.NoError(t, err)
	assert.Equal(t, actualFileId, snap.lastDataFileId)
	assert.Equal(t, 2, len(snap.dataFiles))
	assert.Equal(t, 1, len(snap.hintFiles))
}

func Test_writeMergeFileAndHint(t *testing.T) {
	fs := afero.NewMemMapFs()
	path := "/tmp/esl"
	maxFileId := uint16(4)

	entries := randomKVEntries(1000)
	oversize := func(off uint32) bool {
		// 16 KB
		return off >= 16*1024
	}

	err := writeMergeFileAndHint(fs, path, maxFileId, entries, oversize)
	assert.NoError(t, err)

	// 1000 entries cost about 25 * 1000 = 25 KB,
	// so we should have 2 data files. (0000000002.esld, 0000000003.esld)
	// and 2 hint file (0000000002.hint, 0000000003.hint)

	exists, err := afero.Exists(fs, "/tmp/esl/0000000004.esld")
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = afero.Exists(fs, "/tmp/esl/0000000003.esld")
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = afero.Exists(fs, "/tmp/esl/0000000004.hint")
	assert.NoError(t, err)
	assert.True(t, exists)

	exists, err = afero.Exists(fs, "/tmp/esl/0000000003.hint")
	assert.NoError(t, err)
	assert.True(t, exists)

	snap, err := takeDBPathSnap(fs, path)
	assert.NoError(t, err)
	assert.Equal(t, uint16(maxFileId+1), snap.lastDataFileId)
	assert.Equal(t, 2, len(snap.dataFiles))
	assert.Equal(t, 2, len(snap.hintFiles))
}

func writeEntryIntoFile(fs FileSystem, fileId uint16, filename string, entry *kvEntry) (keydir *keydirMemEntry, err error) {
	file, err := fs.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}
	pos := fi.Size()

	_, err = file.Write(entry.bytes())
	keydir = &keydirMemEntry{
		fileId:      fileId,
		valueSize:   entry.valueSize,
		entryOffset: uint32(pos),
		valueOffset: uint32(pos) + kvEntry_fixedBytes + uint32(entry.keySize),
	}

	return keydir, err
}

func writeHintIntoFile(fs FileSystem, filename string, keydir *keydirFileEntry) error {
	file, err := fs.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(keydir.bytes())
	return err
}

func Test_restoreKeydirIndex_withHintFile(t *testing.T) {
	fs := afero.NewMemMapFs()
	keydirIndex := newKeyDir()

	// prepare data files
	randomEntries := randomKVEntries(10)
	count := 0
	var err error
	var keydir *keydirMemEntry
	for _, ent := range randomEntries {
		count++
		if count <= 5 {
			keydir, err = writeEntryIntoFile(fs, 1, "/tmp/esl/0000000001.esld", ent)
		} else {
			_, err = writeEntryIntoFile(fs, 2, "/tmp/esl/0000000002.esld", ent)
		}
		require.NoError(t, err)

		// only save keydir entry for 0000000001.esld
		err = writeHintIntoFile(fs, "/tmp/esl/0000000001.hint", &keydirFileEntry{
			keydirMemEntry: *keydir,
			keySize:        uint16(len(ent.key)),
			key:            ent.key,
		})
		require.NoError(t, err)
	}

	// restore keydir index
	snap := &dbPathSnap{
		path: "/tmp/esl",
		dataFiles: []string{
			"/tmp/esl/0000000001.esld",
			"/tmp/esl/0000000002.esld",
		},
		hintFiles: []string{
			"/tmp/esl/0000000001.hint",
		},
		lastDataFileId: 2,
	}
	err = restoreKeydirIndex(fs, snap, keydirIndex)
	assert.NoError(t, err)

	// we should have 10 entries in keydirIndex and keydirIndex should have
	// the same entries with randomEntries.
	assert.Equal(t, 10, keydirIndex.len())
	for key, ent := range randomEntries {
		clue := keydirIndex.get([]byte(key))
		assert.NotNil(t, clue)
		assert.NotEmpty(t, clue.fileId)
		assert.NotEmpty(t, clue.valueSize)
		// assert.NotEmpty(t, clue.entryOffset)
		assert.NotEmpty(t, clue.valueOffset)

		assert.Equal(t, ent.valueSize, clue.valueSize)
		assert.Contains(t, []uint16{1, 2}, clue.fileId)
		assert.Equal(t, int(ent.keySize), int(clue.valueOffset-clue.entryOffset-kvEntry_fixedBytes))
	}
}

func Test_restoreKeydirIndex_withoutHintFile(t *testing.T) {
	fs := afero.NewMemMapFs()
	keydirIndex := newKeyDir()

	// prepare data files
	randomEntries := randomKVEntries(10)
	count := 0
	var err error
	for _, ent := range randomEntries {
		count++
		if count <= 5 {
			_, err = writeEntryIntoFile(fs, 1, "/tmp/esl/0000000001.esld", ent)
		} else {
			_, err = writeEntryIntoFile(fs, 2, "/tmp/esl/0000000002.esld", ent)
		}
		require.NoError(t, err)
	}

	snap := &dbPathSnap{
		path: "/tmp/esl",
		dataFiles: []string{
			"/tmp/esl/0000000001.esld",
			"/tmp/esl/0000000002.esld",
		},
		hintFiles:      []string{},
		lastDataFileId: 2,
	}

	err = restoreKeydirIndex(fs, snap, keydirIndex)
	assert.NoError(t, err)

	// we should have 10 entries in keydirIndex and keydirIndex should have
	// the same entries with randomEntries.
	assert.Equal(t, 10, keydirIndex.len())
	for key, ent := range randomEntries {
		clue := keydirIndex.get([]byte(key))
		assert.NotNil(t, clue)
		assert.NotEmpty(t, clue.fileId)
		assert.NotEmpty(t, clue.valueSize)
		// assert.NotEmpty(t, clue.entryOffset)
		assert.NotEmpty(t, clue.valueOffset)

		assert.Equal(t, ent.valueSize, clue.valueSize)
		assert.Contains(t, []uint16{1, 2}, clue.fileId)
		assert.Equal(t, int(ent.keySize), int(clue.valueOffset-clue.entryOffset-kvEntry_fixedBytes))
	}
}

func Test_readDataFile(t *testing.T) {
	fs := afero.NewMemMapFs()
	filename := "/tmp/esl/0000000001.esld"
	fileId := uint16(1)

	expectedKeydirs := make(map[string]*keydirMemEntry, 10)
	expectedKVs := randomKVEntries(10)

	// prepare data file
	for _, ent := range expectedKVs {
		keydir, err := writeEntryIntoFile(fs, fileId, filename, ent)
		require.NoError(t, err)
		expectedKeydirs[string(ent.key)] = keydir
	}

	gotKVs, gotKeydirs, err := readDataFile(fs, filename, fileId)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(gotKVs))
	assert.Equal(t, 10, len(gotKeydirs))

	// expected gotKVs and gotKeydirs should be equal with actual gotKVs and gotKeydirs.
	for _, gotKV := range gotKVs {
		expected, ok := expectedKVs[string(gotKV.key)]
		assert.True(t, ok)
		assert.Equal(t, expected.crc, gotKV.crc)
		assert.Equal(t, expected.tsTimestamp, gotKV.tsTimestamp)
		assert.Equal(t, expected.keySize, gotKV.keySize)
		assert.Equal(t, expected.valueSize, gotKV.valueSize)
		assert.Equal(t, expected.key, gotKV.key)
		assert.Equal(t, expected.value, gotKV.value)
	}

	for key, gotKeydir := range gotKeydirs {
		expected, ok := expectedKeydirs[key]
		assert.True(t, ok)
		assert.Equal(t, expected.fileId, gotKeydir.fileId)
		assert.Equal(t, expected.valueSize, gotKeydir.valueSize)
		assert.Equal(t, expected.entryOffset, gotKeydir.entryOffset)
		assert.Equal(t, expected.valueOffset, gotKeydir.valueOffset)
	}
}

func Test_DB_autoCompact(t *testing.T) {
	fs := afero.NewMemMapFs()

	db, err := Open(
		"/tmp/esl",
		WithFileSystem(fs),
		WithMaxFileBytes(100),
		WithCompactThreshold(4),
		WithCompactInterval(time.Second),
	)
	require.NoError(t, err)

	// write 10 kvEntries and then delete them all 4 times, so that we can simulate the
	// compact situation (more than 4 files, and can be compacted since we delete data).
	// Then wait the auto compact process trigger and finish.

	for i := 0; i < 4; i++ {
		entries := randomKVEntries(10)
		for _, ent := range entries {
			err = db.Put(ent.key, ent.value)
			require.NoError(t, err)
		}

		for key := range entries {
			err = db.Delete([]byte(key))
			require.NoError(t, err)
		}
	}

	// make sure the auto compact process has been triggered and finish.
	time.Sleep(2 * time.Second)

	// wait compact process finish
	for db.inCompaction.Load() {
		time.Sleep(time.Millisecond)
	}

	// we expect only 2 data file and one hint file after compact.
	snap, err := takeDBPathSnap(fs, "/tmp/esl")
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, 3, len(snap.dataFiles))
	assert.Equal(t, 2, len(snap.hintFiles))
}
