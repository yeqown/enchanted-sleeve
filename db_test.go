package esl

import (
	"bytes"
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type dbTestSuite struct {
	suite.Suite

	fs FileSystem
	db *DB
}

func (su *dbTestSuite) SetupSuite() {
	var err error

	rand.New(rand.NewSource(time.Now().UnixNano()))

	su.prepareFileSystem()

	su.db, err = Open("./testdata", WithFileSystem(su.fs))
	su.Require().NoError(err)
}

func (su *dbTestSuite) prepareFileSystem() {
	su.fs = afero.NewMemMapFs()

	// clean up, remove testdata dir.
	err := su.fs.RemoveAll("./testdata")
	su.Require().NoError(err)
}

func (su *dbTestSuite) TearDownSuite() {
	err := su.db.Close()
	su.Require().NoError(err)

	// clean up, remove testdata dir.
	err = su.fs.RemoveAll("./testdata")
	su.Require().NoError(err)
}

func (su *dbTestSuite) Test_DB_GetSet() {
	key := []byte("Test_DB_GetSet")
	value := []byte("value")

	err := su.db.Put(key, value)
	su.NoError(err)

	valueData, err2 := su.db.Get(key)
	su.NoError(err2)
	su.Equal(value, valueData)
}

func (su *dbTestSuite) Test_DB_get() {
	key := []byte("Test_DB_get")
	value := []byte("value")

	err := su.db.Put(key, value)
	su.NoError(err)

	v1, err1 := su.db.get(key, true)
	su.NoError(err1)
	su.Equal(value, v1.value)

	v2, err2 := su.db.get(key, false)
	su.NoError(err2)
	clue := su.db.keyDir.get(key)
	su.NotNil(clue)
	su.Equal(value, v2.value)
	su.Equal(v2.valueSize, clue.valueSize)
	su.NotEmpty(v2.crc)
	su.NotEmpty(v2.tsTimestamp)
	su.NotEmpty(v2.keySize)
	su.NotEmpty(v2.valueSize)
	su.NotEmpty(v2.key)
	su.NotEmpty(v2.value)
	su.Equal(
		int(clue.valueOffset-clue.entryOffset+uint32(clue.valueSize)), // keydir
		int(kvEntry_fixedBytes+v2.keySize+v2.valueSize),               // entry
	)
}

func (su *dbTestSuite) Test_DB_Update() {
	key := []byte("Test_DB_GetSetWithEmptyValue")
	value := []byte("value_before")

	err := su.db.Put(key, value)
	su.NoError(err)

	valueData, err2 := su.db.Get(key)
	su.NoError(err2)
	su.Equal(value, valueData)

	value = []byte("value_after")
	err = su.db.Put(key, value)
	su.NoError(err)

	valueData, err2 = su.db.Get(key)
	su.NoError(err2)
	su.Equal(value, valueData)
}

func (su *dbTestSuite) Test_DB_Delete() {
	key := []byte("Test_DB_Delete")
	value := []byte("value")

	err := su.db.Put(key, value)
	su.NoError(err)

	err = su.db.Delete(key)
	su.NoError(err)

	valueData, err2 := su.db.Get(key)
	su.Error(err2)
	su.Nil(valueData)

	// delete again should not raise error
	err = su.db.Delete(key)
	su.NoError(err)
}

func (su *dbTestSuite) Test_DB_ListKeys() {
	key := []byte("Test_DB_ListKeys")
	value := []byte("value")

	err := su.db.Put(key, value)
	su.NoError(err)

	keys := su.db.ListKeys()
	su.Contains(keys, Key(key))

	err = su.db.Delete(key)
	su.NoError(err)

	keys = su.db.ListKeys()
	su.NotContains(keys, Key(key))
}

func Test_DB(t *testing.T) {
	suite.Run(t, new(dbTestSuite))
}

// go test -v -run ^Test_DB$ -testify.m ^Test_DB_concurrency_access$ -race ./...
func Test_DB_concurrency_access(t *testing.T) {
	fs := afero.NewMemMapFs()
	db, err := Open("/tmp/esl/", WithFileSystem(fs))
	require.NoError(t, err)

	nRoutine := 10
	nCountKey := 100

	keyFunc := func(routineIdx, keyIdx int) []byte {
		return []byte(strconv.Itoa(routineIdx) + "_Test_DB_concurrency_access_" + strconv.Itoa(keyIdx))
	}
	valueFunc := func(routineIdx, keyIdx int) []byte {
		return []byte("value_" + strconv.Itoa(routineIdx) + "_" + strconv.Itoa(keyIdx))
	}

	// There are 10 routines, each routine put 100 key-value pairs into db
	// in parallel, so there are 1000 key-value pairs in db.
	// And there is another 10 routines to get key-value pairs from db in parallel.
	wg := sync.WaitGroup{}
	for i := 0; i < nRoutine; i++ {
		wg.Add(1)
		go func(routineIdx int) {
			defer wg.Done()

			for j := 0; j < nCountKey; j++ {
				err = db.Put(keyFunc(routineIdx, j), valueFunc(routineIdx, j))
				assert.NoError(t, err)
			}
		}(i)
	}

	time.Sleep(100 * time.Millisecond)

	for i := 0; i < nRoutine; i++ {
		wg.Add(1)
		go func(routineIdx int) {
			defer wg.Done()

			for j := 0; j < nCountKey; j++ {
				time.Sleep(time.Microsecond)
				key := keyFunc(routineIdx, j)
				value, err := db.Get(key)
				if errors.Is(err, ErrKeyNotFound) {
					continue
				}

				want := valueFunc(routineIdx, j)
				assert.Truef(t, bytes.Equal(want, value), "key: %s, want(%s) != got(%s)", key, want, value)
			}
		}(i)
	}

	wg.Wait()
}

func Test_DB_MultiWriteGet(t *testing.T) {
	fs := afero.NewMemMapFs()
	db, err := Open("/tmp/esl/", WithFileSystem(fs))
	require.NoError(t, err)

	keyFunc := func(i int) []byte {
		return []byte("Test_DB_MultiWriteGet_" + strconv.Itoa(i))
	}
	valueFunc := func(i int) []byte {
		return []byte("value" + strconv.Itoa(i))
	}

	// 1000 key-value pairs
	for i := 0; i < 1000; i++ {
		key := keyFunc(i)
		value := valueFunc(i)
		err = db.Put(key, value)
		require.NoError(t, err)
	}

	// read 1000 key-value pairs
	for i := 0; i < 1000; i++ {
		key := keyFunc(i)
		got, err := db.Get(key)
		require.NoError(t, err)
		expected := valueFunc(i)
		assert.Equal(t, expected, got)
	}
}

func Test_DB_Sync(t *testing.T) {
	fs := afero.NewMemMapFs()
	db, err := Open("/tmp/esl/", WithFileSystem(fs))
	require.NoError(t, err)

	key := []byte("Test_DB_Sync") // 12 bytes
	value := []byte("value")      // 5 bytes
	err = db.Put(key, value)
	require.NoError(t, err)

	db.Sync()
	// check file content can not be empty
	dataFilename := dataFilename("/tmp/esl/", initDataFileId)
	dataFile, err := fs.Open(dataFilename)
	require.NoError(t, err)
	defer dataFile.Close()

	dataFileInfo, err := dataFile.Stat()
	require.NoError(t, err)
	require.NotZero(t, dataFileInfo.Size())
	assert.Equal(t, int64(kvEntry_fixedBytes)+12+5, dataFileInfo.Size())
}

func Test_DB_Close(t *testing.T) {
	fs := afero.NewMemMapFs()
	db, err := Open("/tmp/esl/", WithFileSystem(fs))
	require.NoError(t, err)

	key := []byte("Test_DB_Close") // 13 bytes
	value := []byte("value")       // 5 bytes
	err = db.Put(key, value)
	require.NoError(t, err)

	err = db.Close()
	require.NoError(t, err)
	// check file content can not be empty
	dataFilename := dataFilename("/tmp/esl/", initDataFileId)
	dataFile, err := fs.Open(dataFilename)
	require.NoError(t, err)
	defer dataFile.Close()

	dataFileInfo, err := dataFile.Stat()
	require.NoError(t, err)
	require.NotZero(t, dataFileInfo.Size())
	assert.Equal(t, int64(kvEntry_fixedBytes)+13+5, dataFileInfo.Size())
}

func Test_DB_Merge(t *testing.T) {
	fs := afero.NewMemMapFs()
	db, err := Open(
		"/tmp/esl/",
		WithFileSystem(fs),         // using mem fs
		WithMaxFileBytes(100),      // 100B
		WithCompactThreshold(1000), // avoid auto merge
	)
	require.NoError(t, err)

	// generate about 4 files, we need more than 400B data, so we need more than 4 * (100/25) = 16 entries
	// create 10 entry first.
	kvEntries := randomKVEntries(10)
	for _, kv := range kvEntries {
		err = db.Put(kv.key, kv.value)
		require.NoError(t, err)
	}
	// and we delete all 10 entries, so that add and delete can be counteracted.
	count := 0
	for key := range kvEntries {
		count++
		if count > 10 {
			break
		}

		err = db.Delete([]byte(key))
		require.NoError(t, err)
		delete(kvEntries, key)
	}
	// create another 6 entry
	kvEntries2 := randomKVEntries(6)
	for _, kv := range kvEntries2 {
		err = db.Put(kv.key, kv.value)
		require.NoError(t, err)
	}

	// expected more than 1 files
	snap, err := takeDBPathSnap(fs, "/tmp/esl/")
	require.NoError(t, err)
	require.NotNil(t, snap)
	assert.Equal(t, 5, len(snap.dataFiles))
	assert.Equal(t, 0, len(snap.hintFiles))
	assert.Equal(t, uint16(5), snap.lastDataFileId)

	// trigger merge
	err = db.Merge()
	require.NoError(t, err)

	// we need to wait compact goroutine finish
	time.Sleep(100 * time.Millisecond)
	for db.inCompaction.Load() {
		time.Sleep(10 * time.Millisecond)
	}

	snap, err = takeDBPathSnap(fs, "/tmp/esl/")
	require.NoError(t, err)
	require.NotNil(t, snap)

	// expected 2 data files, 1 hint file
	assert.Equal(t, 3, len(snap.dataFiles))
	assert.Equal(t, 2, len(snap.hintFiles))
	assert.ElementsMatch(t, []string{"/tmp/esl/0000000005.esld", "/tmp/esl/0000000004.esld", "/tmp/esl/0000000003.esld"}, snap.dataFiles)
	assert.ElementsMatch(t, []string{"/tmp/esl/0000000004.hint", "/tmp/esl/0000000003.hint"}, snap.hintFiles)
	assert.Equal(t, uint16(5), snap.lastDataFileId)
	assert.EqualValues(t, 6, len(db.ListKeys()))
}

func Test_DB_filesystem(t *testing.T) {

	osFs := "OsFs"
	memMapFs := "MemMapFS"

	type args struct {
		db *DB
	}

	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "case 1",
			args: args{
				db: nil,
			},
			want: osFs,
		},
		{
			name: "case 2",
			args: args{
				db: &DB{},
			},
			want: osFs,
		},
		{
			name: "case 3",
			args: args{
				db: &DB{
					opt: &options{},
				},
			},
			want: osFs,
		},
		{
			name: "case 4",
			args: args{
				db: &DB{
					opt: &options{
						fs: afero.NewMemMapFs(),
					},
				},
			},
			want: memMapFs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.args.db.filesystem()
			assert.Equal(t, tt.want, got.Name())
		})
	}
}
