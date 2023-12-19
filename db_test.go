package esl_test

import (
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/suite"

	esl "github.com/yeqown/enchanted-sleeve"
)

type dbTestSuite struct {
	suite.Suite

	fs esl.FileSystem
	db *esl.DB
}

func (su *dbTestSuite) SetupSuite() {
	var err error

	su.prepareFileSystem()

	su.db, err = esl.Open("./testdata", esl.WithFileSystem(su.fs))
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
	key := []byte("key")
	value := []byte("value")

	err := su.db.Put(key, value)
	su.NoError(err)

	valueData, err2 := su.db.Get(key)
	su.NoError(err2)
	su.Equal(value, valueData)
}

// go test -v -run ^Test_DB$ -testify.m ^Test_DB_concurrency_access$ -race ./...
func (su *dbTestSuite) Test_DB_concurrency_access() {
	nRoutine := 10
	nCountKey := 10

	keyFunc := func(routineIdx, keyIdx int) []byte {
		return []byte(strconv.Itoa(routineIdx) + "_Test_DB_concurrency_access_" + strconv.Itoa(keyIdx))
	}
	valueFunc := func(routineIdx, keyIdx int) []byte {
		return []byte("value" + strconv.Itoa(keyIdx))
	}

	wg := sync.WaitGroup{}
	for i := 0; i < nRoutine; i++ {
		wg.Add(1)
		go func(routineIdx int) {
			defer wg.Done()

			for j := 0; j < nCountKey; j++ {
				err := su.db.Put(keyFunc(routineIdx, j), valueFunc(routineIdx, j))
				su.NoError(err)
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		rand.New(rand.NewSource(time.Now().UnixNano()))
		for j := 0; j < nCountKey; j++ {
			time.Sleep(time.Microsecond)

			routineIdx := rand.Intn(nRoutine)
			key := keyFunc(routineIdx, j)
			value, err := su.db.Get(key)
			if errors.Is(err, esl.ErrKeyNotFound) {
				continue
			}
			su.Equal([]byte("value"+strconv.Itoa(j)), value)
		}
	}()

	wg.Wait()
}

func Test_DB(t *testing.T) {
	suite.Run(t, new(dbTestSuite))
}
