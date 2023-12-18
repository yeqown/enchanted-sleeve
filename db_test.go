package esl_test

import (
	"errors"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	esl "github.com/yeqown/enchanted-sleeve"
)

type dbTestSuite struct {
	suite.Suite

	db *esl.DB
}

func (su *dbTestSuite) SetupSuite() {
	var err error
	su.db, err = esl.Open("./testdata")
	su.Require().NoError(err)
}

func (su *dbTestSuite) TearDownSuite() {
	err := su.db.Close()
	su.Require().NoError(err)

	// clean up, remove testdata dir.
	err = os.RemoveAll("./testdata")
	su.Require().NoError(err)
}

func (su *dbTestSuite) Test_DB_GetSet() {
	err := su.db.Put([]byte("key"), []byte("value"))
	su.NoError(err)

	value, err := su.db.Get([]byte("key"))
	su.NoError(err)
	su.Equal([]byte("value"), value)
}

// go test -v -run ^Test_DB$ -testify.m ^Test_DB_concurrency_access$ -race ./...
func (su *dbTestSuite) Test_DB_concurrency_access() {
	nRoutine := 10
	nCountKey := 10

	wg := sync.WaitGroup{}
	for i := 0; i < nRoutine; i++ {
		wg.Add(1)
		go func(routineIdx int) {
			defer wg.Done()

			for j := 0; j < nCountKey; j++ {
				key := []byte(strconv.Itoa(routineIdx) + "_key" + strconv.Itoa(j))
				value := []byte("value" + strconv.Itoa(j))
				err := su.db.Put(key, value)
				su.NoError(err)
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		rand.New(rand.NewSource(time.Now().UnixNano()))
		for j := 0; j < nCountKey; j++ {
			routineIdx := rand.Intn(nRoutine)
			time.Sleep(time.Microsecond)
			key := []byte(strconv.Itoa(routineIdx) + "_key" + strconv.Itoa(j))
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
