package esl_test

import (
	"os"
	"testing"

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

func Test_DB(t *testing.T) {
	suite.Run(t, new(dbTestSuite))
}
