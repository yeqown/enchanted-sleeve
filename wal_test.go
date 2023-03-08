package wal_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/yeqown/go-wal"
)

type testSuiteWAL struct {
	suite.Suite

	WAL *wal.WAL
}

func (t *testSuiteWAL) SetupSuite() {
	var err error
	t.WAL, err = wal.NewWAL(
		wal.DefaultConfig(),
		wal.WithRoot("./testdata/wal"),
		wal.WithMaxSegments(1024),
	)
	t.Require().NoError(err)
}

func (t *testSuiteWAL) TearDownSuite() {
	t.Require().NoError(t.WAL.Close())
}

func (t *testSuiteWAL) Test_WAL_WriteRead() {
	getEntry := func(i int) wal.Entry {
		return wal.Entry("hello world " + strconv.Itoa(i))
	}

	// write
	for i := 0; i < 100; i++ {
		offset, err := t.WAL.Write(getEntry(i))
		t.Require().NoError(err)
		t.Require().Equal(int64(i+1), offset)
	}

	// read
	for i := 0; i < 100; i++ {
		b, err := t.WAL.Read(int64(i + 1))
		t.Require().NoError(err)
		t.Require().Equal(getEntry(i), b)
	}
}

func Test_WAL(t *testing.T) {
	suite.Run(t, new(testSuiteWAL))
}
