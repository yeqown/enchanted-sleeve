package wal

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
)

const __testSuiteWALRoot = "./testdata/wal2"

type testSuiteWAL struct {
	suite.Suite
}

var getEntry = func(i int) Entry {
	return Entry("hello world " + strconv.Itoa(i))
}

func (t *testSuiteWAL) SetupTest() {
}

func (t *testSuiteWAL) TearDownTest() {
	_ = os.RemoveAll(__testSuiteWALRoot)
}

func (t *testSuiteWAL) Test_WAL_WriteRead() {
	wal, err := NewWAL(
		DefaultConfig(),
		WithRoot(__testSuiteWALRoot),
		WithMaxSegments(10),
		WithMaxSegmentSize(1024),
	)
	t.Require().NoError(err)

	// write
	for i := 0; i < 100; i++ {
		offset, err := wal.Write(getEntry(i))
		t.Require().NoError(err)
		t.Require().Equal(int64(i+1), offset)
	}

	// read
	for i := 0; i < 100; i++ {
		b, err := wal.Read(int64(i + 1))
		t.Require().NoError(err)
		t.Require().Equal(getEntry(i), b)
	}
}

func (t *testSuiteWAL) Test_WAL_Restore() {
	wal, err := NewWAL(
		DefaultConfig(),
		WithRoot(__testSuiteWALRoot),
		WithMaxSegments(10),
		WithMaxSegmentSize(1024),
	)
	t.Require().NoError(err)

	// write
	for i := 0; i < 100; i++ {
		offset, err := wal.Write(getEntry(i))
		t.Require().NoError(err)
		t.Require().Equal(int64(i+1), offset)
	}

	err = wal.Close()
	t.Require().NoError(err)

	wal2, err2 := NewWAL(
		DefaultConfig(),
		WithRoot(__testSuiteWALRoot),
		WithMaxSegments(10),
		WithMaxSegmentSize(1024),
	)
	t.Require().NoError(err2)

	// read
	for i := 0; i < 100; i++ {
		b, err := wal2.Read(int64(i + 1))
		t.Require().NoError(err)
		t.Require().Equal(getEntry(i), b)
	}
}

func (t *testSuiteWAL) Test_WAL_TruncateBefore() {
	wal, err := NewWAL(
		DefaultConfig(),
		WithRoot(__testSuiteWALRoot),
		WithMaxSegments(10),
		WithMaxSegmentSize(1024),
	)
	t.Require().NoError(err)

	// write
	for i := 1; i <= 100; i++ {
		offset, err := wal.Write(getEntry(i))
		t.Require().NoError(err)
		t.Require().Equal(int64(i), offset)
	}

	// markTruncated before
	err = wal.TruncateBefore(50)
	t.Require().NoError(err)

	// read
	for i := 1; i <= 100; i++ {
		b, err := wal.Read(int64(i))
		if i <= 50 {
			t.Require().Error(err, "i: %d", i)
			t.Require().Equal(ErrEntryNotFound, err)
		} else {
			t.Require().NoError(err, "i: %d", i)
			t.Require().Equal(getEntry(i), b)
		}
	}
}

func (t *testSuiteWAL) Test_WAL_TruncateBefore_Restore() {
	wal, err := NewWAL(
		DefaultConfig(),
		WithRoot(__testSuiteWALRoot),
		WithMaxSegments(10),
		WithMaxSegmentSize(1024),
	)
	t.Require().NoError(err)

	// write
	for i := 1; i <= 100; i++ {
		offset, err := wal.Write(getEntry(i))
		t.Require().NoError(err)
		t.Require().Equal(int64(i), offset)
	}

	// markTruncated after
	err = wal.TruncateBefore(50)
	t.Require().NoError(err)
	err = wal.Close()
	t.Require().NoError(err)

	wal2, err2 := NewWAL(
		DefaultConfig(),
		WithRoot(__testSuiteWALRoot),
		WithMaxSegments(10),
		WithMaxSegmentSize(1024),
	)
	t.Require().NoError(err2)

	// read
	for i := 1; i <= 100; i++ {
		b, err := wal2.Read(int64(i))
		if i <= 50 {
			t.Require().Error(err)
			t.Require().Equal(wal2.ErrEntryNotFound, err)
		} else {
			t.Require().NoError(err)
			t.Require().Equal(getEntry(i), b)
		}
	}
}

func (t *testSuiteWAL) Test_WAL_OverThan_MaxSegments() {
	wal, err := NewWAL(
		DefaultConfig(),
		WithRoot(__testSuiteWALRoot),
		WithMaxSegments(10),
		WithMaxSegmentSize(1024),
	)
	t.Require().NoError(err)

	// write over than 10 * 1024B
	for i := 0; i < 10000; i++ {
		offset, err := wal.Write(getEntry(i))
		t.Require().NoError(err)
		t.Require().Equal(int64(i+1), offset)
	}

	// check segments
	t.Equal(wal.MaxSegments, len(wal.segments))
	// the first segment CANNOT start from 1
	t.NotEqual(int64(1), wal.segments[0].Start)
	// the last segment MUST end with 10000
	t.Equal(int64(10000), wal.segments[len(wal.segments)-1].End)
	// sum of all segment buf size MUST be less than 10 * 1024B
	var sum int64
	for _, s := range wal.segments {
		sum += int64(s.size())
	}
	t.Less(sum, int64(wal.MaxSegments)*wal.MaxSegmentSize)
	t.Equal(int64(10000), wal.latest)
	t.Equal(wal.segments[0].Start, wal.oldest)

	// close and reopen
	err = wal.Close()
	t.Require().NoError(err)
	wal2, err2 := NewWAL(
		DefaultConfig(),
		WithRoot(__testSuiteWALRoot),
		WithMaxSegments(10),
		WithMaxSegmentSize(1024),
	)
	t.Require().NoError(err2)

	// read not removed entry MUST return getEntry(i)
	oldest := wal2.segments[0].Start
	latest := wal2.segments[len(wal2.segments)-1].End
	t.Equal(wal.oldest, wal2.oldest)
	t.Equal(wal2.oldest, oldest)
	t.Equal(wal.latest, wal2.latest)
	t.Equal(wal2.latest, latest)

	// read removed entry MUST return ErrEntryNotFound
	for i := int64(1); i < oldest; i++ {
		_, err := wal2.Read(i)
		t.Require().Error(err)
		t.Require().Equal(wal2.ErrEntryNotFound, err)
	}
	for i := oldest; i <= latest; i++ {
		b, err := wal2.Read(i)
		t.Require().NoError(err)
		t.Require().Equal(getEntry(int(i-1)), b)
	}
}

func Test_WAL(t *testing.T) {
	suite.Run(t, new(testSuiteWAL))
}
