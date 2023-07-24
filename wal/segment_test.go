package wal

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
)

const __segmentTestSuiteRoot = "./testdata/wal"

type segmentTestSuite struct {
	suite.Suite

	root string
}

func (s *segmentTestSuite) SetupTest() {
	s.root = __segmentTestSuiteRoot
	err := os.MkdirAll(s.root, 0755)
	s.NoError(err)
}

func (s *segmentTestSuite) TearDownTest() {
	err := os.RemoveAll(__segmentTestSuiteRoot)
	s.NoError(err)
}

func (s *segmentTestSuite) TestSegment_newSegment() {
	seg, err := newSegment(__segmentTestSuiteRoot, 1, 1)
	s.NoError(err)

	s.Equal(uint32(1), seg.Index)
	s.Equal(int64(1), seg.Start)
	s.Equal(int64(0), seg.End)
	//s.Equal(int64(-1), seg.Truncated)
	s.Equal("testdata/wal/0000000001.wal", seg.entryFilename)
	s.Equal("testdata/wal/0000000001.wal.meta", seg.metaFilename)
	s.NotNil(seg.entry)
	s.NotNil(seg.meta)
}

func (s *segmentTestSuite) TestSegment_write_read() {
	seg, err := newSegment(__segmentTestSuiteRoot, 1, 1)
	s.NoError(err)

	// write
	_, err = seg.write(Entry("hello world"))
	s.NoError(err)
	s.Equal(int64(1), seg.Start)
	s.Equal(int64(1), seg.End)
	//s.Equal(int64(-1), seg.Truncated)
	s.Equal(1, len(seg.entryPos))

	// read
	b, err := seg.read(1)
	s.NoError(err)
	s.Equal(Entry("hello world"), b)
}

func (s *segmentTestSuite) TestSegment_sync_readSegment() {
	// new a segment, then we write 10 entries into it
	// and then save it, then we open it and read it

	seg, err := newSegment(s.root, 1, 1)
	s.NoError(err)
	for i := 0; i < 10; i++ {
		_, err = seg.write(Entry("hello world" + strconv.Itoa(i)))
		s.NoError(err)
	}
	s.Equal(uint32(1), seg.Index)
	s.Equal(int64(1), seg.Start)
	s.Equal(int64(10), seg.End)
	//s.Equal(int64(-1), seg.Truncated)
	s.Equal(10, len(seg.entryPos))
	s.Equal("testdata/wal/0000000001.wal", seg.entryFilename)
	s.Equal("testdata/wal/0000000001.wal.meta", seg.metaFilename)

	// save and close the segment files
	err = seg.flush(false)
	s.NoError(err)
	s.Equal(false, seg.Archived)

	seg2, err2 := readSegment(s.root, segmentFile("", 1))
	s.NoError(err2)

	s.Equal(uint32(1), seg2.Index)
	s.Equal(int64(1), seg2.Start)
	s.Equal(int64(10), seg2.End)
	//s.Equal(int64(-1), seg2.Truncated)
	s.Equal(10, len(seg2.entryPos))
	s.Equal("testdata/wal/0000000001.wal", seg2.entryFilename)
	s.NotNil(seg2.entry) // since segment is not Archived
	s.Equal("testdata/wal/0000000001.wal.meta", seg2.metaFilename)
	s.NotNil(seg2.meta) // since segment is not Archived
	s.Equal(false, seg.Archived)

	// read the entries
	for i := 0; i < 10; i++ {
		entry, err := seg2.read(int64(i + 1))
		s.NoError(err)
		s.Equal(Entry("hello world"+strconv.Itoa(i)), entry)
	}
}

func (s *segmentTestSuite) TestSegment_archive_readSegment() {
	seg, err := newSegment(s.root, 1, 1)
	s.NoError(err)
	for i := 0; i < 10; i++ {
		_, err = seg.write(Entry("hello world" + strconv.Itoa(i)))
		s.NoError(err)
	}
	s.Equal(uint32(1), seg.Index)
	s.Equal(int64(1), seg.Start)
	s.Equal(int64(10), seg.End)
	//s.Equal(int64(-1), seg.Truncated)
	s.Equal(10, len(seg.entryPos))
	s.Equal("testdata/wal/0000000001.wal", seg.entryFilename)
	s.Equal("testdata/wal/0000000001.wal.meta", seg.metaFilename)
	s.Equal(false, seg.Archived)

	// save and close the segment files
	err = seg.archive()
	s.NoError(err)
	s.Equal(true, seg.Archived)

	seg2, err2 := readSegment(s.root, segmentFile("", 1))
	s.NoError(err2)

	s.Equal(uint32(1), seg2.Index)
	s.Equal(int64(1), seg2.Start)
	s.Equal(int64(10), seg2.End)
	//s.Equal(int64(-1), seg2.Truncated)
	s.Equal(10, len(seg2.entryPos))
	s.Equal("testdata/wal/0000000001.wal", seg2.entryFilename)
	s.Nil(seg2.entry) // since segment is Archived
	s.Equal("testdata/wal/0000000001.wal.meta", seg2.metaFilename)
	s.Nil(seg2.meta) // since segment is Archived
	s.Equal(true, seg.Archived)

	// read the entries
	for i := 0; i < 10; i++ {
		entry, err := seg2.read(int64(i + 1))
		s.NoError(err)
		s.Equal(Entry("hello world"+strconv.Itoa(i)), entry)
	}
}

func (s *segmentTestSuite) TestSegment_truncate0() {
	seg, err := newSegment(s.root, 1, 1)
	s.NoError(err)
	for i := 1; i <= 10; i++ {
		_, err = seg.write(Entry("hello world" + strconv.Itoa(i)))
		s.NoError(err)
	}
	s.Equal(uint32(1), seg.Index)
	s.Equal(int64(1), seg.Start)
	s.Equal(int64(10), seg.End)
	s.Equal(int64(-1), seg.Truncated)
	s.Equal(10, len(seg.entryPos))
	s.Equal("testdata/wal/0000000001.wal", seg.entryFilename)
	s.Equal("testdata/wal/0000000001.wal.meta", seg.metaFilename)
	s.Equal(false, seg.Archived)

	// markTruncated
	removed, err := seg.markTruncated(5)
	s.NoError(err)
	s.False(removed)
	s.Equal(int64(5), seg.Truncated)
	s.Equal(10, len(seg.entryPos))
	s.Equal(int64(5), seg.Truncated)
	s.False(seg.canRead(5))
	s.True(seg.canRead(6))

	// read from WAL file
	seg2, err2 := readSegment(s.root, segmentFile("", 1))
	s.NoError(err2)
	s.Equal(uint32(1), seg2.Index)
	s.Equal(int64(1), seg2.Start)
	s.Equal(int64(10), seg2.End)
	s.Equal(int64(5), seg2.Truncated)
	s.Equal(10, len(seg2.entryPos))
	s.Equal("testdata/wal/0000000001.wal", seg2.entryFilename)
	s.NotNil(seg2.entry) // since segment is not Archived
	s.Equal("testdata/wal/0000000001.wal.meta", seg2.metaFilename)
	s.NotNil(seg2.meta) // since segment is not Archived
	s.Equal(false, seg.Archived)

	// read the entries
	for i := 1; i <= 10; i++ {
		entry, err := seg2.read(int64(i))
		if i <= 5 {
			s.Error(err)
			s.ErrorIs(err, ErrSegmentInvalidOffset)
		} else {
			s.NoError(err)
			s.Equal(Entry("hello world"+strconv.Itoa(i)), entry)
		}
	}
}

func (s *segmentTestSuite) TestSegment_truncate1() {
	// write 10 entries to segment 1, then archive it and write 10 more entries to segment 2
	seg1, err := newSegment(s.root, 1, 1)
	s.NoError(err)
	for i := 1; i <= 10; i++ {
		_, err = seg1.write(Entry("hello world" + strconv.Itoa(i)))
		s.NoError(err)
	}
	err = seg1.archive()
	s.NoError(err)
	s.True(seg1.Archived)
	s.False(seg1.canWrite())
	s.True(seg1.canRead(1))
	s.True(seg1.canRead(10))

	// read from WAL file
	seg3, err3 := readSegment(s.root, segmentFile("", 1))
	s.NoError(err3)
	// now markTruncated 12, we expected segment 1 to be truncated totally
	removed, err := seg3.markTruncated(12)
	s.NoError(err)
	s.True(removed)
	s.Equal(10, len(seg3.entryPos))
	s.Equal(int64(12), seg3.Truncated)
	s.False(seg3.canRead(12))
	s.False(seg3.canWrite())

	// read from WAL file segment 1, should be empty
	_, err31 := readSegment(s.root, segmentFile("", 1))
	s.Error(err31)
}

func (s *segmentTestSuite) TestSegment_truncate2() {
	seg, err := newSegment(s.root, 2, 11)
	s.NoError(err)
	for i := 1; i <= 10; i++ {
		_, err = seg.write(Entry("hello world" + strconv.Itoa(i)))
		s.NoError(err)
	}
	err = seg.archive()
	s.NoError(err)

	// seg2 equal to seg
	seg2, err2 := readSegment(s.root, segmentFile("", 2))
	s.Require().NoError(err2)
	removed, err := seg2.markTruncated(12)
	s.False(removed)
	s.Require().NoError(err)

	seg3, err3 := readSegment(s.root, segmentFile("", 2))
	s.Require().NoError(err3)
	s.Equal(seg2.buf, seg3.buf)
	s.Equal(seg2.entryPos, seg3.entryPos)
	s.Equal(seg2.Truncated, seg3.Truncated)
	s.Equal(seg2.Start, seg3.Start)
	s.Equal(seg2.End, seg3.End)
	s.Equal(seg2.Index, seg3.Index)
	s.Equal(seg2.Archived, seg3.Archived)
	s.Equal(seg2.entryFilename, seg3.entryFilename)
	s.Equal(seg2.metaFilename, seg3.metaFilename)

	s.Equal(uint32(2), seg3.Index)
	s.Equal(int64(11), seg3.Start)
	s.Equal(int64(20), seg3.End)
	s.Equal(int64(12), seg3.Truncated)
	s.Equal(10, len(seg3.entryPos))
	s.Equal("testdata/wal/0000000002.wal", seg3.entryFilename)
	s.Nil(seg3.entry) // since segment is Archived
	s.Equal("testdata/wal/0000000002.wal.meta", seg3.metaFilename)
	s.Nil(seg3.meta) // since segment is not Archived
	s.Equal(true, seg3.Archived)

	for i := 11; i <= 20; i++ {
		entry, err := seg3.read(int64(i))
		if i <= 12 {
			s.Error(err)
			s.ErrorIs(err, ErrSegmentInvalidOffset)
		} else {
			s.NoError(err)
			s.Equal(Entry("hello world"+strconv.Itoa(i-10)), entry)
		}
	}
}

func (s *segmentTestSuite) TestSegment_truncate3() {
	// markTruncated a non-exist segment
	// create a segment and markTruncated it to 10
	seg, err := newSegment(s.root, 3, 21)
	s.NoError(err)
	for i := 0; i < 10; i++ {
		_, err = seg.write(Entry("hello world" + strconv.Itoa(i)))
		s.NoError(err)
	}

	// markTruncated to 10
	removed, err := seg.markTruncated(10)
	s.NoError(err)
	s.False(removed)
	// flush the segment
	err = seg.flush(false)
	s.NoError(err)

	// now we read it back
	seg2, err2 := readSegment(s.root, segmentFile("", 3))
	s.NoError(err2)
	s.Equal(seg.buf, seg2.buf)
	s.Equal(seg.entryPos, seg2.entryPos)
	////s.Equal(seg.Truncated, seg2.Truncated)
	s.Equal(seg.Start, seg2.Start)
	s.Equal(seg.End, seg2.End)
	s.Equal(seg.Index, seg2.Index)
	s.Equal(seg.Archived, seg2.Archived)
}

func Test_Segment(t *testing.T) {
	suite.Run(t, new(segmentTestSuite))
}
