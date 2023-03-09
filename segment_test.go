package wal

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/suite"
)

type segmentTestSuite struct {
	suite.Suite

	root string
}

func (s *segmentTestSuite) SetupTest() {
	s.root = "./testdata/wal"
	err := os.MkdirAll(s.root, 0755)
	s.NoError(err)
}

func (s *segmentTestSuite) TearDownTest() {
	err := os.RemoveAll("./testdata/wal")
	s.NoError(err)
}

func (s *segmentTestSuite) TestSegment_newSegment() {
	seg, err := newSegment("./testdata/wal", 1, 1)
	s.NoError(err)

	s.Equal(uint32(1), seg.Index)
	s.Equal(int64(1), seg.Start)
	s.Equal(int64(0), seg.End)
	s.Equal(int64(0), seg.Truncated)
	s.Equal("testdata/wal/0000000001.wal", seg.entryFilename)
	s.Equal("testdata/wal/0000000001.wal.meta", seg.metaFilename)
	s.NotNil(seg.entry)
	s.NotNil(seg.meta)
}

func (s *segmentTestSuite) TestSegment_write_read() {
	seg, err := newSegment("./testdata/wal", 1, 1)
	s.NoError(err)

	// write
	_, err = seg.write(Entry("hello world"))
	s.NoError(err)
	s.Equal(int64(1), seg.Start)
	s.Equal(int64(1), seg.End)
	s.Equal(int64(0), seg.Truncated)
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
	s.Equal(int64(0), seg.Truncated)
	s.Equal(10, len(seg.entryPos))
	s.Equal("testdata/wal/0000000001.wal", seg.entryFilename)
	s.Equal("testdata/wal/0000000001.wal.meta", seg.metaFilename)

	// save and close the segment files
	err = seg.sync()
	s.NoError(err)
	s.Equal(false, seg.Archived)

	seg2, err2 := readSegment(s.root, segmentFile("", 1))
	s.NoError(err2)

	s.Equal(uint32(1), seg2.Index)
	s.Equal(int64(1), seg2.Start)
	s.Equal(int64(10), seg2.End)
	s.Equal(int64(0), seg2.Truncated)
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
	s.Equal(int64(0), seg.Truncated)
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
	s.Equal(int64(0), seg2.Truncated)
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
	for i := 0; i < 10; i++ {
		_, err = seg.write(Entry("hello world" + strconv.Itoa(i)))
		s.NoError(err)
	}
	s.Equal(uint32(1), seg.Index)
	s.Equal(int64(1), seg.Start)
	s.Equal(int64(10), seg.End)
	s.Equal(int64(0), seg.Truncated)
	s.Equal(10, len(seg.entryPos))
	s.Equal("testdata/wal/0000000001.wal", seg.entryFilename)
	s.Equal("testdata/wal/0000000001.wal.meta", seg.metaFilename)
	s.Equal(false, seg.Archived)

	// truncate
	err = seg.truncate(5)
	s.NoError(err)
	s.Less(seg.Truncated, seg.Start)
	s.Equal(seg.Truncated+1, seg.Start)
	s.Equal(6, len(seg.entryPos))

	// read from WAL file
	seg2, err2 := readSegment(s.root, segmentFile("", 1))
	s.NoError(err2)

	s.Equal(uint32(1), seg2.Index)
	s.Equal(int64(5), seg2.Start)
	s.Equal(int64(10), seg2.End)
	s.Equal(int64(4), seg2.Truncated)
	s.Equal(6, len(seg2.entryPos))
	s.Equal("testdata/wal/0000000001.wal", seg2.entryFilename)
	s.NotNil(seg2.entry) // since segment is not Archived
	s.Equal("testdata/wal/0000000001.wal.meta", seg2.metaFilename)
	s.NotNil(seg2.meta) // since segment is not Archived
	s.Equal(false, seg.Archived)

	// read the entries
	for i := 0; i < 6; i++ {
		entry, err := seg2.read(int64(i + 5))
		s.NoError(err)
		s.Equal(Entry("hello world"+strconv.Itoa(i+4)), entry)
	}
}

func (s *segmentTestSuite) TestSegment_truncate1() {
	// write 10 entries to segment 1, then archive it and write 10 more entries to segment 2
	seg1, err := newSegment(s.root, 1, 1)
	s.NoError(err)
	for i := 0; i < 10; i++ {
		_, err = seg1.write(Entry("hello world" + strconv.Itoa(i)))
		s.NoError(err)
	}
	err = seg1.archive()
	s.NoError(err)

	// read from WAL file
	seg3, err3 := readSegment(s.root, segmentFile("", 1))
	s.NoError(err3)
	_ = seg3

	// now truncate 12, we expected segment 1 to be truncated totally
	// and segment 2 to be truncated partially
	err = seg3.truncate(12)
	s.NoError(err)
	s.Equal(seg3.Truncated, seg3.End)

	// read from WAL file segment 1, should be empty
	_, err31 := readSegment(s.root, segmentFile("", 1))
	s.Error(err31)
}

func (s *segmentTestSuite) TestSegment_truncate2() {
	seg, err := newSegment(s.root, 2, 11)
	s.NoError(err)
	for i := 0; i < 10; i++ {
		_, err = seg.write(Entry("hello world" + strconv.Itoa(i)))
		s.NoError(err)
	}
	err = seg.archive()
	s.NoError(err)

	// seg2 equal to seg
	seg2, err2 := readSegment(s.root, segmentFile("", 2))
	s.Require().NoError(err2)
	err = seg2.truncate(12)
	s.Require().NoError(err)

	seg3, err3 := readSegment(s.root, segmentFile("", 2))
	s.Require().NoError(err3)
	s.Equal(seg2.buf, seg3.buf)
	// s.NotEqual(seg2.entryPos, seg3.entryPos)
	s.Equal(seg2.Truncated, seg3.Truncated)
	s.Equal(seg2.Start, seg3.Start)
	s.Equal(seg2.End, seg3.End)
	s.Equal(seg2.Index, seg3.Index)
	s.Equal(seg2.Archived, seg3.Archived)
	s.Equal(seg2.entryFilename, seg3.entryFilename)
	s.Equal(seg2.metaFilename, seg3.metaFilename)

	s.Equal(uint32(2), seg3.Index)
	s.Equal(int64(12), seg3.Start)
	s.Equal(int64(20), seg3.End)
	s.Equal(int64(11), seg3.Truncated)
	s.Equal(9, len(seg3.entryPos))
	s.Equal("testdata/wal/0000000002.wal", seg3.entryFilename)
	s.Nil(seg3.entry) // since segment is Archived
	s.Equal("testdata/wal/0000000002.wal.meta", seg3.metaFilename)
	s.Nil(seg3.meta) // since segment is not Archived
	s.Equal(true, seg3.Archived)

	// check the entries
	for i := 0; i < 9; i++ {
		entry, err := seg3.read(int64(i + 12))
		s.NoError(err)
		s.Equal(Entry("hello world"+strconv.Itoa(i+1)), entry)
	}
}

func Test_Segment(t *testing.T) {
	suite.Run(t, new(segmentTestSuite))
}
