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

func Test_Segment(t *testing.T) {
	suite.Run(t, new(segmentTestSuite))
}
