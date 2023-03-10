package wal

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

const (
	segmentFileSuffix     = ".wal"
	segmentMetaFileSuffix = ".wal.meta"
)

// segment is the unit of WAL, it contains a entry file and a meta file.
// The entry file is for storing the entries, and the meta file is for storing
// the metadata of the segment.
//
// The entry file would not be deleted unless the segment is archived and all entries
// are marked as truncated.
type segment struct {
	segmentMeta

	buf          []byte
	entryPos     []entryPosition
	entryFlushed int // the last flushed offset of the entry file

	// only current segment has the following fields
	root          string   // root directory of the WAL
	entryFilename string   // name of the entry file
	entry         *os.File // file for storing the entries
	metaFilename  string   // name of the metadata file
	meta          *os.File // file for storing the metadata of the segment
}

type segmentMeta struct {
	Index uint32 `json:"index"` // Index of the segment file

	Archived bool `json:"archived"` // whether the segment is Archived (oversize)

	Start     int64 `json:"start"`     // Start offset of the entries in WAL
	End       int64 `json:"end"`       // End offset of the entries in WAL
	Truncated int64 `json:"truncated"` // Truncated offset of the entries in WAL
}

func (m *segmentMeta) canWrite() (bool, error) {
	can := !m.Archived
	if !can {
		return false, errors.Wrapf(ErrSegmentArchived,
			"segment(%d) is archived, can not write", m.Index)
	}

	return true, nil
}

func (m *segmentMeta) canRead(offset int64) (bool, error) {
	if offset < m.Start || offset > m.End {
		return false, errors.Wrapf(ErrSegmentInvalidOffset,
			"offset(%d) not in range(%d, %d)", offset, m.Start, m.End)
	}

	if offset <= m.Truncated {
		return false, errors.Wrapf(ErrSegmentInvalidOffset, "offset(%d) is truncated", offset)
	}

	return true, nil
}

func newSegment(root string, index uint32, start int64) (*segment, error) {
	seg := &segment{
		segmentMeta: segmentMeta{
			Start:     start,
			Archived:  false,
			End:       start - 1,
			Index:     index,
			Truncated: -1,
		},

		buf:          make([]byte, 0, 1024),
		entryPos:     make([]entryPosition, 0, 256),
		entryFlushed: 0,

		root:          root,
		entryFilename: segmentFile(root, int(index)),
		entry:         nil,
		metaFilename:  segmentMetaFile(root, int(index)),
		meta:          nil,
	}

	err := seg.openFiles()
	if err != nil {
		return nil, err
	}

	return seg, nil
}

func (s *segment) openFiles() error {
	var err error

	s.entry, err = os.OpenFile(s.entryFilename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	s.meta, err = os.OpenFile(s.metaFilename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	return nil
}

func (s *segment) closeFiles() error {
	if s.entry != nil {
		_ = s.entry.Close()
		s.entry = nil
	}
	if s.meta != nil {
		_ = s.meta.Close()
		s.meta = nil
	}

	return nil
}

// archive closes the segment files, it can be called only once,
// while segment is current segment, it will be called when a new segment is created.
func (s *segment) archive() error {
	if s.Archived {
		return nil
	}

	if err := s.flush(true); err != nil {
		return err
	}

	return s.closeFiles()
}

// markTruncated marks the segment as truncated to the given offset.
func (s *segment) markTruncated(offset int64) (removed bool, err error) {
	if offset < s.Start {
		return false, nil
	}

	s.Truncated = offset
	removed = offset >= s.End

	//if offset >= s.End {
	//	err = s.safelyRemove()
	//	return true, err
	//}

	//truncated := false
	//// refresh the segment meta and entry buffer, and then flush them to disk
	//if offset > s.Start {
	//	if offset >= s.End {
	//		if s.Archived {
	//			// archived truncated, remove the segment files directly
	//			s.entryPos = s.entryPos[:0]
	//			s.buf = s.buf[:0]
	//			s.Start = s.End + 1
	//			s.entryFlushed = 0
	//			return s.safelyRemove()
	//		}
	//
	//		// not archived
	//		offset = s.End
	//	}
	//
	//	// partially truncated, we need to markTruncated the segment files
	//	// DOESN'T include the truncated entry.
	//	posIdx := offset - s.Start
	//	if posIdx >= int64(len(s.entryPos)) {
	//		errmsg := fmt.Sprintf("markTruncated(%d) error: range(%d, %d) len(%d) \n", offset, s.Start, s.End, len(s.entryPos))
	//		fmt.Println(errmsg)
	//		return fmt.Errorf(errmsg)
	//	}
	//
	//	pos := s.entryPos[posIdx]
	//	s.buf = s.buf[pos.end:]
	//
	//	// reset the entry positions
	//	s.entryPos = s.entryPos[posIdx+1:]
	//	for idx, p := range s.entryPos {
	//		s.entryPos[idx].offset = p.offset - pos.end
	//		s.entryPos[idx].end = p.end - pos.end
	//	}
	//
	//	// reset segment meta (start, truncated)
	//	s.Start = offset + 1
	//
	//	truncated = true
	//}
	//
	//// flush the segment files to disk
	//return s.flush(truncated)

	err = s.flush(false)
	return removed, err
}

func (s *segment) flush(newArchived bool) error {
	// if the segment is truncated to the end AND archived,
	// remove the segment files directly.
	if s.Archived || newArchived {
		if s.Truncated >= s.End {
			return s.safelyRemove()
		}
	}

	// if the segment is archived before, only flush the meta file
	if s.Archived {
		// only flush the meta file
		return s.flushMeta()
	}

	if err := s.flushEntries(); err != nil {
		return err
	}
	s.Archived = newArchived
	if err := s.flushMeta(); err != nil {
		return err
	}

	return nil
}

func (s *segment) safelyRemove() error {
	_ = s.closeFiles()

	if err := os.Remove(s.entryFilename); err != nil {
		return err
	}
	if err := os.Remove(s.metaFilename); err != nil {
		return err
	}

	return nil
}

func (s *segment) flushEntries() error {
	if s.Archived {
		return nil
	}

	_, err := s.entry.Write(s.buf[s.entryFlushed:])
	if err != nil {
		return err
	}
	s.entryFlushed = len(s.buf)

	// entry and meta files are opened, we need to flush them to disk and close them
	if err := s.entry.Sync(); err != nil {
		defaultLogger.Log("segment flush entryFile(%s) error: %v", s.entryFilename, err)
		return err
	}

	return nil
}

func (s *segment) flushMeta() error {
	data, err := json.Marshal(s.segmentMeta)
	if err != nil {
		return err
	}

	if s.meta != nil {
		_ = s.meta.Truncate(0)
		_, _ = s.meta.Seek(0, io.SeekStart)
		_, err = s.meta.Write(data)

		if err := s.meta.Sync(); err != nil {
			defaultLogger.Log("segment flush metaFile(%s) error: %v", s.metaFilename, err)
		}

		return err
	}

	return os.WriteFile(s.metaFilename, data, 0644)
}

func (s *segment) read(offset int64) (entry Entry, err error) {
	if ok, err1 := s.canRead(offset); !ok {
		return nil, err1
	}

	posIdx := offset - s.Start
	pos := s.entryPos[posIdx]

	data := s.buf[pos.offset:pos.end]
	entry, err = readBinary(data)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func readBinary(data []byte) (Entry, error) {
	n := len(data)
	if n < __EntryLenSize {
		return nil, fmt.Errorf("invalid entry: too short: %d", n)
	}

	entryLen := binary.BigEndian.Uint16(data)
	if int(entryLen) != n-__EntryLenSize {
		return nil, fmt.Errorf("invalid entry: incorrect entryLen(%d) and data(%d)-2", entryLen, n)
	}

	return data[__EntryLenSize:], nil
}

func writeBinary(entry Entry) []byte {
	buf := make([]byte, __EntryLenSize, __EntryLenSize+len(entry))
	binary.BigEndian.PutUint16(buf, uint16(len(entry)))
	buf = append(buf, entry...)

	return buf
}

func (s *segment) write(entry Entry) (offset int64, err error) {
	if can, err1 := s.canWrite(); !can {
		return -1, err1
	}

	// encode the entry, and append it to the buffer
	buf := writeBinary(entry)
	pos := entryPosition{
		offset: len(s.buf),
		end:    len(s.buf) + len(buf),
	}
	s.buf = append(s.buf, buf...)
	s.End += 1
	s.entryPos = append(s.entryPos, pos)

	return s.End, nil
}

func (s *segment) size() int {
	return len(s.buf)
}

func segmentFile(root string, idx int) string {
	return segmentFilePrefix(root, idx) + segmentFileSuffix
}

func segmentMetaFile(root string, idx int) string {
	return segmentFilePrefix(root, idx) + segmentMetaFileSuffix
}

// segmentIndexFromName returns the Index of the segment file.
// The segment file name must be in the format of %010d.wal.
func segmentIndexFromName(name string) (int, error) {
	if !isSegmentFile(name) {
		return 0, fmt.Errorf("invalid segment file name: %s", name)
	}

	name = filepath.Base(name)
	name = strings.TrimSuffix(name, segmentFileSuffix)

	return strconv.Atoi(name)
}

func segmentFilePrefix(root string, idx int) string {
	return filepath.Join(root, fmt.Sprintf("%010d", idx))
}

// readSegment reads the segment meta file and returns a segment.
// The segment file must be in the format of %010d.wal.
func readSegment(root string, name string) (*segment, error) {
	index, err := segmentIndexFromName(name)
	if err != nil {
		return nil, err
	}

	// read the segment meta file
	meta, err := readSegmentMeta(segmentMetaFile(root, index))
	if err != nil {
		return nil, err
	}

	// read entries from the segment file
	data, err := os.ReadFile(filepath.Join(root, name))
	if err != nil {
		return nil, err
	}

	seg := &segment{
		segmentMeta: *meta,

		buf:          data,
		entryPos:     make([]entryPosition, 0, 256),
		entryFlushed: len(data),

		root:          root,
		entryFilename: segmentFile(root, index),
		entry:         nil,
		metaFilename:  segmentMetaFile(root, index),
		meta:          nil,
	}

	var (
		offset int
		n      = len(data)
	)

	for offset < n {
		if n-offset < __EntryLenSize {
			defaultLogger.Log("size(%d) of left data(%s) is too short", n-offset, data[offset:])
			return nil, errors.Wrapf(ErrSegmentFileMess,
				"size(%d) of left data(%s) is too short", n-offset, data[offset:])
		}

		// read the entry length
		entryLen := binary.BigEndian.Uint16(data[offset:])
		next := offset + int(entryLen) + __EntryLenSize
		if next > n {
			return nil, errors.Wrapf(ErrSegmentFileMess, "invalid entry length(%d)", entryLen)
		}

		pos := entryPosition{
			offset: offset,
			end:    next,
		}
		seg.entryPos = append(seg.entryPos, pos)

		// update the offset
		offset = next
	}

	// compare the entry count and the entry position count
	bufLen := len(seg.buf)
	entryPosNum := len(seg.entryPos)
	if entryNum := seg.End - seg.Start + 1; entryPosNum != int(entryNum) {
		defaultLogger.Log(
			"invalid entry count(%d) [%d:%d] and entryPos count(%d)", entryNum, seg.End, seg.Start, entryPosNum)
		return nil, errors.Wrapf(ErrSegmentFileMess,
			"invalid entry count(%d) and entryPos count(%d)", entryNum, entryPosNum)
	}

	// compare buf size and entry position end
	last := len(seg.entryPos) - 1
	lastEnd := seg.entryPos[last].end
	if lastEnd != bufLen {
		defaultLogger.Log(
			"invalid buf size(%d) and last(%d) entry end(%d)", bufLen, last, lastEnd)
		return nil, errors.Wrapf(ErrSegmentFileMess,
			"invalid buf size(%d) and last(%d) end(%d)", bufLen, last, lastEnd)
	}

	if !seg.Archived {
		err := seg.openFiles()
		if err != nil {
			return nil, err
		}
	}

	return seg, nil
}

// readSegmentMeta reads the segment meta file and returns the segment meta.
// The segment meta file must be in the format of %010d.wal.meta.
func readSegmentMeta(name string) (*segmentMeta, error) {
	data, err := os.ReadFile(name)
	if err != nil {
		return nil, err
	}

	meta := &segmentMeta{}
	if err = json.Unmarshal(data, meta); err != nil {
		return nil, err
	}

	return meta, nil
}

func isSegmentFile(name string) bool {
	return strings.HasSuffix(name, segmentFileSuffix)
}
