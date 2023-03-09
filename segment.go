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
)

const (
	segmentFileSuffix     = ".wal"
	segmentMetaFileSuffix = ".wal.meta"
)

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

func newSegment(root string, index uint32, start int64) (*segment, error) {
	seg := &segment{
		segmentMeta: segmentMeta{
			Start:     start,
			Archived:  false,
			End:       start - 1,
			Truncated: -1,
			Index:     index,
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

// archive closes the segment files, it can be called only once,
// while segment is current segment, it will be called when a new segment is created.
func (s *segment) archive() error {
	if s.Archived {
		return nil
	}

	s.Archived = true
	if err := s.sync(); err != nil {
		return err
	}

	// close the segment files
	if s.entry != nil {
		if err := s.entry.Close(); err != nil {
			return err
		}
		s.entry = nil
	}
	if s.meta != nil {
		if err := s.meta.Close(); err != nil {
			return err
		}
		s.meta = nil
	}

	return nil
}

// truncate truncates the segment file to the given offset.
// If offset is not in the range of the segment, nothing will be done, otherwise
// we mark the max offset of the segment as the given offset, and truncate the segment file.
func (s *segment) truncate(offset int64) error {
	if offset < s.Start {
		return nil
	}

	s.Truncated = offset

	return s.sync()
}

// sync flushes the segment files to disk.
// If segment contains no entry, it will be removed, otherwise we
// flush the segment files to disk, and update the metadata of the segment.
//
// hasArchived indicates whether the segment has been Archived, if it is not archived,
// we will only update the metadata of the segment.
func (s *segment) sync() error {
	entryChanged := false
	if s.Truncated > s.Start {
		// totally truncated, remove the segment files
		if s.Truncated >= s.End {
			s.Start = s.End
			return s.safelyRemove()
		}
		// partially truncated, we need to truncate the segment files
		// DOESN'T include the truncated entry.
		posIdx := s.Truncated - s.Start
		if posIdx >= int64(len(s.entryPos)) {
			errmsg := fmt.Sprintf("truncate(%d) error: range(%d, %d) len(%d) \n", s.Truncated, s.Start, s.End, len(s.entryPos))
			fmt.Println(errmsg)
			return fmt.Errorf(errmsg)
		}
		pos := s.entryPos[posIdx]
		s.buf = s.buf[pos.offset:]
		// reset the entry positions
		s.entryPos = s.entryPos[posIdx:]
		// reset segment meta (start, truncated)
		s.Start = s.Truncated
		entryChanged = true
	}

	// not Truncated, we need to flush the segment files to disk
	if err := s.flushEntries(entryChanged); err != nil {
		return err
	}
	if err := s.flushMeta(); err != nil {
		return err
	}

	// if segment is not Archived yet, we need to close the segment files
	if s.entry != nil {
		// entry and meta files are opened, we need to sync them to disk and close them
		if err := s.entry.Sync(); err != nil {
			return err
		}
	}
	if s.meta != nil {
		if err := s.meta.Sync(); err != nil {
			return err
		}
	}

	return nil
}

func (s *segment) safelyRemove() error {
	if s.entry != nil {
		_ = s.entry.Close()
	}
	if s.meta != nil {
		_ = s.meta.Close()
	}

	if err := os.Remove(s.entryFilename); err != nil {
		return err
	}
	if err := os.Remove(s.metaFilename); err != nil {
		return err
	}
	return nil
}

func (s *segment) flushEntries(fullWrite bool) error {
	// archived and not changed, nothing to do
	if s.entry == nil && !fullWrite {
		// archived segment
		return nil
	}

	if s.entry != nil && fullWrite {
		// we need to truncate the segment file
		if err := s.entry.Truncate(0); err != nil {
			return err
		}
		if _, err := s.entry.Seek(0, io.SeekStart); err != nil {
			return err
		}

		_, err := s.entry.Write(s.buf)
		return err
	}

	if s.entry == nil && fullWrite {
		return os.WriteFile(s.entryFilename, s.buf, 0644)
	}

	// s.entry != nil && !fullWrite
	// incremental write
	_, err := s.entry.Write(s.buf[s.entryFlushed:])
	s.entryFlushed = len(s.buf)
	return err
}

func (s *segment) flushMeta() error {
	data, err := json.Marshal(s.segmentMeta)
	if err != nil {
		return err
	}

	if s.meta != nil {
		// update the metadata of the segment
		err = s.meta.Truncate(0)
		if err != nil {
			return err
		}

		_, err = s.meta.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}

		_, err = s.meta.Write(data)
		return err
	}

	return os.WriteFile(s.metaFilename, data, 0644)
}

func (s *segment) read(offset int64) (entry Entry, err error) {
	if offset < s.Start || offset > s.End {
		return nil, fmt.Errorf("invalid offset: %d", offset)
	}

	posIdx := offset - s.Start
	pos := s.entryPos[posIdx]

	data := s.buf[pos.offset:pos.end]
	if err != nil {
		return nil, err
	}
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
			panic("invalid entry, data mess")
		}

		// read the entry length
		entryLen := binary.BigEndian.Uint16(data)
		next := offset + int(entryLen) + __EntryLenSize
		if next > n {
			panic("invalid entry, data mess")
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
	if len(seg.entryPos) != int(seg.End-seg.Start+1) {
		return nil, fmt.Errorf("invalid entry, data mess")
	}
	// compare buf size and entry position end
	if seg.entryPos[len(seg.entryPos)-1].end != len(seg.buf) {
		return nil, fmt.Errorf("invalid entry, data mess")
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
