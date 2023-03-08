package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
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

	buf      []byte
	entryPos []entryPosition

	// only current segment has the following fields
	root          string   // root directory of the WAL
	entryFilename string   // name of the entry file
	entry         *os.File // file for storing the entries
	metaFilename  string   // name of the metadata file
	meta          *os.File // file for storing the metadata of the segment
}

type segmentMeta struct {
	Index uint32 // Index of the segment file

	Archived bool // whether the segment is Archived (oversize)

	Start     int64 // Start offset of the entries in WAL
	End       int64 // End offset of the entries in WAL
	Truncated int64 // Truncated offset in the segment file (Start <= Truncated <= End)
}

func newSegment(root string, index uint32, start int64) (*segment, error) {
	seg := &segment{
		segmentMeta: segmentMeta{
			Start:     start,
			End:       start - 1,
			Truncated: start - 1,
			Index:     index,
		},

		buf: make([]byte, 0, 1024),

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
	// FIXME: 初次打包和后续打包的区分
	s.Archived = true
	if err := s.sync(); err != nil {
		return err
	}

	if err := s.entry.Close(); err != nil {
		return err
	}
	s.entry = nil

	if err := s.meta.Close(); err != nil {
		return err
	}
	s.meta = nil

	return nil
}

// sync flushes the segment files to disk.
// If segment contains no entry, it will be removed, otherwise we
// flush the segment files to disk, and update the metadata of the segment.
//
// Archived indicates whether the segment is Archived, if it is not Archived,
// we will only update the metadata of the segment.
func (s *segment) sync() error {
	// if segment is Truncated, we need to remove the entry file and meta file
	if s.isTruncated() {
		if err := os.Remove(s.entryFilename); err != nil {
			return err
		}
		if err := os.Remove(s.metaFilename); err != nil {
			return err
		}
		return nil
	}

	// not Truncated, we need to flush the segment files to disk
	if err := s.flushEntries(); err != nil {
		return err
	}
	if err := s.flushMeta(); err != nil {
		return err
	}

	// if segment is not Archived yet, we need to close the segment files
	if !s.Archived || (s.entry != nil && s.meta != nil) {
		// entry and meta files are opened, we need to sync them to disk and close them
		if err := s.entry.Sync(); err != nil {
			return err
		}
		if err := s.meta.Sync(); err != nil {
			return err
		}
	}

	return nil
}

func (s *segment) flushEntries() error {
	if s.Archived && s.entry == nil {
		return nil
	}

	if s.entry == nil {
		return fmt.Errorf("entry file is not opened")
	}

	_, err := s.entry.Write(s.buf)
	return err
}

func (s *segment) flushMeta() error {
	buf := bytes.NewBuffer(nil)
	err := gob.NewEncoder(buf).Encode(s.segmentMeta)
	if err != nil {
		return err
	}

	if !s.Archived {
		if s.meta == nil {
			return fmt.Errorf("meta file is not opened")
		}

		// update the metadata of the segment
		_, err = s.meta.Write(buf.Bytes())
		return err
	}

	return os.WriteFile(s.metaFilename, buf.Bytes(), 0644)
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

// isTruncated indicates how many offset entries was Truncated.
func (s *segment) isTruncated() bool {
	return s.Truncated >= s.Start
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
		segmentMeta:   *meta,
		buf:           data,
		entryPos:      make([]entryPosition, 0, 256),
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
	err = gob.NewDecoder(bytes.NewBuffer(data)).Decode(meta)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func isSegmentFile(name string) bool {
	return strings.HasSuffix(name, segmentFileSuffix)
}
