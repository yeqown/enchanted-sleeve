package wal

import (
	"errors"
	"os"
	"sort"
)

var _ __WALSpec = (*WAL)(nil)

type __WALSpec interface {
	Close() error // closes the WAL and all underlying files
	Flush() error // flushes all data to disk

	Write(entry Entry) (offset int64, err error)        // returns the offset of the entry
	Read(offset int64) (entry Entry, err error)         // returns the entry at the given offset
	ReadLatest() (entry Entry, offset int64, err error) // same to Read(-1)

	TruncateBefore(offset int64) error // removes all entries before the given offset
	// TruncateAfter(offset int64) error  // removes all entries after the given offset
}

// WAL is a write-ahead log for storing data that needs to be persisted to disk.
type WAL struct {
	*Config

	segments          []*segment
	current           *segment
	currentSegmentIdx uint32

	entryOffset int64
}

func NewWAL(config *Config, options ...OptionWAL) (*WAL, error) {
	if config == nil {
		config = DefaultConfig()
	}

	for _, o := range options {
		o.apply(config)
	}

	w := &WAL{
		Config: config,

		segments:          make([]*segment, 0, config.MaxSegments),
		current:           nil,
		currentSegmentIdx: 0,

		entryOffset: 0,
	}

	err := w.restore()
	if err != nil {
		return nil, err
	}

	return w, nil
}

// restore restores the WAL from the underlying files.
// This method should be called after the WAL is created.
func (w *WAL) restore() error {
	if w.Root == "" {
		return errors.New("root directory is not set")
	}

	// if the root directory does not exist, create it
	if _, err := os.Stat(w.Root); os.IsNotExist(err) {
		err := os.MkdirAll(w.Root, 0755)
		if err != nil {
			return err
		}
	}

	// exists the root directory, restore the WAL from the underlying files
	// read all files in the root directory
	files, err := os.ReadDir(w.Root)
	if err != nil {
		return err
	}

	// iterate all files in the root directory
	for _, file := range files {
		// skip non-segment files
		if !file.IsDir() && !isSegmentFile(file.Name()) {
			continue
		}

		// read the segment meta file
		seg, err := readSegment(w.Root, file.Name())
		if err != nil {
			return err
		}

		// append the segment to the list of segments
		w.segments = append(w.segments, seg)
	}

	// if there is no segment file, create a new segment
	if len(w.segments) == 0 {
		// create a new segment file, and set it as the current segment
		return w.applySegment()
	}

	// sort the segments by Index
	sort.Slice(w.segments, func(i, j int) bool {
		return w.segments[i].Index < w.segments[j].Index
	})
	// set the current segment
	w.current = w.segments[len(w.segments)-1]
	w.currentSegmentIdx = w.current.Index
	w.entryOffset = w.current.End

	// if the maximum number of segments is reached, release the oldest seg
	for len(w.segments) > w.MaxSegments {
		w.releaseSegment(0)
	}

	return nil
}

// applySegment applies a new segment to the WAL.
func (w *WAL) applySegment() error {
	w.currentSegmentIdx += 1
	seg, err := newSegment(w.Root, w.currentSegmentIdx, w.entryOffset+1)
	if err != nil {
		return err
	}

	if w.current != nil {
		if err := w.current.archive(); err != nil {
			return err
		}
	}

	w.segments = append(w.segments, seg)
	w.current = seg

	// if the maximum number of segments is reached, release the oldest seg
	if len(w.segments) > w.MaxSegments {
		w.releaseSegment(0)
	}

	return nil
}

func (w *WAL) releaseSegment(index int) {
	seg := w.segments[index]

	// remove the segment from the list of segments
	w.segments = append(w.segments[:index], w.segments[index+1:]...)

	// if the segment is the current segment, set the current segment to nil
	if w.current == w.segments[index] {
		w.current = nil
	}

	_ = seg.safelyRemove()
}

func (w *WAL) Close() error {
	err := w.Flush()
	if err != nil {
		return err
	}

	// TODO: close all segments
	//for _, seg := range w.segments {
	//	err := seg.close()
	//	if err != nil {
	//		return err
	//	}
	//}

	return nil
}

// Flush loop through all segments, and sync them to disk.
// if segment is not nil, sync it (if it's Truncated, it should be deleted, otherwise it should be Archived)
func (w *WAL) Flush() error {
	for _, seg := range w.segments {
		if seg == nil {
			continue
		}

		err := seg.sync()
		if err != nil {
			return err
		}
	}

	return nil
}

// Write writes an entry to the WAL.
func (w *WAL) Write(entry Entry) (offset int64, err error) {
	if w.current == nil {
		err := w.applySegment()
		if err != nil {
			return 0, err
		}
	}

	// write the entry to the current segment
	offset, err = w.current.write(entry)
	w.entryOffset = offset

	// if the current segment is full, apply a new segment
	if int64(w.current.size()) >= w.MaxSegmentSize {
		err := w.applySegment()
		if err != nil {
			return offset, err
		}
	}

	return offset, err
}

func (w *WAL) locateSegment(offset int64) (*segment, error) {
	// locate the segment that contains the entry, binary search
	//segIdx := sort.Search(len(w.segments), func(i int) bool {
	//	return w.segments[i].Start > offset
	//})

	// locate the segment that contains the entry, binary search
	segIdx := sort.Search(len(w.segments), func(i int) bool {
		return w.segments[i].End >= offset
	})

	if segIdx < len(w.segments) {
		seg := w.segments[segIdx]
		if seg.Start <= offset && offset <= seg.End {
			return seg, nil
		}
	}

	return nil, ErrSegmentNotFound
}

func (w *WAL) Read(offset int64) (entry Entry, err error) {
	if offset < 0 {
		offset = w.entryOffset
	}

	seg, err := w.locateSegment(offset)
	if err != nil {
		if errors.Is(err, ErrSegmentNotFound) {
			return nil, ErrEntryNotFound
		}
		return nil, err
	}

	// read the entry from the segment
	if entry, err = seg.read(offset); err != nil {
		if errors.Is(err, ErrSegmentInvalidOffset) {
			return nil, ErrEntryNotFound
		}
		return nil, err
	}

	return entry, nil
}

// ReadLatest reads the latest entry from the WAL.
// Same as Read(-1) but returns the offset of the entry.
func (w *WAL) ReadLatest() (entry Entry, offset int64, err error) {
	entry, err = w.Read(w.entryOffset)
	if err != nil {
		return nil, 0, err
	}

	return entry, w.entryOffset, nil
}

func (w *WAL) TruncateBefore(offset int64) error {
	seg, err := w.locateSegment(offset)
	if err != nil {
		return err
	}

	// loop all segments before the located segment, including the located segment
	for _, s := range w.segments {
		if s.Index > seg.Index {
			continue
		}

		// mark the segment.Truncated the max offset in segment,
		// so that the segment can be released when the WAL is flushed
		if err = s.truncate(offset); err != nil {
			return err
		}
	}

	return nil
}
