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

	TruncateBefore(offset int64) error // removes all entries before the given offset(included)
	// TruncateAfter(offset int64) error  // removes all entries after the given offset
}

// WAL is a write-ahead log for storing data that needs to be persisted to disk.
// FIXME: concurrent safety
type WAL struct {
	*Config

	segments          []*segment
	current           *segment
	currentSegmentIdx uint32

	latest int64 // the offset of the latest entry
	oldest int64 // the offset of the oldest entry
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

		latest: 0,
		oldest: 0,
	}

	if config.Logger != nil {
		defaultLogger = config.Logger
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
		return w.allocSegment()
	}

	// sort the segments by Index
	sort.Slice(w.segments, func(i, j int) bool {
		return w.segments[i].Index < w.segments[j].Index
	})

	lastSeg := w.segments[len(w.segments)-1]
	// if the last segment is not archived, set it as the current segment
	if w.current == nil && !lastSeg.Archived {
		w.current = lastSeg
	}
	w.currentSegmentIdx = lastSeg.Index
	w.latest = lastSeg.End
	w.oldest = max(w.segments[0].Start, w.segments[0].Truncated+1)

	return nil
}

// allocSegment applies a new segment to the WAL.segment list, and set it as the current segment.
// This method should be called after the WAL is created or a segment is fulled,
// and we need to apply a new segment to the WAL.
//
// This method will release the oldest segment (normally the segment in segments[0]) if
// the number of segments exceeds the maximum number of segments.
func (w *WAL) allocSegment() error {
	w.currentSegmentIdx += 1
	seg, err := newSegment(w.Root, w.currentSegmentIdx, w.latest+1)
	if err != nil {
		return err
	}

	if w.current != nil {
		if err := w.current.flush(true); err != nil {
			return err
		}
	}

	w.segments = append(w.segments, seg)
	w.current = seg
	// new segment flush immediately, since it's allocated.
	_ = w.current.flush(false)

	// if the maximum number of segments is reached, release the oldest seg
	for len(w.segments) > w.MaxSegments {
		w.releaseSegment(0)
	}

	return nil
}

func (w *WAL) releaseSegment(index int) {
	seg := w.segments[index]

	// if the segment is the current segment, set the current segment to nil
	if w.current.Index == seg.Index {
		w.current = nil
	}
	seg.safelyRemove()

	w.segments = append(w.segments[:index], w.segments[index+1:]...) // remove the segment from the list
	w.oldest = max(w.oldest, seg.End+1)                              // update the oldest offset
}

func (w *WAL) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}

	for _, seg := range w.segments {
		if err := seg.close(); err != nil {
			defaultLogger.Log("WAL.Close failed to close segment(%d) with error: %v", seg.Index, err)
		}
	}

	return nil
}

// Flush loop through all segments, and flush them to disk.
func (w *WAL) Flush() error {
	for _, seg := range w.segments {
		if seg == nil {
			continue
		}

		err := seg.flush(false)
		if err != nil {
			return err
		}
	}

	return nil
}

// Write writes an entry to the WAL.
func (w *WAL) Write(entry Entry) (offset int64, err error) {
	if w.current == nil {
		err := w.allocSegment()
		if err != nil {
			return 0, err
		}
	}

	// write the entry to the current segment
	offset, err = w.current.write(entry)
	if err != nil {
		return 0, err
	}
	w.latest = offset
	if w.oldest == 0 {
		w.oldest = offset
	}

	// if the current segment is full, apply a new segment
	if int64(w.current.size()) >= w.MaxSegmentSize {
		err := w.allocSegment()
		if err != nil {
			return offset, err
		}
	}

	return offset, err
}

// locateSegment finds the segment containing the given offset (binary search).
func (w *WAL) locateSegment(offset int64) (*segment, error) {
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
		offset = w.latest
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
	entry, err = w.Read(w.latest)
	if err != nil {
		return nil, 0, err
	}

	return entry, w.latest, nil
}

func (w *WAL) ReadOldest() (entry Entry, offset int64, err error) {
	entry, err = w.Read(w.oldest)
	if err != nil {
		return nil, 0, err
	}

	return entry, w.oldest, nil
}

func (w *WAL) TruncateBefore(offset int64) error {
	seg, err := w.locateSegment(offset)
	if err != nil && !errors.Is(err, ErrSegmentNotFound) {
		return err
	}

	if seg == nil {
		return nil
	}

	// move the oldest offset to the located segment
	w.oldest = max(offset+1, w.oldest)
	if w.oldest > w.latest {
		w.oldest = w.latest
	}

	// loop all segments before the located segment, including the located segment
	for index, s := range w.segments {
		if s.Index > seg.Index {
			continue
		}

		// mark the segment.Truncated the max offset in segment,
		// so that the segment can be released when the WAL is flushed
		shouldRemove, err := s.markTruncated(offset)
		if err != nil {
			return err
		}

		if shouldRemove {
			w.releaseSegment(index)
		}
	}

	return nil
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}

	return b
}
