package wal

// Entry is the data type that is stored in the WAL.
// NOTE: each entry must be less than 2^16 Byte(64KB).
type Entry []byte

const __EntryLenSize = 2 // 2 bytes

// entry bytes = buf[offset:End]
type entryPosition struct {
	offset int
	end    int // end offset of the entry in the segment buffer
}
