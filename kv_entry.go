package esl

import (
	"encoding/binary"
	"hash/crc32"
	"sync"
	"time"
)

const (
	kvEntry_fixedBytes     = 12
	kvEntry_tsTimestampOff = 4
	kvEntry_keySizeOff     = kvEntry_tsTimestampOff + 4
	kvEntry_valueSizeOff   = kvEntry_keySizeOff + 2
	kvEntry_keyOff         = kvEntry_valueSizeOff + 2
)

// kvEntry is a single key value pair in an ESL file.
type kvEntry struct {
	crc         uint32
	tsTimestamp uint32 // 32 bit timestamp, internal use only
	keySize     uint16 // key size in bytes, max 1024 bytes
	valueSize   uint16 // value size in bytes
	key         []byte
	value       []byte
}

// _checksumEntry avoid using this function since it allocates a new slice
// to store the data. and it is not efficient.
func _checksumEntry(ent *kvEntry) uint32 {
	data := make([]byte, kvEntry_fixedBytes-kvEntry_tsTimestampOff+ent.keySize+ent.valueSize)
	pos := 0
	binary.BigEndian.PutUint32(data, ent.tsTimestamp)
	pos += 4
	binary.BigEndian.PutUint16(data[pos:], ent.keySize)
	pos += 2
	binary.BigEndian.PutUint16(data[pos:], ent.valueSize)
	pos += 2
	copy(data[pos:], ent.key)
	pos += int(ent.keySize)
	copy(data[pos:], ent.value)

	return crc32.ChecksumIEEE(data)
}

func _checksumRaw(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// func (ent *kvEntry) fillcrc() {
// 	if ent == nil {
// 		panic("fillcrc on nil ent")
// 	}
//
// 	ent.crc = checksum(ent)
// }

func (ent *kvEntry) validateChecksum() bool {
	if ent == nil {
		return false
	}

	return ent.crc == _checksumEntry(ent)
}

func (ent *kvEntry) bytes() []byte {
	data := make([]byte, len(ent.key)+len(ent.value)+kvEntry_fixedBytes)

	// binary.BigEndian.PutUint32(data, ent.crc)

	binary.BigEndian.PutUint32(data[kvEntry_tsTimestampOff:], ent.tsTimestamp)
	binary.BigEndian.PutUint16(data[kvEntry_keySizeOff:], ent.keySize)
	binary.BigEndian.PutUint16(data[kvEntry_valueSizeOff:], ent.valueSize)
	copy(data[kvEntry_keyOff:], ent.key)
	copy(data[kvEntry_keyOff+ent.keySize:], ent.value)

	// fill crc at last.
	ent.crc = _checksumRaw(data[kvEntry_tsTimestampOff:])
	binary.BigEndian.PutUint32(data, ent.crc)

	return data
}

// tombstone indicates the kvEntry contains a tombstone value.
func (ent *kvEntry) tombstone() bool {
	return ent.value == nil
}

var (
	keyEntryPool = sync.Pool{
		New: func() interface{} {
			return &kvEntry{
				crc:         0,
				tsTimestamp: uint32(time.Now().Unix()),
				keySize:     0,
				valueSize:   0,
				key:         nil,
				value:       nil,
			}
		},
	}
)

func newEntry(key, value []byte) *kvEntry {
	ent := keyEntryPool.Get().(*kvEntry)

	ent.crc = 0
	ent.tsTimestamp = uint32(time.Now().Unix())
	ent.keySize = uint16(len(key))
	ent.valueSize = uint16(len(value))
	ent.key = key
	ent.value = value

	return ent
}

func releaseEntry(ent *kvEntry) {
	ent.crc = 0
	ent.tsTimestamp = 0
	ent.keySize = 0
	ent.valueSize = 0
	ent.key = nil
	ent.value = nil

	keyEntryPool.Put(ent)
}

func decodeEntryFromHeader(header []byte) (*kvEntry, error) {
	if len(header) < kvEntry_fixedBytes {
		return nil, ErrInvalidEntryHeader
	}

	ent := &kvEntry{
		crc:         binary.BigEndian.Uint32(header),
		tsTimestamp: binary.BigEndian.Uint32(header[kvEntry_tsTimestampOff:]),
		keySize:     binary.BigEndian.Uint16(header[kvEntry_keySizeOff:]),
		valueSize:   binary.BigEndian.Uint16(header[kvEntry_valueSizeOff:]),
		key:         nil,
		value:       nil,
	}

	ent.key = make([]byte, ent.keySize)
	ent.value = make([]byte, ent.valueSize)

	return ent, nil
}

const entrySizeAssumption = 30

// estimateEntry returns the number of entries that can be stored in the given
// number of bytes. We assume the average key size is 10 bytes and the average
// value size is 20 bytes.
func estimateEntry(bytes int64) int {
	if bytes < entrySizeAssumption {
		return 0
	}

	return int(bytes/entrySizeAssumption) + 1
}
