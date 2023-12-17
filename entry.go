package esl

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

const (
	kvEntry_crc_fixedBytes     = 8
	kvEntry_crc_tsTimestampOff = 0
	kvEntry_crc_keySizeOff     = kvEntry_crc_tsTimestampOff + 4
	kvEntry_crc_valueSizeOff   = kvEntry_crc_keySizeOff + 2
	kvEntry_crc_keyOff         = kvEntry_crc_valueSizeOff + 2

	kvEntry_bytes_fixedBytes     = 12
	kvEntry_bytes_tsTimestampOff = 4
	kvEntry_bytes_keySizeOff     = kvEntry_bytes_tsTimestampOff + 4
	kvEntry_bytes_valueSizeOff   = kvEntry_bytes_keySizeOff + 2
	kvEntry_bytes_keyOff         = kvEntry_bytes_valueSizeOff + 2
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

func (ent *kvEntry) checksum() {
	if ent == nil {
		panic("checksum on nil ent")
	}

	data := make([]byte, len(ent.key)+len(ent.value)+kvEntry_crc_fixedBytes)
	binary.BigEndian.PutUint32(data, ent.tsTimestamp)
	binary.BigEndian.PutUint16(data[kvEntry_crc_keySizeOff:], ent.keySize)
	binary.BigEndian.PutUint16(data[kvEntry_crc_valueSizeOff:], ent.valueSize)
	copy(data[kvEntry_crc_keyOff:], ent.key)
	copy(data[kvEntry_crc_keyOff+ent.keySize:], ent.value)

	ent.crc = crc32.ChecksumIEEE(data)
}

func (ent *kvEntry) bytes() []byte {
	data := make([]byte, len(ent.key)+len(ent.value)+kvEntry_bytes_fixedBytes)
	binary.BigEndian.PutUint32(data, ent.crc)
	binary.BigEndian.PutUint32(data[kvEntry_bytes_tsTimestampOff:], ent.tsTimestamp)
	binary.BigEndian.PutUint16(data[kvEntry_bytes_keySizeOff:], ent.keySize)
	binary.BigEndian.PutUint16(data[kvEntry_bytes_valueSizeOff:], ent.valueSize)
	copy(data[kvEntry_bytes_keyOff:], ent.key)
	copy(data[kvEntry_bytes_keyOff+ent.keySize:], ent.value)

	return data
}

// tombstone indicates the kvEntry contains a tombstone value.
func (ent *kvEntry) tombstone() bool {
	return ent.value == nil
}

func newEntry(key, value []byte) *kvEntry {
	ent := &kvEntry{
		crc:         0,
		tsTimestamp: uint32(time.Now().Unix()),
		keySize:     uint16(len(key)),
		valueSize:   uint16(len(value)),
		key:         key,
		value:       value,
	}

	ent.checksum()

	return ent
}

func decodeEntryFromHeader(header []byte) (*kvEntry, error) {
	if len(header) < kvEntry_bytes_fixedBytes {
		return nil, ErrInvalidEntryHeader
	}

	ent := &kvEntry{
		crc:         binary.BigEndian.Uint32(header),
		tsTimestamp: binary.BigEndian.Uint32(header[kvEntry_bytes_tsTimestampOff:]),
		keySize:     binary.BigEndian.Uint16(header[kvEntry_bytes_keySizeOff:]),
		valueSize:   binary.BigEndian.Uint16(header[kvEntry_bytes_valueSizeOff:]),
		key:         nil,
		value:       nil,
	}

	ent.key = make([]byte, ent.keySize)
	ent.value = make([]byte, ent.valueSize)

	return ent, nil
}

// keydirEntry is a single keydir entry in an ESL hash index structure.
type keydirEntry struct {
	fileId      uint16
	valueSize   uint16
	tsTimestamp uint32 // TODO: what's the purpose of tsTimestamp?
	entryOffset uint32 // uint32 is enough since maxDataFileSize is 100MB
	valueOffset uint32 // uint32 is enough since maxDataFileSize is 100MB
}

func (e keydirEntry) bytes() []byte {
	data := make([]byte, 16)
	binary.BigEndian.PutUint16(data, e.fileId)
	binary.BigEndian.PutUint16(data[2:], e.valueSize)
	binary.BigEndian.PutUint32(data[4:], e.tsTimestamp)
	binary.BigEndian.PutUint64(data[8:], uint64(e.valueOffset))
	binary.BigEndian.PutUint64(data[12:], uint64(e.entryOffset))

	return data
}

func (e keydirEntry) size() int {
	return 16
}
