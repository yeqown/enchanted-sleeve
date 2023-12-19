package esl

import (
	"encoding/binary"
	"sync"

	"github.com/pkg/errors"
)

const (
	keydirMem_Size       = 12
	keydirFile_fixedSize = keydirMem_Size + 2
)

// keydirMemEntry is a single keydir entry in an ESL hash index structure.
type keydirMemEntry struct {
	fileId      uint16
	valueSize   uint16
	entryOffset uint32
	valueOffset uint32 // uint32 is enough (about 4GB for a single file)
}

func (e keydirMemEntry) bytes() []byte {
	data := make([]byte, keydirMem_Size)
	binary.BigEndian.PutUint16(data, e.fileId)
	binary.BigEndian.PutUint16(data[2:], e.valueSize)
	binary.BigEndian.PutUint32(data[4:], e.entryOffset)
	binary.BigEndian.PutUint32(data[8:], e.valueOffset)

	return data
}

func decodeKeydirEntry(data []byte) (*keydirMemEntry, error) {
	if len(data) != keydirMem_Size {
		return nil, ErrInvalidKeydirData
	}

	keydir := &keydirMemEntry{
		fileId:      binary.BigEndian.Uint16(data[:2]),
		valueSize:   binary.BigEndian.Uint16(data[2:]),
		entryOffset: binary.BigEndian.Uint32(data[4:]),
		valueOffset: binary.BigEndian.Uint32(data[8:]),
	}

	return keydir, nil
}

func (e keydirMemEntry) size() int {
	return keydirMem_Size
}

// keydirMemTable is a map of keydir entries, the key is generic type T.
type keydirMemTable struct {
	lock    sync.RWMutex
	indexes map[string]*keydirMemEntry
}

func newKeyDir() *keydirMemTable {
	return &keydirMemTable{
		lock:    sync.RWMutex{},
		indexes: make(map[string]*keydirMemEntry, 1024),
	}
}

func (kd *keydirMemTable) len() int {
	kd.lock.RLock()
	defer kd.lock.RUnlock()

	return len(kd.indexes)
}

func (kd *keydirMemTable) get(key []byte) *keydirMemEntry {
	kd.lock.RLock()
	defer kd.lock.RUnlock()

	ent, ok := kd.indexes[string(key)]
	if ok {
		return ent
	}

	return nil
}

func (kd *keydirMemTable) set(key []byte, ent *keydirMemEntry) {
	kd.lock.Lock()
	defer kd.lock.Unlock()

	kd.indexes[string(key)] = ent
}

// func (kd *keydirMemTable) del(key []byte) {
// 	kd.lock.Lock()
// 	defer kd.lock.Unlock()
//
// 	delete(kd.indexes, string(key))
// }

type keydirFileEntry struct {
	keydirMemEntry

	keySize uint16
	key     []byte
}

func (e *keydirFileEntry) bytes() []byte {
	data := make([]byte, keydirFile_fixedSize+e.keySize)
	copy(data[:keydirMem_Size], e.keydirMemEntry.bytes())
	binary.BigEndian.PutUint16(data[keydirMem_Size:], e.keySize)
	copy(data[keydirFile_fixedSize:], e.key)

	return data
}

// decodeKeydirFileEntry read keydirFileEntry from data(fixed part only) and
// allocate key memory.
func decodeKeydirFileEntry(data []byte) (*keydirFileEntry, error) {
	if len(data) < keydirFile_fixedSize {
		return nil, ErrInvalidKeydirFileData
	}

	m, err := decodeKeydirEntry(data[:keydirMem_Size])
	if err != nil {
		return nil, errors.Wrap(err, "decodeKeydirFileEntry")
	}

	keydir := &keydirFileEntry{
		keydirMemEntry: *m,
		keySize:        binary.BigEndian.Uint16(data[keydirMem_Size:]),
		key:            nil,
	}

	keydir.key = make([]byte, keydir.keySize)

	return keydir, nil
}
