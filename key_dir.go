package esl

import "sync"

// keyDirIndex is a map of keydir entries, the key is generic type T.
type keyDirIndex struct {
	lock    sync.RWMutex
	hashmap map[string]*keydirEntry
}

func newKeyDir() *keyDirIndex {
	return &keyDirIndex{
		lock:    sync.RWMutex{},
		hashmap: make(map[string]*keydirEntry, 1024),
	}
}

func (kd *keyDirIndex) get(key []byte) *keydirEntry {
	kd.lock.RLock()
	defer kd.lock.RUnlock()

	ent, ok := kd.hashmap[string(key)]
	if ok {
		return ent
	}

	return nil
}

func (kd *keyDirIndex) set(key []byte, ent *keydirEntry) {
	kd.lock.Lock()
	defer kd.lock.Unlock()

	kd.hashmap[string(key)] = ent
}

func (kd *keyDirIndex) del(key []byte) {
	kd.lock.Lock()
	defer kd.lock.Unlock()

	delete(kd.hashmap, string(key))
}
