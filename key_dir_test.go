package esl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_decodeKeydirEntry(t *testing.T) {
	entry := keydirMemEntry{
		fileId:      1,
		valueSize:   10,
		entryOffset: 10,
		valueOffset: 20,
	}
	encoded := entry.bytes()
	assert.Equal(t, keydirMem_Size, len(encoded))

	entry2, err := decodeKeydirEntry(encoded)
	assert.NoError(t, err)
	assert.NotNil(t, entry2)

	assert.Equal(t, entry.fileId, entry2.fileId)
	assert.Equal(t, entry.valueSize, entry2.valueSize)
	assert.Equal(t, entry.entryOffset, entry2.entryOffset)
	assert.Equal(t, entry.valueOffset, entry2.valueOffset)
}

func Test_decodeKeydirFileEntry(t *testing.T) {
	entry := keydirFileEntry{
		keydirMemEntry: keydirMemEntry{
			fileId:      1,
			valueSize:   10,
			entryOffset: 10,
			valueOffset: 20,
		},
		keySize: 10,
		key:     []byte("hellohello"),
	}

	encoded := entry.bytes()
	assert.Equal(t, int(keydirFile_fixedSize+entry.keySize), len(encoded))

	entry2, err := decodeKeydirFileEntry(encoded)
	assert.NoError(t, err)
	assert.NotNil(t, entry2)

	assert.Equal(t, entry.fileId, entry2.fileId)
	assert.Equal(t, entry.valueSize, entry2.valueSize)
	assert.Equal(t, entry.entryOffset, entry2.entryOffset)
	assert.Equal(t, entry.valueOffset, entry2.valueOffset)
	assert.Equal(t, entry.keySize, entry2.keySize)

}
