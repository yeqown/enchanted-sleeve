package esl

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_checksum(t *testing.T) {
	type args struct {
		ent *kvEntry
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			name: "normal case",
			args: args{
				ent: &kvEntry{
					crc:         0,
					tsTimestamp: 1702878103,
					keySize:     5,
					valueSize:   5,
					key:         []byte("hello"),
					value:       []byte("world"),
				},
			},
			want: 4020150805,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := checksum(tt.args.ent); got != tt.want {
				t.Errorf("checksum() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_kvEntry_fillcrc(t *testing.T) {
	entry := &kvEntry{
		crc:         0,
		tsTimestamp: 1702878103,
		keySize:     5,
		valueSize:   5,
		key:         []byte("hello"),
		value:       []byte("world"),
	}

	entry.fillcrc()

	assert.Equal(t, uint32(4020150805), entry.crc)
}

func Test_kvEntry_bytes(t *testing.T) {
	entry := &kvEntry{
		crc:         4020150805,
		tsTimestamp: 1702878103,
		keySize:     5,
		valueSize:   5,
		key:         []byte("hello"),
		value:       []byte("world"),
	}

	got := entry.bytes()
	want := []byte{
		0xef,
		0x9e,
		0xa2,
		0x15,
		0x65,
		0x7f,
		0xdb,
		0x97,
		0x0,
		0x5,
		0x0,
		0x5,
		0x68,
		0x65,
		0x6c,
		0x6c,
		0x6f,
		0x77,
		0x6f,
		0x72,
		0x6c,
		0x64,
	}
	assert.Equal(t, want, got)
}

func Test_kvEntry_encodeAndDecode(t *testing.T) {

	key := []byte("hello")
	value := []byte("world")
	keySize := uint16(len(key))
	valueSize := uint16(len(value))

	entry := newEntry(key, value)
	assert.Equal(t, keySize, entry.keySize)
	assert.Equal(t, valueSize, entry.valueSize)
	encoded := entry.bytes()

	require.Greater(t, len(encoded), kvEntry_fixedBytes)
	assert.Equal(t, int(kvEntry_fixedBytes+keySize+valueSize), len(encoded))

	header := encoded[0:kvEntry_fixedBytes]
	entry2, err := decodeEntryFromHeader(header)
	require.NoError(t, err)
	require.NotNil(t, entry2)

	assert.Equal(t, entry.crc, entry2.crc)
	assert.Equal(t, entry.tsTimestamp, entry2.tsTimestamp)
	assert.Equal(t, entry.keySize, entry2.keySize)
	assert.Equal(t, entry.valueSize, entry2.valueSize)
	assert.Equal(t, int(entry.keySize), cap(entry2.key))
	assert.Equal(t, int(entry.keySize), len(entry2.key))
	assert.Equal(t, int(entry.valueSize), cap(entry2.value))
	assert.Equal(t, int(entry.valueSize), len(entry2.value))
}

func Test_estimateEntry(t *testing.T) {
	type args struct {
		bytes int64
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "normal case 1",
			args: args{
				bytes: 100,
			},
			want: 4,
		},
		{
			name: "normal case 2",
			args: args{
				bytes: 1000,
			},
			want: 34,
		},
		{
			name: "abnormal case 1",
			args: args{
				bytes: 0,
			},
			want: 0,
		},
		{
			name: "abnormal case 2",
			args: args{
				bytes: -1,
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, estimateEntry(tt.args.bytes), "estimateEntry(%v)", tt.args.bytes)
		})
	}
}
