package esl

import (
	"testing"
	"os"
	"github.com/stretchr/testify/require"
	"strconv"
	"math/rand"
	"time"
)

var (
	benchmarkDataPath = "./benchmark-data/esl"
)

func Benchmark_DB_Put(b *testing.B) {
	b.StopTimer()
	err := os.MkdirAll(benchmarkDataPath, 0744)
	require.NoError(b, err)
	defer func() {
		_ = os.RemoveAll(benchmarkDataPath)
	}()

	db, err := Open(benchmarkDataPath)

	keyFunc := func(i int) []byte {
		return []byte("key" + strconv.Itoa(i))
	}
	valueFunc := func(i int) []byte {
		return []byte("value" + strconv.Itoa(i))
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		err = db.Put(keyFunc(i), valueFunc(i))
		require.NoError(b, err)
	}
}

func Benchmark_DB_Get(b *testing.B) {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	b.StopTimer()
	err := os.MkdirAll(benchmarkDataPath, 0744)
	require.NoError(b, err)
	defer func() {
		_ = os.RemoveAll(benchmarkDataPath)
	}()

	db, err := Open(benchmarkDataPath)
	require.NoError(b, err)

	// prepare 1000,000
	keyFunc := func(i int) []byte {
		return []byte("key" + strconv.Itoa(i))
	}
	valueFunc := func(i int) []byte {
		return []byte("value" + strconv.Itoa(i))
	}
	for i := 0; i < 1000_000; i++ {
		err = db.Put(keyFunc(i), valueFunc(i))
		require.NoError(b, err)
	}

	randomKey := func() []byte {
		i := rand.Intn(1000_000)
		return keyFunc(i)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		_, err = db.Get(randomKey())
		require.NoError(b, err)
	}
}
