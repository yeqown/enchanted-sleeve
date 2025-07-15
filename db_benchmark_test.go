package esl

import (
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	benchmarkDataPath = "./benchmark/data/esl"
)

// Running benchmark to generate cpu and memory profile
// go test -bench=Benchmark_DB_Put -benchmem -cpuprofile ./benchmark/cpu.out -memprofile ./benchmark/mem.out
// go tool pprof -http=:8080 ./benchmark/cpu.out
// go tool pprof -http=:8080 ./benchmark/mem.out
func Benchmark_DB_Put(b *testing.B) {
	b.StopTimer()
	err := os.MkdirAll(benchmarkDataPath, 0744)
	require.NoError(b, err)
	defer func() {
		_ = os.RemoveAll(benchmarkDataPath)
	}()

	db, err := Open(benchmarkDataPath)
	require.NoError(b, err)
	require.NotNil(b, db)

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

// Running benchmark to generate cpu and memory profile
// go test -bench=Benchmark_DB_Get -benchmem -cpuprofile ./benchmark/cpu.out -memprofile ./benchmark/mem.out
// go tool pprof -http=:8080 ./benchmark/cpu.out
// go tool pprof -http=:8080 ./benchmark/mem.out
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
