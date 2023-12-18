package main

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
	"sync"

	esl "github.com/yeqown/enchanted-sleeve"
)

// Simulating read go routine and write goroutine to access db concurrently.
//

var (
	readRoutineCount  = flag.Int("read", 1, "read routine count")
	writeRoutineCount = flag.Int("write", 1, "write routine count")
	count             = flag.Int("n", 10000, "count of key to write")
)

// start read routine to access db concurrently.
// go run -race ./main.go -read=10 -write=10

func main() {
	flag.Parse()

	db, err := esl.Open("./testdata")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	readRoutine(db)
	writeRoutine(db)

	fmt.Println("done")
}

func readRoutine(db *esl.DB) {
	wg := sync.WaitGroup{}
	for i := 0; i < *readRoutineCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			num := 0

			for num < *count {
				num++
				_, err := db.Get([]byte("key"))
				if err != nil && !errors.Is(err, esl.ErrKeyNotFound) {
					fmt.Printf("readRoutine failed: %v\n", err)
					continue
				}
			}
		}()
	}

	wg.Wait()
}

func writeRoutine(db *esl.DB) {
	wg := sync.WaitGroup{}
	for i := 0; i < *writeRoutineCount; i++ {
		wg.Add(1)

		// each write routine will write the same key but different value.
		go func(i int) {
			defer wg.Done()
			num := 0

			for num < *count {
				num++
				err := db.Put([]byte("key"), []byte("value"+strconv.Itoa(i)))
				if err != nil {
					fmt.Printf("writeRoutine failed: %v\n", err)
					continue
				}
			}
		}(i)
	}

	wg.Wait()
}
