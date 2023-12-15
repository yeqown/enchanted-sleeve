package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	wal2 "github.com/yeqown/enchanted-sleeve/wal"
)

func example2() {
	w, err := wal2.NewWAL(
		wal2.DefaultConfig(),
		wal2.WithRoot("./testdata/wal"),
		wal2.WithMaxSegments(5),         // 5 segments
		wal2.WithMaxSegmentSize(2*1024), // 2KB
	)
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	// now you can use wal to write and read data
	// truncate per read 1000 entries
	// write is the 2 times of read
	padding16B := func(i int) []byte {
		return []byte(fmt.Sprintf("%016d", i))
	}

	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			time.Sleep(50 * time.Millisecond)
			_, err = w.Write(padding16B(i))
			if err != nil {
				log.Printf("write err: %v\n", err)
			}
		}
	}()

	go func() {
		defer wg.Done()
		var (
			hasRead      int64
			truncateMark int
			entry        []byte
		)

		for i := 0; i < 1000; i++ {
			time.Sleep(100 * time.Millisecond)
			entry, hasRead, err = w.ReadOldest()
			if err != nil {
				log.Printf("read oldest(%d) err: %v\n", hasRead, err)
			} else {
				log.Printf("read oldest(%d): %s\n", hasRead, string(entry))
			}

			truncateMark += 1
			if truncateMark >= 100 {
				err := w.TruncateBefore(hasRead + 100)
				if err != nil {
					log.Printf("truncate err: %v\n", err)
				}
				truncateMark = 0
			}
		}
	}()

	wg.Wait()

	if err := w.Close(); err != nil {
		panic(err)
	}
}
