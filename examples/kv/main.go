package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"

	esl "github.com/yeqown/enchanted-sleeve"
)

var (
	read  = flag.Bool("read", true, "enable read mode")
	write = flag.Bool("write", false, "enable write mode")
	count = flag.Int("n", 100, "count of write operation")
)

func main() {
	flag.Parse()

	db, err := esl.Open("./testdata")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if *write {
		writeIn(db)
	}

	if *read {
		readOut(db)
	}
}

// writeIn mock count random write and read operation
// key = random_$idx
// value = random_$idx
//
// op = ["add", "modify", "delete"]
// if op is 'modify' or 'delete' the key should be got from exists keys
func writeIn(db *esl.DB) {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	ops := []string{"add", "modify", "delete"}
	existsKeys := make([]string, 0, 10000)
	gotKey := func() string {
		if len(existsKeys) == 0 {
			return ""
		}

		return existsKeys[rand.Intn(len(existsKeys))]
	}

	var err error

	for i := 0; i < *count; i++ {
		op := rand.Intn(3)
		switch op {
		case 0:
			key := []byte(fmt.Sprintf("random_%d", i))
			value := key
			err = db.Put(key, value)
			existsKeys = append(existsKeys, string(key))
		case 1:
			key := gotKey()
			if key == "" {
				continue
			}
			value := []byte(fmt.Sprintf("random_%d", i))
			err = db.Put([]byte(key), value)
		case 2:
			key := gotKey()
			if key == "" {
				continue
			}
			err = db.Delete([]byte(key))
		default:
			panic("invalid op")
		}

		if err != nil {
			fmt.Printf("op(%s) failed: %v\n", ops[op], err)
			continue
		}
	}
}

// readOut read db from exists file
func readOut(db *esl.DB) {
	keys := db.ListKeys()

	for _, key := range keys {
		value, err := db.Get(key)
		if err != nil {
			fmt.Printf("read key(%s) failed: %v\n", key, err)
			continue
		}

		fmt.Printf("key(%s) = %s\n", key, value)
	}
}
