package main

import (
	"strconv"
	"github.com/dgraph-io/badger"
	"fmt"
)

func main() {
	opts := badger.DefaultOptions
	opts.Dir = "db_main"
	opts.ValueDir = "db_main"
	var db, err = badger.Open(opts)

	defer db.Close()
	if err != nil {
		panic(err)
	}
	iterations := 1000000
	for n := 0; n < iterations; n++ {
		key := strconv.Itoa(n)
		db.Update(func(txn *badger.Txn) error {
			err := txn.Set([]byte(key), []byte(key))
			return err
		})
	}

	for n := 0; n < iterations; n++ {
		key := strconv.Itoa(n)
		db.Update(func(txn *badger.Txn) error {
			err := txn.Delete([]byte(key))
			return err
		})
	}
	for {
		err := db.RunValueLogGC(0.5)
		fmt.Println("GC")
		if err != nil {
			break
		}
	}
}
