package storage

import (
	"github.com/tidwall/buntdb"
	"time"
)

type StorageBunt struct {
	db *buntdb.DB
}

func NewBunt(storagePath string) *StorageBunt {
	storage := &StorageBunt{}

	var db, err = buntdb.Open(storagePath)
	if err != nil {
		panic(err)
	}

	db.SetConfig(buntdb.Config{
		SyncPolicy:         buntdb.Never,
		AutoShrinkDisabled: true,
	})

	storage.db = db
	go storage.runStorageGC()

	return storage
}

func (storage *StorageBunt) Close() error {
	return storage.db.Close()
}

func (storage *StorageBunt) Set(key string, value []byte) (err error) {
	return storage.db.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(key, string(value), nil)
		return err
	})
}

func (storage *StorageBunt) Del(key string) (err error) {
	return storage.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(key)
		return err
	})
}

func (storage *StorageBunt) Get(key string) (value []byte, err error) {
	storage.db.View(func(tx *buntdb.Tx) error {
		data, err := tx.Get(key)
		if err != nil {
			return err
		}
		value = make([]byte, len(data))
		copy(value, data)
		return nil
	})
	return
}

func (storage *StorageBunt) Iterate(fn func(key []byte, value []byte)) {
	storage.db.View(func(tx *buntdb.Tx) error {
		err := tx.Ascend("", func(key, value string) bool {
			fn([]byte(key), []byte(value))
			return true
		})
		return err
	})
}

func (storage *StorageBunt) runStorageGC() {
	timer := time.Tick(time.Minute)
	for {
		select {
		case <-timer:
			storage.db.Shrink()
		}
	}
}
