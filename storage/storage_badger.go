package storage

import (
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
)

type StorageBadger struct {
	db *badger.DB
}

func NewBadger(storageDir string) *StorageBadger {
	storage := &StorageBadger{}
	opts := badger.DefaultOptions
	opts.SyncWrites = false
	opts.TableLoadingMode = options.MemoryMap
	opts.Dir = storageDir
	opts.ValueDir = storageDir
	var err error
	storage.db, err = badger.Open(opts)
	if err != nil {
		panic(err)
	}

	return storage
}

func (storage *StorageBadger) Close() error {
	return storage.db.Close()
}

func (storage *StorageBadger) Set(key string, value []byte) (err error) {
	return storage.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), value)
		return err
	})
}

func (storage *StorageBadger) Del(key string) (err error) {
	return storage.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(key))
		return err
	})
}

func (storage *StorageBadger) Get(key string) (value []byte, err error) {
	err = storage.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(value)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func (storage *StorageBadger) Iterate(fn func(key []byte, value []byte)) {
	storage.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.AllVersions = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			v, err := item.Value()
			if err != nil {
				return err
			}
			fn(k, v)
		}
		return nil
	})
}

func (storage *StorageBadger) runStorageGC() {
	timer := time.Tick(30 * time.Minute)
	for {
		select {
		case <-timer:
			storage.db.RunValueLogGC(0.7)
		}
	}
}
