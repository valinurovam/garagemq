package storage

import (
	"time"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/valinurovam/garagemq/interfaces"
)

// Badger implements wrapper for badger database
type Badger struct {
	db *badger.DB
}

// NewBadger returns new instance of badger wrapper
func NewBadger(storageDir string) *Badger {
	storage := &Badger{}
	opts := badger.DefaultOptions
	opts.SyncWrites = true
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

// ProcessBatch process batch of operations
func (storage *Badger) ProcessBatch(batch []*interfaces.Operation) (err error) {
	return storage.db.Update(func(txn *badger.Txn) error {
		for _, op := range batch {
			if op.Op == interfaces.OpSet {
				txn.Set([]byte(op.Key), op.Value)
			}
			if op.Op == interfaces.OpDel {
				txn.Delete([]byte(op.Key))
			}
		}
		return nil
	})
}

// Close properly closes badger database
func (storage *Badger) Close() error {
	return storage.db.Close()
}

// Set adds a key-value pair to the database
func (storage *Badger) Set(key string, value []byte) (err error) {
	return storage.db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), value)
		return err
	})
}

// Del deletes a key
func (storage *Badger) Del(key string) (err error) {
	return storage.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete([]byte(key))
		return err
	})
}

// Get returns value by key
func (storage *Badger) Get(key string) (value []byte, err error) {
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

// Iterate iterates over all keys
func (storage *Badger) Iterate(fn func(key []byte, value []byte)) {
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

func (storage *Badger) runStorageGC() {
	timer := time.Tick(30 * time.Minute)
	for {
		select {
		case <-timer:
			storage.db.RunValueLogGC(0.7)
		}
	}
}
