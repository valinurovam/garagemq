package storage

import (
	"time"

	"github.com/dgraph-io/badger"
	"github.com/valinurovam/garagemq/interfaces"
)

// Badger implements wrapper for badger database
type Badger struct {
	db *badger.DB
}

// NewBadger returns new instance of badger wrapper
func NewBadger(storageDir string) *Badger {
	storage := &Badger{}
	opts := badger.DefaultOptions(storageDir)
	opts.SyncWrites = true
	opts.Dir = storageDir
	opts.ValueDir = storageDir
	var err error
	storage.db, err = badger.Open(opts)
	if err != nil {
		panic(err)
	}

	go storage.runStorageGC()

	return storage
}

// ProcessBatch process batch of operations
func (storage *Badger) ProcessBatch(batch []*interfaces.Operation) (err error) {
	return storage.db.Update(func(txn *badger.Txn) error {
		for _, op := range batch {
			if op.Op == interfaces.OpSet {
				if err = txn.Set([]byte(op.Key), op.Value); err != nil {
					return err
				}
			}
			if op.Op == interfaces.OpDel {
				if err = txn.Delete([]byte(op.Key)); err != nil {
					return err
				}
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
		opts.AllVersions = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			fn(k, v)
		}
		return nil
	})
}

// Iterate iterates over keys with prefix
func (storage *Badger) IterateByPrefix(prefix []byte, limit uint64, fn func(key []byte, value []byte)) uint64 {
	var totalIterated uint64
	storage.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix) && ((limit > 0 && totalIterated < limit) || limit <= 0); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			fn(k, v)
			totalIterated++
		}
		return nil
	})

	return totalIterated
}

func (storage *Badger) KeysByPrefixCount(prefix []byte) uint64 {
	var count uint64
	storage.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = false
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}

		return nil
	})

	return count
}

// Iterate iterates over keys with prefix
func (storage *Badger) DeleteByPrefix(prefix []byte) {
	deleteKeys := func(keysForDelete [][]byte) error {
		if err := storage.db.Update(func(txn *badger.Txn) error {
			for _, key := range keysForDelete {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		return nil
	}

	collectSize := 100000
	keysForDeleteBunches := make([][][]byte, 0)
	keysForDelete := make([][]byte, 0, collectSize)
	keysCollected := 0

	// создать банчи и удалять банчами после итератора же ну
	storage.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = false
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			keysForDelete = append(keysForDelete, key)
			keysCollected++
			if keysCollected == collectSize {
				keysForDeleteBunches = append(keysForDeleteBunches, keysForDelete)
				keysForDelete = make([][]byte, 0, collectSize)
				keysCollected = 0
			}
		}
		if keysCollected > 0 {
			keysForDeleteBunches = append(keysForDeleteBunches, keysForDelete)
		}

		return nil
	})

	for _, keys := range keysForDeleteBunches {
		deleteKeys(keys)
	}
}

// Iterate iterates over keys with prefix
func (storage *Badger) IterateByPrefixFrom(prefix []byte, from []byte, limit uint64, fn func(key []byte, value []byte)) uint64 {
	var totalIterated uint64
	storage.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.AllVersions = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(from); it.ValidForPrefix(prefix) && ((limit > 0 && totalIterated < limit) || limit <= 0); it.Next() {
			item := it.Item()
			k := item.KeyCopy(nil)
			v, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			fn(k, v)
			totalIterated++
		}
		return nil
	})

	return totalIterated
}

func (storage *Badger) runStorageGC() {
	timer := time.NewTicker(10 * time.Minute)
	for {
		select {
		case <-timer.C:
			storage.storageGC()
		}
	}
}

func (storage *Badger) storageGC() {
again:
	err := storage.db.RunValueLogGC(0.5)
	if err == nil {
		goto again
	}
}
