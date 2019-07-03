package msgstorage

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/interfaces"
)

// MsgStorage represents storage for store all durable messages
// All operations (add, update and delete) store into little queues and
// periodically persist every 20ms
// If storage in confirm-mode - in every persisted message storage send confirm to vhost
type MsgStorage struct {
	db            interfaces.DbStorage
	persistLock   sync.Mutex
	add           map[string]*amqp.Message
	update        map[string]*amqp.Message
	del           map[string]*amqp.Message
	protoVersion  string
	closeCh       chan bool
	confirmSyncCh chan *amqp.Message
	confirmMode   bool
	writeCh       chan struct{}
}

// NewMsgStorage returns new instance of message storage
func NewMsgStorage(db interfaces.DbStorage, protoVersion string) *MsgStorage {
	msgStorage := &MsgStorage{
		db:            db,
		protoVersion:  protoVersion,
		closeCh:       make(chan bool),
		confirmSyncCh: make(chan *amqp.Message, 4096),
		writeCh:       make(chan struct{}, 5),
	}
	msgStorage.cleanPersistQueue()
	go msgStorage.periodicPersist()
	return msgStorage
}

func (storage *MsgStorage) cleanPersistQueue() {
	storage.add = make(map[string]*amqp.Message)
	storage.update = make(map[string]*amqp.Message)
	storage.del = make(map[string]*amqp.Message)
}

// We try to persist messages every 20ms and every 1000msg
func (storage *MsgStorage) periodicPersist() {
	tick := time.NewTicker(20 * time.Millisecond)

	go func() {
		for range tick.C {
			storage.writeCh <- struct{}{}
		}
	}()

	for range storage.writeCh {
		select {
		case <-storage.closeCh:
			return
		default:
			storage.persist()
		}
	}
}

// no need to be thread safe
func (storage *MsgStorage) getQueueLen() int {
	return len(storage.add) + len(storage.update) + len(storage.del)
}

func (storage *MsgStorage) persist() {
	storage.persistLock.Lock()
	add := storage.add
	del := storage.del
	update := storage.update
	storage.cleanPersistQueue()
	storage.persistLock.Unlock()

	rmDel := make([]string, 0)
	for delKey := range del {
		if _, ok := add[delKey]; ok {
			delete(add, delKey)
			rmDel = append(rmDel, delKey)
		}

		delete(update, delKey)
	}

	for _, delKey := range rmDel {
		delete(del, delKey)
	}

	batch := make([]*interfaces.Operation, 0, len(add)+len(update)+len(del))
	for key, message := range add {
		data, _ := message.Marshal(storage.protoVersion)
		batch = append(
			batch,
			&interfaces.Operation{
				Key:   key,
				Value: data,
				Op:    interfaces.OpSet,
			},
		)
	}

	for key, message := range update {
		data, _ := message.Marshal(storage.protoVersion)
		batch = append(
			batch,
			&interfaces.Operation{
				Key:   key,
				Value: data,
				Op:    interfaces.OpSet,
			},
		)
	}

	for key := range del {
		batch = append(
			batch,
			&interfaces.Operation{
				Key: key,
				Op:  interfaces.OpDel,
			},
		)
	}

	if err := storage.db.ProcessBatch(batch); err != nil {
		panic(err)
	}

	for _, message := range add {
		if message.ConfirmMeta != nil && storage.confirmMode && message.ConfirmMeta.DeliveryTag > 0 {
			message.ConfirmMeta.ActualConfirms++
			storage.confirmSyncCh <- message
		}
	}
}

// ReceiveConfirms set message storage in confirm mode and return channel for receive confirms
func (storage *MsgStorage) ReceiveConfirms() chan *amqp.Message {
	storage.confirmMode = true
	return storage.confirmSyncCh
}

// Add append message into add-queue
func (storage *MsgStorage) Add(message *amqp.Message, queue string) error {
	if storage.getQueueLen() > 1000 {
		storage.writeCh <- struct{}{}
	}

	storage.persistLock.Lock()
	defer storage.persistLock.Unlock()

	storage.add[makeKey(message.ID, queue)] = message
	return nil
}

// Update append message into update-queue
func (storage *MsgStorage) Update(message *amqp.Message, queue string) error {
	storage.persistLock.Lock()
	defer storage.persistLock.Unlock()
	storage.update[makeKey(message.ID, queue)] = message
	return nil
}

// Del append message into del-queue
func (storage *MsgStorage) Del(message *amqp.Message, queue string) error {
	storage.persistLock.Lock()
	defer storage.persistLock.Unlock()
	storage.del[makeKey(message.ID, queue)] = message
	return nil
}

// Iterate iterates over all messages
func (storage *MsgStorage) Iterate(fn func(queue string, message *amqp.Message)) {
	storage.db.Iterate(
		func(key []byte, value []byte) {
			queueName := getQueueFromKey(string(key))
			message := &amqp.Message{}
			message.Unmarshal(value, storage.protoVersion)
			fn(queueName, message)
		},
	)
}

// IterateByQueue iterates over queue and call fn on each message
func (storage *MsgStorage) IterateByQueue(queue string, limit uint64, fn func(message *amqp.Message)) {
	prefix := "msg." + queue + "."
	storage.db.IterateByPrefix(
		[]byte(prefix),
		limit,
		func(key []byte, value []byte) {
			message := &amqp.Message{}
			message.Unmarshal(value, storage.protoVersion)
			fn(message)
		},
	)
}

// IterateByQueueFromMsgID iterates over queue from specific msgId and call fn on each message
func (storage *MsgStorage) IterateByQueueFromMsgID(queue string, msgID uint64, limit uint64, fn func(message *amqp.Message)) uint64 {
	prefix := "msg." + queue + "."
	from := makeKey(msgID, queue)
	return storage.db.IterateByPrefixFrom(
		[]byte(prefix),
		[]byte(from),
		limit,
		func(key []byte, value []byte) {
			message := &amqp.Message{}
			message.Unmarshal(value, storage.protoVersion)
			fn(message)
		},
	)
}

// GetQueueLength returns queue length in message storage
func (storage *MsgStorage) GetQueueLength(queue string) uint64 {
	prefix := "msg." + queue + "."
	return storage.db.KeysByPrefixCount([]byte(prefix))
}

// PurgeQueue delete messages
func (storage *MsgStorage) PurgeQueue(queue string) {
	prefix := []byte("msg." + queue + ".")
	storage.db.DeleteByPrefix(prefix)
}

// Close properly "stop" message storage
func (storage *MsgStorage) Close() error {
	storage.closeCh <- true
	storage.persistLock.Lock()
	defer storage.persistLock.Unlock()
	return storage.db.Close()
}

func makeKey(id uint64, queue string) string {
	return "msg." + queue + "." + strconv.FormatInt(int64(id), 10)
}

func getQueueFromKey(key string) string {
	parts := strings.Split(key, ".")
	return parts[1]
}
