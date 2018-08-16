package msgstorage

import (
	"bytes"
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
}

// NewMsgStorage returns new instance of message storage
func NewMsgStorage(db interfaces.DbStorage, protoVersion string) *MsgStorage {
	msgStorage := &MsgStorage{
		db:            db,
		protoVersion:  protoVersion,
		closeCh:       make(chan bool),
		confirmSyncCh: make(chan *amqp.Message, 4096),
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

func (storage *MsgStorage) periodicPersist() {
	tick := time.NewTicker(20 * time.Millisecond)
	for range tick.C {
		select {
		case <-storage.closeCh:
			return
		default:
			storage.persist()
		}
	}
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

	storage.db.ProcessBatch(batch)

	for _, message := range add {
		if storage.confirmMode && message.ConfirmMeta.DeliveryTag > 0 {
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

// Iterate with func fn over messages
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

// PurgeQueue delete messages
func (storage *MsgStorage) PurgeQueue(queue string) {
	prefix := []byte("msg." + queue)
	storage.db.Iterate(
		func(key []byte, value []byte) {
			if !bytes.HasPrefix(key, prefix) {
				return
			}
			storage.db.Del(string(key))
		},
	)
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
