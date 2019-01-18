package queue

import (
	"github.com/valinurovam/garagemq/amqp"
)

type MsgStorageMock struct {
	add    bool
	update bool
	del    bool
	purged bool

	messages []*amqp.Message
	index    map[uint64]int
	pos      int
}

func NewStorageMock(msgCap int) *MsgStorageMock {
	mock := &MsgStorageMock{}
	mock.messages = make([]*amqp.Message, msgCap)
	mock.index = make(map[uint64]int)

	return mock
}

// Add append message into add-queue
func (storage *MsgStorageMock) Add(message *amqp.Message, queue string) error {
	storage.add = true

	if storage.messages != nil {
		storage.messages[storage.pos] = message
		storage.index[message.ID] = storage.pos
		storage.pos++
	}

	return nil
}

// Update append message into update-queue
func (storage *MsgStorageMock) Update(message *amqp.Message, queue string) error {
	storage.update = true
	return nil
}

// Del append message into del-queue
func (storage *MsgStorageMock) Del(message *amqp.Message, queue string) error {
	storage.del = true
	return nil
}

// PurgeQueue delete messages
func (storage *MsgStorageMock) PurgeQueue(queue string) {
	storage.purged = true
}

func (storage *MsgStorageMock) GetQueueLength(queue string) uint64 {
	return uint64(len(storage.messages))
}

func (storage *MsgStorageMock) IterateByQueueFromMsgID(queue string, msgID uint64, limit uint64, fn func(message *amqp.Message)) uint64 {
	if storage.messages != nil {
		var startPos int
		var ok bool
		if startPos, ok = storage.index[msgID]; !ok {
			msgID++

			if startPos, ok = storage.index[msgID]; !ok {
				return 0
			}
		}

		var iterated uint64
		for i := startPos; i < len(storage.messages); i++ {
			fn(storage.messages[i])
			iterated++

			if iterated == limit {
				break
			}
		}

		return iterated
	}

	return 0
}
