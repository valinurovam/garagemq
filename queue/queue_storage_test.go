package queue

import (
	"github.com/valinurovam/garagemq/amqp"
)

type MsgStorageMock struct {
	add    bool
	update bool
	del    bool
	purged bool
}

// Add append message into add-queue
func (storage *MsgStorageMock) Add(message *amqp.Message, queue string) error {
	storage.add = true
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
