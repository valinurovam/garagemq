package interfaces

import (
	"github.com/valinurovam/garagemq/amqp"
)

type Channel interface {
	SendContent(method amqp.Method, message *amqp.Message)
	SendMethod(method amqp.Method)
	NextDeliveryTag() uint64
	AddUnackedMessage(dTag uint64, cTag string, queue string, message *amqp.Message)
}

type Consumer interface {
	Consume()
	Tag() string
	Cancel()
}

const OpSet = 1
const OpDel = 2

type Operation struct {
	Key   string
	Value []byte
	Op    byte
}

type DbStorage interface {
	Set(key string, value []byte) (err error)
	Del(key string) (err error)
	Get(key string) (value []byte, err error)
	Iterate(fn func(key []byte, value []byte))
	ProcessBatch(batch []*Operation) (err error)
	Close() error
}
