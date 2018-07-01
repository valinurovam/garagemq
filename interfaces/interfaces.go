package interfaces

import (
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/qos"
)

// TODO some interfaces looks like useless

type Queue interface {
	Push(item interface{})
	Pop() (res interface{})
	Length() int64
}

type AmqpQueue interface {
	Start()
	Stop() error
	Push(message *amqp.Message)
	PushFromStorage(message *amqp.Message)
	Pop() *amqp.Message
	PopQos(qosList []*qos.AmqpQos) *amqp.Message
	AckMsg(id uint64)
	RemoveConsumer(cTag string)
	GetName() string
	IsExclusive() bool
	IsAutoDelete() bool
	IsDurable() bool
	IsActive() bool
	ConnId() uint64
	Length() uint64
	ConsumersCount() int
	Purge() uint64
	AddConsumer(consumer Consumer, exclusive bool) error
	EqualWithErr(qu AmqpQueue) error
	Delete(ifUnused bool, ifEmpty bool) (uint64, error)
	Marshal(protoVersion string) []byte
}

type Channel interface {
	SendContent(method amqp.Method, message *amqp.Message)
	SendMethod(method amqp.Method)
	NextDeliveryTag() uint64
	AddUnackedMessage(dTag uint64, cTag string, queue string, message *amqp.Message)
}

type Consumer interface {
	Consume()
	Tag() string
	Stop()
	Start()
	Cancel()
}

type Binding interface {
	MatchDirect(exchange string, routingKey string) bool
	MatchFanout(exchange string) bool
	MatchTopic(exchange string, routingKey string) bool
	GetExchange() string
	GetRoutingKey() string
	GetQueue() string
	Equal(biding Binding) bool
}

type DbStorage interface {
	Set(key string, value []byte) (err error)
	Del(key string) (err error)
	Get(key string) (value []byte, err error)
	Iterate(fn func(key []byte, value []byte))
	Close() error
}
