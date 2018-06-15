package interfaces

import (
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/qos"
)

type Queue interface {
	Push(item interface{})
	Pop() (res interface{})
	Length() int64
}

type AmqpQueue interface {
	Push(message *amqp.Message)
	Pop() *amqp.Message
	PopQos(qosList []*qos.AmqpQos) *amqp.Message
	RemoveConsumer(cTag string)
}

type Channel interface {
	SendContent(method amqp.Method, message *amqp.Message)
	NextDeliveryTag() uint64
	AddUnackedMessage(dTag uint64, cTag string, message *amqp.Message)
}
