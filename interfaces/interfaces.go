package interfaces

import "github.com/valinurovam/garagemq/amqp"

type Queue interface {
	Push(item interface{})
	Pop() (res interface{})
	Length() int64
}

type AmqpQueue interface {
	Push(message *amqp.Message)
	Pop() *amqp.Message
}

type Channel interface {
	SendContent(method amqp.Method, message *amqp.Message)
	NextDeliveryTag() uint64
}