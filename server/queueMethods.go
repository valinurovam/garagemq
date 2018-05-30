package server

import (
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/queue"
	"github.com/valinurovam/garagemq/binding"
	"github.com/valinurovam/garagemq/exchange"
)

func (channel *Channel) queueRoute(method amqp.Method) *amqp.Error {
	switch method := method.(type) {
	case *amqp.QueueDeclare:
		return channel.queueDeclare(method)
	case *amqp.QueueBind:
		return channel.queueBind(method)
	}

	return amqp.NewConnectionError(amqp.NotImplemented, "unable to route queue method "+method.Name(), method.ClassIdentifier(), method.MethodIdentifier())
}

func (channel *Channel) queueDeclare(method *amqp.QueueDeclare) *amqp.Error {
	vhost := channel.conn.getVhost()
	vhost.AppendQueue(&queue.Queue{Name: method.Queue})
	channel.sendMethod(&amqp.QueueDeclareOk{
		Queue:         method.Queue,
		MessageCount:  0,
		ConsumerCount: 0,
	})

	return nil
}

func (channel *Channel) queueBind(method *amqp.QueueBind) *amqp.Error {
	// @todo check queue already exists
	vhost := channel.conn.getVhost()
	ex := vhost.GetExchange(method.Exchange)
	bind := binding.New(method.Queue, method.Exchange, method.RoutingKey, method.Arguments, ex.ExType == exchange.EX_TYPE_TOPIC)
	ex.AppendBinding(bind)
	channel.sendMethod(&amqp.QueueBindOk{})

	return nil
}
