package server

import (
	"github.com/valinurovam/garagemq/amqp"
)

func (channel *Channel) basicRoute(method amqp.Method) *amqp.Error {
	switch method := method.(type) {
	case *amqp.BasicQos:
		return channel.basicQos(method)
	case *amqp.BasicPublish:
		return channel.basicPublish(method)
	}

	return amqp.NewConnectionError(amqp.NotImplemented, "unable to route basic method "+method.Name(), method.ClassIdentifier(), method.MethodIdentifier())
}

func (channel *Channel) basicQos(method *amqp.BasicQos) (err *amqp.Error) {
	channel.sendMethod(&amqp.BasicQosOk{})

	return nil
}

func (channel *Channel) basicPublish(method *amqp.BasicPublish) (err *amqp.Error) {
	vhost := channel.conn.getVhost()

	if vhost.GetExchange(method.Exchange) == nil {
		return amqp.NewChannelError(amqp.NotFound, "exchange not found", method.ClassIdentifier(), method.MethodIdentifier())
	}

	channel.currentMessage = amqp.NewMessage(method)
	return nil
}
