package server

import "github.com/valinurovam/garagemq/amqp"

func (channel *Channel) basicRoute(method amqp.Method) *amqp.Error {
	switch method := method.(type) {
	case *amqp.BasicQos:
		return channel.basicQos(method)
	}

	return amqp.NewConnectionError(amqp.NotImplemented, "Unable to route basic method", method.ClassIdentifier(), method.MethodIdentifier())
}

func (channel *Channel) basicQos(method *amqp.BasicQos) (err *amqp.Error) {
	channel.sendMethod(&amqp.BasicQosOk{})

	return nil
}
