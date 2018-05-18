package server

import (
	"github.com/valinurovam/garagemq/amqp"
)

func (channel *Channel) channelRoute(method amqp.Method) *amqp.Error {
	switch method := method.(type) {
	case *amqp.ChannelOpen:
		return channel.channelOpen(method)
	case *amqp.ChannelClose:
		return channel.channelClose(method)
	}

	return amqp.NewConnectionError(amqp.NotImplemented, "Unable to route channel method", method.ClassIdentifier(), method.MethodIdentifier())
}

func (channel *Channel) channelOpen(method *amqp.ChannelOpen) (err *amqp.Error) {
	// The client MUST NOT use this method on an already­opened channel
	if channel.status == ChannelOpen {
		return amqp.NewConnectionError(amqp.ChannelError, "Channel already open", method.ClassIdentifier(), method.MethodIdentifier())
	}

	channel.sendMethod(&amqp.ChannelOpenOk{})
	channel.status = ChannelOpen

	return nil
}

func (channel *Channel) channelClose(method *amqp.ChannelClose) (err *amqp.Error) {
	channel.sendMethod(&amqp.ChannelCloseOk{})
	return nil
}
