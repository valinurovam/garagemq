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
	case *amqp.ChannelCloseOk:
		return channel.channelCloseOk(method)
	}

	return amqp.NewConnectionError(amqp.NotImplemented, "unable to route channel method "+method.Name(), method.ClassIdentifier(), method.MethodIdentifier())
}

func (channel *Channel) channelOpen(method *amqp.ChannelOpen) (err *amqp.Error) {
	// The client MUST NOT use this method on an already­opened channel
	if channel.status == ChannelOpen {
		return amqp.NewConnectionError(amqp.ChannelError, "channel already open", method.ClassIdentifier(), method.MethodIdentifier())
	}

	channel.SendMethod(&amqp.ChannelOpenOk{})
	channel.status = ChannelOpen

	return nil
}

func (channel *Channel) channelClose(method *amqp.ChannelClose) (err *amqp.Error) {
	channel.status = ChannelClosed
	channel.SendMethod(&amqp.ChannelCloseOk{})
	channel.close()
	return nil
}

func (channel *Channel) channelCloseOk(method *amqp.ChannelCloseOk) (err *amqp.Error) {
	channel.status = ChannelClosed
	return nil
}
