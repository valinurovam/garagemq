package server

import (
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/consumer"
	"github.com/valinurovam/garagemq/qos"
)

func (channel *Channel) basicRoute(method amqp.Method) *amqp.Error {
	switch method := method.(type) {
	case *amqp.BasicQos:
		return channel.basicQos(method)
	case *amqp.BasicPublish:
		return channel.basicPublish(method)
	case *amqp.BasicConsume:
		return channel.basicConsume(method)
	case *amqp.BasicAck:
		return channel.basicAck(method)
	}

	return amqp.NewConnectionError(amqp.NotImplemented, "unable to route basic method "+method.Name(), method.ClassIdentifier(), method.MethodIdentifier())
}

func (channel *Channel) basicQos(method *amqp.BasicQos) (err *amqp.Error) {
	channel.updateQos(method.PrefetchCount, method.PrefetchSize, method.Global)
	channel.sendMethod(&amqp.BasicQosOk{})

	return nil
}

func (channel *Channel) basicAck(method *amqp.BasicAck) (err *amqp.Error) {
	return channel.handleAck(method)
}

func (channel *Channel) basicPublish(method *amqp.BasicPublish) (err *amqp.Error) {
	vhost := channel.conn.getVirtualHost()

	if vhost.GetExchange(method.Exchange) == nil {
		return amqp.NewChannelError(amqp.NotFound, "exchange not found", method.ClassIdentifier(), method.MethodIdentifier())
	}

	channel.currentMessage = amqp.NewMessage(method)
	return nil
}

func (channel *Channel) basicConsume(method *amqp.BasicConsume) (err *amqp.Error) {

	queue := channel.conn.getVirtualHost().GetQueue(method.Queue)

	if queue == nil {
		return amqp.NewChannelError(amqp.NotFound, "Queue not found", method.ClassIdentifier(), method.MethodIdentifier())
	}

	cmr := consumer.New(method.Queue, method.ConsumerTag, method.NoAck, channel, queue, []*qos.AmqpQos{channel.qos, channel.conn.qos})
	channel.addConsumer(cmr)

	if !method.NoWait {
		channel.sendMethod(&amqp.BasicConsumeOk{ConsumerTag: cmr.ConsumerTag})
	}

	cmr.Start()
	queue.AddConsumer(cmr)

	return nil
}
