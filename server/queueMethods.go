package server

import (
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/queue"
	"github.com/valinurovam/garagemq/binding"
	"github.com/valinurovam/garagemq/exchange"
	"fmt"
)

func (channel *Channel) queueRoute(method amqp.Method) *amqp.Error {
	switch method := method.(type) {
	case *amqp.QueueDeclare:
		return channel.queueDeclare(method)
	case *amqp.QueueBind:
		return channel.queueBind(method)
	case *amqp.QueueUnbind:
		return channel.queueUnbind(method)
	case *amqp.QueuePurge:
		return channel.queuePurge(method)
	case *amqp.QueueDelete:
		return channel.queueDelete(method)
	}

	return amqp.NewConnectionError(amqp.NotImplemented, "unable to route queue method "+method.Name(), method.ClassIdentifier(), method.MethodIdentifier())
}

// TODO refactoring queueDeclare please, cause DRY
func (channel *Channel) queueDeclare(method *amqp.QueueDeclare) *amqp.Error {
	vhost := channel.conn.getVirtualHost()
	existingQueue := vhost.GetQueue(method.Queue)

	if method.Passive {
		if method.NoWait {
			return nil
		}

		if existingQueue == nil {
			return amqp.NewChannelError(
				amqp.NotFound,
				fmt.Sprintf("queue '%s' not found", method.Queue),
				method.ClassIdentifier(),
				method.MethodIdentifier(),
			)
		} else {
			if existingQueue.IsExclusive() && existingQueue.ConnId() != channel.conn.id {
				return amqp.NewChannelError(
					amqp.ResourceLocked,
					fmt.Sprintf("queue '%s' is locked to another connection", method.Queue),
					method.ClassIdentifier(),
					method.MethodIdentifier(),
				)
			}

			channel.sendMethod(&amqp.QueueDeclareOk{
				Queue:         method.Queue,
				MessageCount:  uint32(existingQueue.Length()),
				ConsumerCount: uint32(existingQueue.ConsumersCount()),
			})
		}

		return nil
	}

	newQueue := queue.NewQueue(method.Queue, channel.conn.id, method.Exclusive, method.AutoDelete, method.Durable)

	if existingQueue != nil {
		if existingQueue.IsExclusive() && existingQueue.ConnId() != channel.conn.id {
			return amqp.NewChannelError(
				amqp.ResourceLocked,
				fmt.Sprintf("queue '%s' is locked to another connection", method.Queue),
				method.ClassIdentifier(),
				method.MethodIdentifier(),
			)
		}

		if err := existingQueue.EqualWithErr(newQueue); err != nil {
			return amqp.NewChannelError(
				amqp.PreconditionFailed,
				err.Error(),
				method.ClassIdentifier(),
				method.MethodIdentifier(),
			)
		}

		channel.sendMethod(&amqp.QueueDeclareOk{
			Queue:         method.Queue,
			MessageCount:  uint32(existingQueue.Length()),
			ConsumerCount: uint32(existingQueue.ConsumersCount()),
		})
		return nil
	}

	newQueue.Start()
	vhost.AppendQueue(newQueue)
	channel.sendMethod(&amqp.QueueDeclareOk{
		Queue:         method.Queue,
		MessageCount:  0,
		ConsumerCount: 0,
	})

	return nil
}

func (channel *Channel) queueBind(method *amqp.QueueBind) *amqp.Error {
	vhost := channel.conn.getVirtualHost()
	ex := vhost.GetExchange(method.Exchange)
	if ex == nil {
		return amqp.NewChannelError(
			amqp.NotFound,
			fmt.Sprintf("exchange '%s' not found", method.Exchange),
			method.ClassIdentifier(),
			method.MethodIdentifier(),
		)
	}

	queue := vhost.GetQueue(method.Queue)
	if queue == nil {
		return amqp.NewChannelError(
			amqp.NotFound,
			fmt.Sprintf("queue '%s' not found", method.Exchange),
			method.ClassIdentifier(),
			method.MethodIdentifier(),
		)
	}

	if queue.IsExclusive() && queue.ConnId() != channel.conn.id {
		return amqp.NewChannelError(
			amqp.ResourceLocked,
			fmt.Sprintf("queue '%s' is locked to another connection", method.Queue),
			method.ClassIdentifier(),
			method.MethodIdentifier(),
		)
	}

	bind := binding.New(method.Queue, method.Exchange, method.RoutingKey, method.Arguments, ex.ExType == exchange.EX_TYPE_TOPIC)
	ex.AppendBinding(bind)

	if !method.NoWait {
		channel.sendMethod(&amqp.QueueBindOk{})
	}

	return nil
}

func (channel *Channel) queueUnbind(method *amqp.QueueUnbind) *amqp.Error {
	vhost := channel.conn.getVirtualHost()
	ex := vhost.GetExchange(method.Exchange)
	if ex == nil {
		return amqp.NewChannelError(
			amqp.NotFound,
			fmt.Sprintf("exchange '%s' not found", method.Exchange),
			method.ClassIdentifier(),
			method.MethodIdentifier(),
		)
	}

	queue := vhost.GetQueue(method.Queue)
	if queue == nil {
		return amqp.NewChannelError(
			amqp.NotFound,
			fmt.Sprintf("queue '%s' not found", method.Exchange),
			method.ClassIdentifier(),
			method.MethodIdentifier(),
		)
	}

	if queue.IsExclusive() && queue.ConnId() != channel.conn.id {
		return amqp.NewChannelError(
			amqp.ResourceLocked,
			fmt.Sprintf("queue '%s' is locked to another connection", method.Queue),
			method.ClassIdentifier(),
			method.MethodIdentifier(),
		)
	}

	bind := binding.New(method.Queue, method.Exchange, method.RoutingKey, method.Arguments, ex.ExType == exchange.EX_TYPE_TOPIC)
	ex.RemoveBiding(bind)

	channel.sendMethod(&amqp.QueueUnbindOk{})

	return nil
}

func (channel *Channel) queuePurge(method *amqp.QueuePurge) *amqp.Error {
	return amqp.NewChannelError(amqp.NotImplemented, method.Name(), method.ClassIdentifier(), method.MethodIdentifier())
}

func (channel *Channel) queueDelete(method *amqp.QueueDelete) *amqp.Error {
	return amqp.NewChannelError(amqp.NotImplemented, method.Name(), method.ClassIdentifier(), method.MethodIdentifier())
}
