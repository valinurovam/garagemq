package server

import (
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/binding"
	"github.com/valinurovam/garagemq/exchange"
	"github.com/valinurovam/garagemq/queue"
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

func (channel *Channel) queueDeclare(method *amqp.QueueDeclare) *amqp.Error {
	var existingQueue *queue.Queue
	var notFoundErr, exclusiveErr *amqp.Error

	if method.Queue == "" {
		return amqp.NewChannelError(
			amqp.CommandInvalid,
			"queue name is required",
			method.ClassIdentifier(),
			method.MethodIdentifier(),
		)
	}

	existingQueue, notFoundErr = channel.getQueueWithError(method.Queue, method)
	exclusiveErr = channel.checkQueueLockWithError(existingQueue, method)

	if method.Passive {
		if method.NoWait {
			return nil
		}

		if existingQueue == nil {
			return notFoundErr
		}

		if exclusiveErr != nil {
			return exclusiveErr
		}

		channel.SendMethod(&amqp.QueueDeclareOk{
			Queue:         method.Queue,
			MessageCount:  uint32(existingQueue.Length()),
			ConsumerCount: uint32(existingQueue.ConsumersCount()),
		})

		return nil
	}

	newQueue := channel.conn.GetVirtualHost().NewQueue(
		method.Queue,
		channel.conn.id,
		method.Exclusive,
		method.AutoDelete,
		method.Durable,
		channel.server.config.Queue.ShardSize,
	)

	if existingQueue != nil {
		if exclusiveErr != nil {
			return exclusiveErr
		}

		if err := existingQueue.EqualWithErr(newQueue); err != nil {
			return amqp.NewChannelError(
				amqp.PreconditionFailed,
				err.Error(),
				method.ClassIdentifier(),
				method.MethodIdentifier(),
			)
		}

		channel.SendMethod(&amqp.QueueDeclareOk{
			Queue:         method.Queue,
			MessageCount:  uint32(existingQueue.Length()),
			ConsumerCount: uint32(existingQueue.ConsumersCount()),
		})
		return nil
	}

	newQueue.Start()
	channel.conn.GetVirtualHost().AppendQueue(newQueue)
	channel.SendMethod(&amqp.QueueDeclareOk{
		Queue:         method.Queue,
		MessageCount:  0,
		ConsumerCount: 0,
	})

	return nil
}

func (channel *Channel) queueBind(method *amqp.QueueBind) *amqp.Error {
	var ex *exchange.Exchange
	var qu *queue.Queue
	var err *amqp.Error

	if ex, err = channel.getExchangeWithError(method.Exchange, method); err != nil {
		return err
	}

	if qu, err = channel.getQueueWithError(method.Queue, method); err != nil {
		return err
	}

	if err = channel.checkQueueLockWithError(qu, method); err != nil {
		return err
	}

	bind := binding.NewBinding(method.Queue, method.Exchange, method.RoutingKey, method.Arguments, ex.ExType() == exchange.ExTypeTopic)
	ex.AppendBinding(bind)

	if ex.IsDurable() && qu.IsDurable() {
		channel.conn.GetVirtualHost().PersistBinding(bind)
	}

	if !method.NoWait {
		channel.SendMethod(&amqp.QueueBindOk{})
	}

	return nil
}

func (channel *Channel) queueUnbind(method *amqp.QueueUnbind) *amqp.Error {
	var ex *exchange.Exchange
	var qu *queue.Queue
	var err *amqp.Error

	if ex, err = channel.getExchangeWithError(method.Exchange, method); err != nil {
		return err
	}

	if qu, err = channel.getQueueWithError(method.Queue, method); err != nil {
		return err
	}

	if err = channel.checkQueueLockWithError(qu, method); err != nil {
		return err
	}

	bind := binding.NewBinding(method.Queue, method.Exchange, method.RoutingKey, method.Arguments, ex.ExType() == exchange.ExTypeTopic)
	ex.RemoveBinding(bind)
	channel.conn.GetVirtualHost().RemoveBindings([]*binding.Binding{bind})
	channel.SendMethod(&amqp.QueueUnbindOk{})

	return nil
}

func (channel *Channel) queuePurge(method *amqp.QueuePurge) *amqp.Error {
	var qu *queue.Queue
	var err *amqp.Error

	if qu, err = channel.getQueueWithError(method.Queue, method); err != nil {
		return err
	}

	if err = channel.checkQueueLockWithError(qu, method); err != nil {
		return err
	}

	msgCnt := qu.Purge()
	channel.srvMetrics.Total.Counter.Dec(int64(msgCnt))
	channel.srvMetrics.Ready.Counter.Dec(int64(msgCnt))
	if !method.NoWait {
		channel.SendMethod(&amqp.QueuePurgeOk{MessageCount: uint32(msgCnt)})
	}
	return nil
}

func (channel *Channel) queueDelete(method *amqp.QueueDelete) *amqp.Error {
	var qu *queue.Queue
	var err *amqp.Error

	if qu, err = channel.getQueueWithError(method.Queue, method); err != nil {
		return err
	}

	if err = channel.checkQueueLockWithError(qu, method); err != nil {
		return err
	}

	var length, errDel = channel.conn.GetVirtualHost().DeleteQueue(method.Queue, method.IfUnused, method.IfEmpty)
	channel.srvMetrics.Total.Counter.Dec(int64(length))
	channel.srvMetrics.Ready.Counter.Dec(int64(length))
	if errDel != nil {
		return amqp.NewChannelError(amqp.PreconditionFailed, errDel.Error(), method.ClassIdentifier(), method.MethodIdentifier())
	}

	channel.SendMethod(&amqp.QueueDeleteOk{MessageCount: uint32(length)})
	return nil
}
