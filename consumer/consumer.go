package consumer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/interfaces"
	"github.com/valinurovam/garagemq/qos"
	"github.com/valinurovam/garagemq/queue"
)

const (
	Started = iota
	Stopped
	Paused
)

var cid uint64

type Consumer struct {
	Id          uint64
	Queue       string
	ConsumerTag string
	noAck       bool
	channel     interfaces.Channel
	queue       *queue.Queue
	status      int
	qos         []*qos.AmqpQos
	consume     chan bool
	stopLock    sync.RWMutex
}

func New(queueName string, consumerTag string, noAck bool, channel interfaces.Channel, queue *queue.Queue, qos []*qos.AmqpQos) *Consumer {
	id := atomic.AddUint64(&cid, 1)
	if consumerTag == "" {
		consumerTag = generateTag(id)
	}
	return &Consumer{
		Id:          id,
		Queue:       queueName,
		ConsumerTag: consumerTag,
		noAck:       noAck,
		channel:     channel,
		queue:       queue,
		qos:         qos,
		consume:     make(chan bool, 1),
	}
}

func generateTag(id uint64) string {
	return fmt.Sprintf("%d_%d", time.Now().Unix(), id)
}

func (consumer *Consumer) Start() {
	consumer.status = Started
	go consumer.startConsume()
	consumer.Consume()
}

func (consumer *Consumer) startConsume() {
	var message *amqp.Message
	for _ = range consumer.consume {
		if consumer.status == Stopped {
			break
		}

		if consumer.noAck {
			message = consumer.queue.Pop()
		} else {
			message = consumer.queue.PopQos(consumer.qos)
		}

		if message == nil {
			continue
		}

		dTag := consumer.channel.NextDeliveryTag()
		if !consumer.noAck {
			consumer.channel.AddUnackedMessage(dTag, consumer.ConsumerTag, consumer.queue.GetName(), message)
		}

		consumer.channel.SendContent(&amqp.BasicDeliver{
			ConsumerTag: consumer.ConsumerTag,
			DeliveryTag: dTag,
			Redelivered: message.DeliveryCount > 1,
			Exchange:    message.Exchange,
			RoutingKey:  message.RoutingKey,
		}, message)

		consumer.Consume()
	}
}

func (consumer *Consumer) Pause() {
	consumer.status = Paused
}

func (consumer *Consumer) UnPause() {
	consumer.status = Started
}

func (consumer *Consumer) Consume() {
	if consumer.status == Stopped || consumer.status == Paused {
		return
	}

	select {
	case consumer.consume <- true:
	default:
	}
}

func (consumer *Consumer) Stop() {
	consumer.stopLock.Lock()
	defer consumer.stopLock.Unlock()
	if consumer.status == Stopped {
		return
	}
	consumer.status = Stopped
	consumer.queue.RemoveConsumer(consumer.ConsumerTag)
	close(consumer.consume)
}

func (consumer *Consumer) Cancel() {
	consumer.Stop()
	consumer.channel.SendMethod(&amqp.BasicCancel{ConsumerTag: consumer.ConsumerTag, NoWait: true})
}

func (consumer *Consumer) Tag() string {
	return consumer.ConsumerTag
}

func (consumer *Consumer) Qos() []*qos.AmqpQos {
	return consumer.qos
}
