package consumer

import (
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/qos"
	"github.com/valinurovam/garagemq/interfaces"
	"fmt"
	"time"
	"sync/atomic"
	"sync"
)

const (
	Started = iota
	Stopped
)

var cid uint64

type Consumer struct {
	Id          uint64
	Queue       string
	ConsumerTag string
	noAck       bool
	channel     interfaces.Channel
	queue       interfaces.AmqpQueue
	status      int
	qos         []*qos.AmqpQos
	consume     chan bool
	stopLock    sync.Mutex
}

func New(queueName string, consumerTag string, noAck bool, channel interfaces.Channel, queue interfaces.AmqpQueue, qos []*qos.AmqpQos) *Consumer {
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
}

func (consumer *Consumer) startConsume() {
	for _ = range consumer.consume {
		if consumer.status != Started {
			break
		}
		message := consumer.queue.PopQos(consumer.qos)
		if message == nil {
			continue
		}

		dTag := consumer.channel.NextDeliveryTag()
		if !consumer.noAck {
			consumer.channel.AddUnackedMessage(dTag, consumer.ConsumerTag, message)
		}

		consumer.channel.SendContent(&amqp.BasicDeliver{
			ConsumerTag: consumer.ConsumerTag,
			DeliveryTag: dTag,
			Redelivered: false,
			Exchange:    message.Exchange,
			RoutingKey:  message.RoutingKey,
		}, message)

		consumer.Consume()
	}
}

func (consumer *Consumer) Consume() {
	consumer.stopLock.Lock()
	defer consumer.stopLock.Unlock()
	if consumer.status == Stopped {
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
