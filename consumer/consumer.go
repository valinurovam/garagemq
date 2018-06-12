package consumer

import (
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/qos"
	"github.com/valinurovam/garagemq/interfaces"
	"fmt"
	"time"
	"sync/atomic"
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
	qos         []*qos.Qos
	consume     chan bool
}

func New(queueName string, consumerTag string, noAck bool, channel interfaces.Channel, queue interfaces.AmqpQueue, qos []*qos.Qos) *Consumer {
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
		message := consumer.queue.Pop()
		if message == nil {
			continue
		}
		consumer.channel.SendContent(&amqp.BasicDeliver{
			ConsumerTag: consumer.ConsumerTag,
			DeliveryTag: consumer.channel.NextDeliveryTag(),
			Redelivered: false,
			Exchange:    message.Exchange,
			RoutingKey:  message.RoutingKey,
		}, message)

		consumer.Consume()
	}
}

func (consumer *Consumer) Consume() {
	select {
	case consumer.consume <- true:
	default:
	}
}

func (consumer *Consumer) Stop() {
	consumer.status = Stopped
}
