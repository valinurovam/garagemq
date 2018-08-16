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
	started = iota
	stopped
	paused
)

var cid uint64

// Consumer implements AMQP consumer
type Consumer struct {
	ID          uint64
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

// NewConsumer returns new instance of Consumer
func NewConsumer(queueName string, consumerTag string, noAck bool, channel interfaces.Channel, queue *queue.Queue, qos []*qos.AmqpQos) *Consumer {
	id := atomic.AddUint64(&cid, 1)
	if consumerTag == "" {
		consumerTag = generateTag(id)
	}
	return &Consumer{
		ID:          id,
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

// Start starting consumer to fetch messages from queue
func (consumer *Consumer) Start() {
	consumer.status = started
	go consumer.startConsume()
	consumer.Consume()
}

// startConsume waiting a signal from consume channel and try to pop message from queue
// if not set noAck consumer pop message with qos rules and add message to unacked message queue
func (consumer *Consumer) startConsume() {
	var message *amqp.Message
	for range consumer.consume {
		if consumer.status == stopped {
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

// Pause pause consumer, used by channel.flow change
func (consumer *Consumer) Pause() {
	consumer.status = paused
}

// UnPause unpause consumer, used by channel.flow change
func (consumer *Consumer) UnPause() {
	consumer.status = started
}

// Consume send signal into consumer channel, than consumer can try to pop message from queue
func (consumer *Consumer) Consume() {
	if consumer.status == stopped || consumer.status == paused {
		return
	}

	select {
	case consumer.consume <- true:
	default:
	}
}

// Stop stops consumer and remove it from queue consumers list
func (consumer *Consumer) Stop() {
	consumer.stopLock.Lock()
	defer consumer.stopLock.Unlock()
	if consumer.status == stopped {
		return
	}
	consumer.status = stopped
	consumer.queue.RemoveConsumer(consumer.ConsumerTag)
	close(consumer.consume)
}

// Cancel stops consumer and send basic.cancel method to the client
func (consumer *Consumer) Cancel() {
	consumer.Stop()
	consumer.channel.SendMethod(&amqp.BasicCancel{ConsumerTag: consumer.ConsumerTag, NoWait: true})
}

// Tag returns consumer tag
func (consumer *Consumer) Tag() string {
	return consumer.ConsumerTag
}

// Qos returns consumer qos rules
func (consumer *Consumer) Qos() []*qos.AmqpQos {
	return consumer.qos
}
