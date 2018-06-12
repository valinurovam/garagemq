package queue

import (
	"github.com/valinurovam/garagemq/safequeue"
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/consumer"
	"sync"
)

type MessagesQueue interface {
	Push(item interface{})
	Pop() (res interface{})
	Length() int64
}

type Queue struct {
	messages  MessagesQueue
	cmrLock   sync.Mutex
	consumers []*consumer.Consumer
	Name      string
	call      chan bool
}

func NewQueue(name string) *Queue {
	return &Queue{
		messages: safequeue.NewSafeQueue(8192),
		Name:     name,
		call:     make(chan bool, 1),
	}
}

func (queue *Queue) Start() {
	go func() {
		for _ = range queue.call {
			for _, cmr := range queue.consumers {
				cmr.Consume()
			}
		}
	}()
}

func (queue *Queue) Push(message *amqp.Message) {
	queue.messages.Push(message)
	queue.callConsumers()
}

func (queue *Queue) Pop() *amqp.Message {
	if message := queue.messages.Pop(); message != nil {
		return message.(*amqp.Message)
	}

	return nil
}

func (queue *Queue) AddConsumer(consumer *consumer.Consumer) {
	queue.cmrLock.Lock()
	queue.consumers = append(queue.consumers, consumer)
	queue.cmrLock.Unlock()
	queue.callConsumers()
}

func (queue *Queue) RemoveConsumer(id uint64) {
	queue.cmrLock.Lock()
	for i, cmr := range queue.consumers {
		if cmr.Id == id {
			queue.consumers = append(queue.consumers[:i], queue.consumers[i+1:]...)
		}
	}
	queue.cmrLock.Unlock()
}

func (queue *Queue) callConsumers() {
	select {
	case queue.call <- true:
	default:
	}
}

func (queue *Queue) Length() int64 {
	return queue.messages.Length();
}
