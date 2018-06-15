package queue

import (
	"github.com/valinurovam/garagemq/safequeue"
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/consumer"
	"sync"
	"github.com/valinurovam/garagemq/qos"
	"errors"
	"fmt"
)

type Queue struct {
	safequeue.SafeQueue
	Name        string
	connId      uint64
	exclusive   bool
	autoDelete  bool
	durable     bool
	cmrLock     sync.Mutex
	consumers   []*consumer.Consumer
	call        chan bool
	wasConsumed bool
}

func NewQueue(name string, connId uint64, exclusive bool, autoDelete bool, durable bool) *Queue {
	return &Queue{
		SafeQueue:   *safequeue.NewSafeQueue(8192),
		Name:        name,
		connId:      connId,
		exclusive:   exclusive,
		autoDelete:  autoDelete,
		durable:     durable,
		call:        make(chan bool, 1),
		wasConsumed: false,
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
	queue.SafeQueue.Push(message)
	queue.callConsumers()
}

func (queue *Queue) Pop() *amqp.Message {
	if message := queue.SafeQueue.Pop(); message != nil {
		return message.(*amqp.Message)
	}

	return nil
}

func (queue *Queue) PopQos(qosList []*qos.AmqpQos) *amqp.Message {
	queue.SafeQueue.Lock()
	defer queue.SafeQueue.Unlock()
	if headItem := queue.SafeQueue.HeadItem(); headItem != nil {
		message := headItem.(*amqp.Message)
		allowed := true
		for _, q := range qosList {
			if !q.IsActive() {
				continue
			}
			if !q.Inc(1, uint32(message.BodySize)) {
				allowed = false
				break
			}
		}

		if allowed {
			queue.SafeQueue.DirtyPop()
			return message
		}
	}

	return nil
}

func (queue *Queue) AddConsumer(consumer *consumer.Consumer) {
	queue.wasConsumed = true
	queue.cmrLock.Lock()
	queue.consumers = append(queue.consumers, consumer)
	queue.cmrLock.Unlock()
	queue.callConsumers()
}

func (queue *Queue) RemoveConsumer(cTag string) {
	queue.cmrLock.Lock()
	for i, cmr := range queue.consumers {
		if cmr.ConsumerTag == cTag {
			queue.consumers = append(queue.consumers[:i], queue.consumers[i+1:]...)
		}
	}

	if len(queue.consumers) == 0 && queue.wasConsumed && queue.autoDelete{
		// TODO deleteQueue
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
	return queue.SafeQueue.Length();
}

func (queue *Queue) ConsumersCount() int {
	queue.cmrLock.Lock()
	defer queue.cmrLock.Unlock()
	return len(queue.consumers)
}

func (qA *Queue) EqualWithErr(qB *Queue) error {
	errTemplate := "inequivalent arg '%s' for queue '%s': received '%s' but current is '%s'"
	if qA.durable != qB.durable {
		return errors.New(fmt.Sprintf(errTemplate, "durable", qA.Name, qB.durable, qA.durable))
	}
	if qA.autoDelete != qB.autoDelete {
		return errors.New(fmt.Sprintf(errTemplate, "autoDelete", qA.Name, qB.autoDelete, qA.autoDelete))
	}
	if qA.exclusive != qB.exclusive {
		return errors.New(fmt.Sprintf(errTemplate, "exclusive", qA.Name, qB.exclusive, qA.exclusive))
	}
	return nil
}

func (q *Queue) IsExclusive() bool {
	return q.exclusive
}
func (q *Queue) ConnId() uint64 {
	return q.connId
}
