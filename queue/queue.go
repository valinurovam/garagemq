package queue

import (
	"github.com/valinurovam/garagemq/safequeue"
	"github.com/valinurovam/garagemq/amqp"
	"sync"
	"github.com/valinurovam/garagemq/qos"
	"errors"
	"fmt"
	"github.com/valinurovam/garagemq/interfaces"
)

type Queue struct {
	safequeue.SafeQueue
	name        string
	connId      uint64
	exclusive   bool
	autoDelete  bool
	durable     bool
	cmrLock     sync.Mutex
	consumers   []interfaces.Consumer
	call        chan bool
	wasConsumed bool
	shardSize   int
}

func NewQueue(name string, connId uint64, exclusive bool, autoDelete bool, durable bool, shardSize int) interfaces.AmqpQueue {
	return &Queue{
		SafeQueue:   *safequeue.NewSafeQueue(shardSize),
		name:        name,
		connId:      connId,
		exclusive:   exclusive,
		autoDelete:  autoDelete,
		durable:     durable,
		call:        make(chan bool, 1),
		wasConsumed: false,
		shardSize:   shardSize,
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

func (queue *Queue) GetName() string {
	return queue.name
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

func (queue *Queue) Purge() (length uint64) {
	queue.SafeQueue.Lock()
	length = queue.SafeQueue.Length()
	defer queue.SafeQueue.Unlock()
	queue.dirtyPurge()
	return
}

func (queue *Queue) dirtyPurge() {
	queue.SafeQueue = *safequeue.NewSafeQueue(queue.shardSize)
}

func (queue *Queue) AddConsumer(consumer interfaces.Consumer, exclusive bool) error {
	queue.wasConsumed = true
	queue.cmrLock.Lock()
	if exclusive && len(queue.consumers) != 0 {
		return errors.New("queue is busy by exclusive consumer")
	}
	queue.consumers = append(queue.consumers, consumer)
	queue.cmrLock.Unlock()
	queue.callConsumers()
	return nil
}

func (queue *Queue) RemoveConsumer(cTag string) {
	queue.cmrLock.Lock()
	for i, cmr := range queue.consumers {
		if cmr.Tag() == cTag {
			queue.consumers = append(queue.consumers[:i], queue.consumers[i+1:]...)
		}
	}

	if len(queue.consumers) == 0 && queue.wasConsumed && queue.autoDelete {
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

func (queue *Queue) Length() uint64 {
	return queue.SafeQueue.Length();
}

func (queue *Queue) ConsumersCount() int {
	queue.cmrLock.Lock()
	defer queue.cmrLock.Unlock()
	return len(queue.consumers)
}

func (qA *Queue) EqualWithErr(qB interfaces.AmqpQueue) error {
	errTemplate := "inequivalent arg '%s' for queue '%s': received '%s' but current is '%s'"
	if qA.durable != qB.IsDurable() {
		return errors.New(fmt.Sprintf(errTemplate, "durable", qA.name, qB.IsDurable(), qA.durable))
	}
	if qA.autoDelete != qB.IsAutoDelete() {
		return errors.New(fmt.Sprintf(errTemplate, "autoDelete", qA.name, qB.IsAutoDelete(), qA.autoDelete))
	}
	if qA.exclusive != qB.IsExclusive() {
		return errors.New(fmt.Sprintf(errTemplate, "exclusive", qA.name, qB.IsExclusive(), qA.exclusive))
	}
	return nil
}

func (q *Queue) IsDurable() bool {
	return q.durable
}

func (q *Queue) IsAutoDelete() bool {
	return q.autoDelete
}

func (q *Queue) IsExclusive() bool {
	return q.exclusive
}

func (q *Queue) ConnId() uint64 {
	return q.connId
}
