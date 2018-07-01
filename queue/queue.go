package queue

import (
	"errors"
	"fmt"
	"sync"

	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/interfaces"
	"github.com/valinurovam/garagemq/msgstorage"
	"github.com/valinurovam/garagemq/qos"
	"github.com/valinurovam/garagemq/safequeue"
)

type Queue struct {
	safequeue.SafeQueue
	name            string
	connId          uint64
	exclusive       bool
	autoDelete      bool
	durable         bool
	cmrLock         sync.RWMutex
	consumers       []interfaces.Consumer
	call            chan bool
	wasConsumed     bool
	shardSize       int
	actLock         sync.Mutex
	active          bool
	storage         *msgstorage.MsgStorage
	currentConsumer int
}

func NewQueue(name string, connId uint64, exclusive bool, autoDelete bool, durable bool, shardSize int, storage *msgstorage.MsgStorage) *Queue {
	return &Queue{
		SafeQueue:       *safequeue.NewSafeQueue(shardSize),
		name:            name,
		connId:          connId,
		exclusive:       exclusive,
		autoDelete:      autoDelete,
		durable:         durable,
		call:            make(chan bool, 1),
		wasConsumed:     false,
		active:          false,
		shardSize:       shardSize,
		storage:         storage,
		currentConsumer: 0,
	}
}

func (queue *Queue) Start() {
	queue.active = true
	go func() {
		for _ = range queue.call {
			func() {
				queue.cmrLock.RLock()
				defer queue.cmrLock.RUnlock()
				cmrCount := len(queue.consumers)
				for i := 0; i < cmrCount; i++ {
					if !queue.active {
						return
					}
					queue.currentConsumer = (queue.currentConsumer + 1) % cmrCount
					cmr := queue.consumers[queue.currentConsumer]
					cmr.Consume()
				}
			}()
		}
	}()
}

func (queue *Queue) Stop() error {
	queue.active = false
	queue.cancelConsumers()
	return nil
}

func (queue *Queue) GetName() string {
	return queue.name
}

func (queue *Queue) Push(message *amqp.Message) {
	if queue.durable {
		queue.storage.Add(message, queue.name)
	}

	queue.SafeQueue.Push(message)
	queue.callConsumers()
}

func (queue *Queue) PushFromStorage(message *amqp.Message) {
	queue.SafeQueue.Push(message)
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

func (queue *Queue) AckMsg(id uint64) {
	if queue.durable {
		queue.storage.Del(id, queue.name)
	}
}

func (queue *Queue) Purge() (length uint64) {
	oldQueue := queue.SafeQueue
	oldQueue.Lock()
	defer oldQueue.Unlock()
	length = queue.SafeQueue.DirtyLength()
	queue.dirtyPurge()
	return
}

// TODO Looks like hack
func (queue *Queue) dirtyPurge() {
	queue.SafeQueue = *safequeue.NewSafeQueue(queue.shardSize)
}

func (queue *Queue) Delete(ifUnused bool, ifEmpty bool) (uint64, error) {
	queue.actLock.Lock()
	queue.cmrLock.RLock()
	oldQueue := queue.SafeQueue
	oldQueue.Lock()
	defer queue.actLock.Unlock()
	defer queue.cmrLock.RUnlock()
	defer oldQueue.Unlock()

	queue.active = false

	if ifUnused && len(queue.consumers) != 0 {
		return 0, errors.New("queue has consumers")
	}

	if ifEmpty && queue.SafeQueue.DirtyLength() != 0 {
		return 0, errors.New("queue has messages")
	}

	// TODO Purge durable queue

	queue.cancelConsumers()
	length := queue.SafeQueue.DirtyLength()
	queue.dirtyPurge()

	return length, nil
}

func (queue *Queue) AddConsumer(consumer interfaces.Consumer, exclusive bool) error {
	queue.cmrLock.Lock()
	defer queue.cmrLock.Unlock()

	if !queue.active {
		return errors.New(fmt.Sprintf("queue is not active"))
	}
	queue.wasConsumed = true

	if exclusive && len(queue.consumers) != 0 {
		return errors.New(fmt.Sprintf("queue is busy by %d consumers", len(queue.consumers)))
	}
	queue.consumers = append(queue.consumers, consumer)

	queue.callConsumers()
	return nil
}

func (queue *Queue) RemoveConsumer(cTag string) {
	queue.cmrLock.Lock()
	defer queue.cmrLock.Unlock()

	for i, cmr := range queue.consumers {
		if cmr.Tag() == cTag {
			queue.consumers = append(queue.consumers[:i], queue.consumers[i+1:]...)
			break
		}
	}
	cmrCount := len(queue.consumers)
	if cmrCount == 0 {
		queue.currentConsumer = 0
	} else {
		queue.currentConsumer = (queue.currentConsumer + 1) % cmrCount
	}

	if cmrCount == 0 && queue.wasConsumed && queue.autoDelete {
		// TODO deleteQueue
	}
}

func (queue *Queue) callConsumers() {
	select {
	case queue.call <- true:
	default:
	}
}

func (queue *Queue) cancelConsumers() {
	for _, cmr := range queue.consumers {
		cmr.Cancel()
	}
}

func (queue *Queue) Length() uint64 {
	return queue.SafeQueue.Length();
}

func (queue *Queue) ConsumersCount() int {
	queue.cmrLock.RLock()
	defer queue.cmrLock.RUnlock()
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

func (queue *Queue) IsDurable() bool {
	return queue.durable
}

func (queue *Queue) IsAutoDelete() bool {
	return queue.autoDelete
}

func (queue *Queue) IsExclusive() bool {
	return queue.exclusive
}

func (queue *Queue) ConnId() uint64 {
	return queue.connId
}

func (queue *Queue) IsActive() bool {
	return queue.active
}
