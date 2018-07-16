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

// Queue is an implementation of the AMQP-queue entity
type Queue struct {
	safequeue.SafeQueue
	name            string
	connID          uint64
	exclusive       bool
	autoDelete      bool
	durable         bool
	cmrLock         sync.RWMutex
	consumers       []interfaces.Consumer
	call            chan bool
	wasConsumed     bool
	shardSize       int
	actLock         sync.RWMutex
	active          bool
	storage         *msgstorage.MsgStorage
	currentConsumer int
}

// NewQueue returns new instance of Queue
func NewQueue(name string, connID uint64, exclusive bool, autoDelete bool, durable bool, shardSize int, storage *msgstorage.MsgStorage) *Queue {
	return &Queue{
		SafeQueue:       *safequeue.NewSafeQueue(shardSize),
		name:            name,
		connID:          connID,
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
		for range queue.call {
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
	return nil
}

func (queue *Queue) GetName() string {
	return queue.name
}

func (queue *Queue) Push(message *amqp.Message, silent bool) {
	if silent {
		queue.SafeQueue.Push(message)
		return
	}

	if queue.durable && message.IsPersistent() {
		queue.storage.Add(message, queue.name)
	}

	queue.SafeQueue.Push(message)
	queue.callConsumers()
}

func (queue *Queue) Pop() *amqp.Message {
	queue.actLock.RLock()
	defer queue.actLock.RUnlock()
	if !queue.active {
		return nil
	}
	if message := queue.SafeQueue.Pop(); message != nil {
		return message.(*amqp.Message)
	}

	return nil
}

func (queue *Queue) PopQos(qosList []*qos.AmqpQos) *amqp.Message {
	queue.actLock.RLock()
	defer queue.actLock.RUnlock()
	if !queue.active {
		return nil
	}
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

func (queue *Queue) AckMsg(message *amqp.Message) {
	if queue.durable && message.IsPersistent(){
		queue.storage.Del(message.Id, queue.name)
	}
}

func (queue *Queue) Requeue(message *amqp.Message) {
	message.DeliveryCount++
	queue.SafeQueue.PushHead(message)
	if queue.durable && message.IsPersistent() {
		queue.storage.Update(message, queue.name)
	}
	queue.callConsumers()
}

func (queue *Queue) Purge() (length uint64) {
	queue.SafeQueue.Lock()
	defer queue.SafeQueue.Unlock()
	length = queue.SafeQueue.DirtyLength()
	queue.SafeQueue.DirtyPurge()

	if queue.durable {
		queue.storage.PurgeQueue(queue.name)
	}
	return
}

func (queue *Queue) Delete(ifUnused bool, ifEmpty bool) (uint64, error) {
	queue.actLock.Lock()
	queue.cmrLock.Lock()
	queue.SafeQueue.Lock()
	defer queue.actLock.Unlock()
	defer queue.cmrLock.Unlock()
	defer queue.SafeQueue.Unlock()

	queue.active = false

	if ifUnused && len(queue.consumers) != 0 {
		return 0, errors.New("queue has consumers")
	}

	if ifEmpty && queue.SafeQueue.DirtyLength() != 0 {
		return 0, errors.New("queue has messages")
	}

	queue.cancelConsumers()
	length := queue.SafeQueue.DirtyLength()

	if queue.durable {
		queue.storage.PurgeQueue(queue.name)
	}

	return length, nil
}

func (queue *Queue) AddConsumer(consumer interfaces.Consumer, exclusive bool) error {
	queue.cmrLock.Lock()
	defer queue.cmrLock.Unlock()

	if !queue.active {
		return fmt.Errorf(("queue is not active"))
	}
	queue.wasConsumed = true

	if exclusive && len(queue.consumers) != 0 {
		return fmt.Errorf("queue is busy by %d consumers", len(queue.consumers))
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

func (queue *Queue) EqualWithErr(qB *Queue) error {
	errTemplate := "inequivalent arg '%s' for queue '%s': received '%s' but current is '%s'"
	if queue.durable != qB.IsDurable() {
		return fmt.Errorf(errTemplate, "durable", queue.name, qB.IsDurable(), queue.durable)
	}
	if queue.autoDelete != qB.autoDelete {
		return fmt.Errorf(errTemplate, "autoDelete", queue.name, qB.autoDelete, queue.autoDelete)
	}
	if queue.exclusive != qB.IsExclusive() {
		return fmt.Errorf(errTemplate, "exclusive", queue.name, qB.IsExclusive(), queue.exclusive)
	}
	return nil
}

func (queue *Queue) MsgStorage() *msgstorage.MsgStorage {
	return queue.storage
}

func (queue *Queue) Marshal(protoVersion string) []byte {
	return []byte(queue.name)
}

func (queue *Queue) Unmarshal(data []byte, protoVersion string) string {
	return string(data)
}

func (queue *Queue) IsDurable() bool {
	return queue.durable
}

func (queue *Queue) IsExclusive() bool {
	return queue.exclusive
}

func (queue *Queue) ConnID() uint64 {
	return queue.connID
}

func (queue *Queue) IsActive() bool {
	return queue.active
}
