package queue

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/interfaces"
	"github.com/valinurovam/garagemq/metrics"
	"github.com/valinurovam/garagemq/qos"
	"github.com/valinurovam/garagemq/safequeue"
)

type MetricsState struct {
	Ready    *metrics.TrackCounter
	Unacked  *metrics.TrackCounter
	Total    *metrics.TrackCounter
	Incoming *metrics.TrackCounter
	Deliver  *metrics.TrackCounter
	Get      *metrics.TrackCounter
	Ack      *metrics.TrackCounter

	ServerReady   *metrics.TrackCounter
	ServerUnacked *metrics.TrackCounter
	ServerTotal   *metrics.TrackCounter
	ServerDeliver *metrics.TrackCounter
	ServerAck     *metrics.TrackCounter
}

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
	consumeExcl     bool
	call            chan bool
	wasConsumed     bool
	shardSize       int
	actLock         sync.RWMutex
	active          bool
	storage         interfaces.MsgStorage
	currentConsumer int
	metrics         *MetricsState
	autoDeleteQueue chan string
}

// NewQueue returns new instance of Queue
func NewQueue(name string, connID uint64, exclusive bool, autoDelete bool, durable bool, shardSize int, storage interfaces.MsgStorage, autoDeleteQueue chan string) *Queue {
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
		autoDeleteQueue: autoDeleteQueue,
		metrics: &MetricsState{
			Ready:    metrics.NewTrackCounter(0, true),
			Unacked:  metrics.NewTrackCounter(0, true),
			Total:    metrics.NewTrackCounter(0, true),
			Incoming: metrics.NewTrackCounter(0, true),
			Deliver:  metrics.NewTrackCounter(0, true),
			Get:      metrics.NewTrackCounter(0, true),
			Ack:      metrics.NewTrackCounter(0, true),

			ServerReady:   metrics.NewTrackCounter(0, true),
			ServerUnacked: metrics.NewTrackCounter(0, true),
			ServerTotal:   metrics.NewTrackCounter(0, true),
			ServerDeliver: metrics.NewTrackCounter(0, true),
			ServerAck:     metrics.NewTrackCounter(0, true),
		},
	}
}

// Start starts base queue loop to send events to consumers
// Current consumer to handle message from queue selected by round robin
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

// Stop stops main queue loop
func (queue *Queue) Stop() error {
	queue.active = false
	return nil
}

// GetName returns queue name
func (queue *Queue) GetName() string {
	return queue.name
}

// Push append message into queue tail and put it into message storage
// if queue is durable and message's persistent flag is true
// When push call with silent mode true - only append message into queue
// Silent mode used in server start
func (queue *Queue) Push(message *amqp.Message, silent bool) {
	queue.metrics.ServerTotal.Counter.Inc(1)
	queue.metrics.ServerReady.Counter.Inc(1)

	queue.metrics.Total.Counter.Inc(1)
	queue.metrics.Ready.Counter.Inc(1)

	if silent {
		queue.SafeQueue.Push(message)
		return
	}

	if queue.durable && message.IsPersistent() {
		// TODO handle error
		queue.storage.Add(message, queue.name)
	} else if message.ConfirmMeta != nil {
		message.ConfirmMeta.ActualConfirms++
	}

	queue.metrics.Incoming.Counter.Inc(1)

	queue.SafeQueue.Push(message)
	queue.callConsumers()
}

// Pop returns message from queue head without QOS check
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

// PopQos returns message from queue head with QOS check
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

// AckMsg accept ack event for message
func (queue *Queue) AckMsg(message *amqp.Message) {
	queue.actLock.RLock()
	defer queue.actLock.RUnlock()
	if !queue.active {
		return
	}
	if queue.durable && message.IsPersistent() {
		// TODO handle error
		queue.storage.Del(message, queue.name)
	}

	queue.metrics.Ack.Counter.Inc(1)
	queue.metrics.Total.Counter.Dec(1)

	queue.metrics.ServerAck.Counter.Inc(1)
	queue.metrics.ServerTotal.Counter.Dec(1)

	queue.metrics.Unacked.Counter.Dec(1)
	queue.metrics.ServerUnacked.Counter.Dec(1)
}

// Requeue add message into queue head
func (queue *Queue) Requeue(message *amqp.Message) {
	queue.actLock.RLock()
	defer queue.actLock.RUnlock()
	if !queue.active {
		return
	}
	message.DeliveryCount++
	queue.SafeQueue.PushHead(message)
	if queue.durable && message.IsPersistent() {
		// TODO handle error
		queue.storage.Update(message, queue.name)
	}
	queue.metrics.Ready.Counter.Inc(1)
	queue.metrics.ServerReady.Counter.Inc(1)

	queue.metrics.Unacked.Counter.Dec(1)
	queue.metrics.ServerUnacked.Counter.Dec(1)

	queue.callConsumers()
}

// Purge clean queue and message storage for durable queues
func (queue *Queue) Purge() (length uint64) {
	queue.SafeQueue.Lock()
	defer queue.SafeQueue.Unlock()
	length = queue.SafeQueue.DirtyLength()
	queue.SafeQueue.DirtyPurge()

	if queue.durable {
		queue.storage.PurgeQueue(queue.name)
	}
	queue.metrics.Total.Counter.Dec(int64(length))
	queue.metrics.Ready.Counter.Dec(int64(length))

	queue.metrics.ServerTotal.Counter.Dec(int64(length))
	queue.metrics.ServerReady.Counter.Dec(int64(length))
	return
}

// Delete cancel consumers and delete its messages from storage
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

	queue.metrics.Total.Counter.Dec(int64(length))
	queue.metrics.Ready.Counter.Dec(int64(length))

	queue.metrics.ServerTotal.Counter.Dec(int64(length))
	queue.metrics.ServerReady.Counter.Dec(int64(length))

	return length, nil
}

// AddConsumer add consumer to consumer messages with exclusive check
func (queue *Queue) AddConsumer(consumer interfaces.Consumer, exclusive bool) error {
	queue.cmrLock.Lock()
	defer queue.cmrLock.Unlock()

	if !queue.active {
		return fmt.Errorf("queue is not active")
	}
	queue.wasConsumed = true

	if len(queue.consumers) != 0 && (queue.consumeExcl || exclusive) {
		return fmt.Errorf("queue is busy by %d consumers", len(queue.consumers))
	}

	if exclusive {
		queue.consumeExcl = true
	}

	queue.consumers = append(queue.consumers, consumer)

	queue.callConsumers()
	return nil
}

// RemoveConsumer remove consumer
// If it was last consumer and queue is auto-delte - queue will be removed
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
		queue.consumeExcl = false
	} else {
		queue.currentConsumer = (queue.currentConsumer + 1) % cmrCount
	}

	if cmrCount == 0 && queue.wasConsumed && queue.autoDelete {
		queue.autoDeleteQueue <- queue.name
	}
}

// Send event to call next consumer, that it can receive next message
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

// Length returns queue length
func (queue *Queue) Length() uint64 {
	return queue.SafeQueue.Length()
}

// ConsumersCount returns consumers count
func (queue *Queue) ConsumersCount() int {
	queue.cmrLock.RLock()
	defer queue.cmrLock.RUnlock()
	return len(queue.consumers)
}

// EqualWithErr returns is given queue equal to current
func (queue *Queue) EqualWithErr(qB *Queue) error {
	errTemplate := "inequivalent arg '%s' for queue '%s': received '%t' but current is '%t'"
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

// Marshal returns raw representation of queue to store into storage
func (queue *Queue) Marshal(protoVersion string) (data []byte, err error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	if err = amqp.WriteShortstr(buf, queue.name); err != nil {
		return nil, err
	}

	var autoDelete byte
	if queue.autoDelete {
		autoDelete = 1
	} else {
		autoDelete = 0
	}

	if err = amqp.WriteOctet(buf, autoDelete); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unmarshal returns queue from storage raw bytes data
func (queue *Queue) Unmarshal(data []byte, protoVersion string) (err error) {
	buf := bytes.NewReader(data)
	if queue.name, err = amqp.ReadShortstr(buf); err != nil {
		return err
	}

	var autoDelete byte

	if autoDelete, err = amqp.ReadOctet(buf); err != nil {
		return err
	}
	queue.autoDelete = autoDelete > 0
	queue.durable = true
	return
}

// IsDurable returns is queue durable
func (queue *Queue) IsDurable() bool {
	return queue.durable
}

// IsExclusive returns is queue exclusive
func (queue *Queue) IsExclusive() bool {
	return queue.exclusive
}

// IsAutoDelete returns is queue should be deleted automatically
func (queue *Queue) IsAutoDelete() bool {
	return queue.autoDelete
}

// ConnID returns ID of connection that create this queue
func (queue *Queue) ConnID() uint64 {
	return queue.connID
}

// IsActive returns is queue's main loop is active
func (queue *Queue) IsActive() bool {
	return queue.active
}

// SetMetrics set external metrics
func (queue *Queue) SetMetrics(m *MetricsState) {
	queue.metrics = m
}

// GetMetrics returns metrics
func (queue *Queue) GetMetrics() *MetricsState {
	return queue.metrics
}
