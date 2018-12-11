package safequeue

import (
	"sync"

	"github.com/valinurovam/garagemq/amqp"
)

// We change item's type from {}interface to *amqp.Message, cause we know that we use safequeue only in AMQP context
// TODO Move safe queue into amqp pacakge

// TODO Is that implementation faster? test simple slice queue
type SafeQueue struct {
	sync.Mutex
	shards    [][]*amqp.Message
	shardSize int
	tailIdx   int
	tail      []*amqp.Message
	tailPos   int
	headIdx   int
	head      []*amqp.Message
	headPos   int
	length    uint64
}

func NewSafeQueue(shardSize int) *SafeQueue {
	queue := &SafeQueue{
		shardSize: shardSize,
		shards:    [][]*amqp.Message{make([]*amqp.Message, shardSize)},
	}

	queue.tailIdx = 0
	queue.tail = queue.shards[queue.tailIdx]
	queue.headIdx = 0
	queue.head = queue.shards[queue.headIdx]
	return queue
}

func (queue *SafeQueue) Push(item *amqp.Message) {
	queue.Lock()
	defer queue.Unlock()

	queue.tail[queue.tailPos] = item
	queue.tailPos++
	queue.length++

	if queue.tailPos == queue.shardSize {
		queue.tailPos = 0
		queue.tailIdx = len(queue.shards)

		buffer := make([][]*amqp.Message, len(queue.shards)+1)
		buffer[queue.tailIdx] = make([]*amqp.Message, queue.shardSize)
		copy(buffer, queue.shards)

		queue.shards = buffer
		queue.tail = queue.shards[queue.tailIdx]
	}
}

func (queue *SafeQueue) PushHead(item *amqp.Message) {
	queue.Lock()
	defer queue.Unlock()

	if queue.headPos == 0 {
		buffer := make([][]*amqp.Message, len(queue.shards)+1)
		copy(buffer[1:], queue.shards)
		buffer[queue.headIdx] = make([]*amqp.Message, queue.shardSize)

		queue.shards = buffer
		queue.tailIdx++
		queue.headPos = queue.shardSize
		queue.tail = queue.shards[queue.tailIdx]
		queue.head = queue.shards[queue.headIdx]
	}
	queue.length++
	queue.headPos--
	queue.head[queue.headPos] = item
}

func (queue *SafeQueue) Pop() (item *amqp.Message) {
	queue.Lock()
	item = queue.DirtyPop()
	queue.Unlock()
	return
}

func (queue *SafeQueue) DirtyPop() (item *amqp.Message) {
	item, queue.head[queue.headPos] = queue.head[queue.headPos], nil
	if item == nil {
		return item
	}
	queue.headPos++
	queue.length--
	if queue.headPos == queue.shardSize {
		buffer := make([][]*amqp.Message, len(queue.shards)-1)
		copy(buffer, queue.shards[queue.headIdx+1:])

		queue.shards = buffer

		queue.headPos = 0
		queue.tailIdx--
		queue.head = queue.shards[queue.headIdx]
	}
	return
}

func (queue *SafeQueue) Length() uint64 {
	queue.Lock()
	defer queue.Unlock()
	return queue.length
}

func (queue *SafeQueue) DirtyLength() uint64 {
	return queue.length
}

func (queue *SafeQueue) HeadItem() (res *amqp.Message) {
	return queue.head[queue.headPos]
}

func (queue *SafeQueue) DirtyPurge() {
	queue.shards = [][]*amqp.Message{make([]*amqp.Message, queue.shardSize)}
	queue.tailIdx = 0
	queue.tail = queue.shards[queue.tailIdx]
	queue.headIdx = 0
	queue.head = queue.shards[queue.headIdx]
	queue.length = 0
}

func (queue *SafeQueue) Purge() {
	queue.Lock()
	defer queue.Unlock()
	queue.DirtyPurge()
}
