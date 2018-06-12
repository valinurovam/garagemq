package safequeue

import (
	"sync"
)

// TODO Is that implementation faster? test simple slice queue
type safeQueue struct {
	sync.Mutex
	shards    [][]interface{}
	shardSize int
	tailIdx   int
	tail      []interface{}
	tailPos   int
	headIdx   int
	head      []interface{}
	headPos   int

	length int64
}

func NewSafeQueue(shardSize int) *safeQueue {
	queue := &safeQueue{
		shardSize: shardSize,
		shards:    [][]interface{}{make([]interface{}, shardSize)},
	}

	queue.tailIdx = 0
	queue.tail = queue.shards[queue.tailIdx]
	queue.headIdx = 0
	queue.head = queue.shards[queue.headIdx]
	return queue
}

func (queue *safeQueue) Push(item interface{}) {
	queue.Lock()

	queue.tail[queue.tailPos] = item
	queue.tailPos++
	queue.length++

	if queue.tailPos == queue.shardSize {
		queue.tailPos = 0
		queue.tailIdx = len(queue.shards)

		buffer := make([][]interface{}, len(queue.shards)+1)
		buffer[queue.tailIdx] = make([]interface{}, queue.shardSize)
		copy(buffer, queue.shards)

		queue.shards = buffer
		queue.tail = queue.shards[queue.tailIdx]
	}
	queue.Unlock()
}

func (queue *safeQueue) Pop() (res interface{}) {
	queue.Lock()

	res, queue.head[queue.headPos] = queue.head[queue.headPos], nil
	if res == nil {
		queue.Unlock()
		return res
	}
	queue.headPos++
	queue.length--
	if queue.headPos == queue.shardSize {

		buffer := make([][]interface{}, len(queue.shards)-1)
		copy(buffer, queue.shards[queue.headIdx+1:])

		queue.shards = buffer

		queue.headPos = 0
		queue.tailIdx--
		queue.head = queue.shards[queue.headIdx]
	}
	queue.Unlock()
	return
}

func (queue *safeQueue) Length() int64 {
	queue.Lock()
	defer queue.Unlock()
	return queue.length
}
