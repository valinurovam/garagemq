package qos

import "sync"

type AmqpQos struct {
	sync.Mutex
	prefetchCount uint16
	prefetchSize  uint32
	currentCount  uint16
	currentSize   uint32
}

func New(prefetchCount uint16, prefetchSize uint32) *AmqpQos {
	return &AmqpQos{
		prefetchCount: prefetchCount,
		prefetchSize:  prefetchSize,
	}
}

func (qos *AmqpQos) Update(prefetchCount uint16, prefetchSize uint32) {
	qos.prefetchCount = prefetchCount
	qos.prefetchSize = prefetchSize
}

func (qos *AmqpQos) IsActive() bool {
	return qos.prefetchCount != 0 || qos.prefetchSize != 0
}

func (qos *AmqpQos) Inc(count uint16, size uint32) bool {
	qos.Lock()
	defer qos.Unlock()

	newCount := qos.currentCount + count
	newSize := qos.currentSize + size

	if (qos.prefetchCount == 0 || newCount <= qos.prefetchCount) && (qos.prefetchSize == 0 || newSize <= qos.prefetchSize) {
		qos.currentCount = newCount
		qos.currentSize = newSize
		return true
	}

	return false
}

func (qos *AmqpQos) Dec(count uint16, size uint32) {
	qos.Lock()
	defer qos.Unlock()

	qos.currentCount = qos.currentCount - count
	qos.currentSize = qos.currentSize - size
}

func (qos *AmqpQos) Release() {
	qos.Lock()
	defer qos.Unlock()
	qos.currentCount = 0
	qos.currentSize = 0
}
