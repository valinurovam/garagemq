package qos

import "sync"

type Qos struct {
	sync.Mutex
	prefetchCount uint16
	prefetchSize  uint32
	currentCount  uint16
	currentSize   uint32
}

func New(prefetchCount uint16, prefetchSize uint32) *Qos {
	return &Qos{
		prefetchCount: prefetchCount,
		prefetchSize:  prefetchSize,
	}
}

func (qos *Qos) IsActive() bool {
	return qos.prefetchCount == 0 && qos.prefetchSize == 0
}

func (qos *Qos) Inc(count uint16, size uint32) bool {
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

func (qos *Qos) Dec(count uint16, size uint32) {
	qos.Lock()
	defer qos.Unlock()

	qos.currentCount = qos.currentCount - count
	qos.currentSize = qos.currentSize - size
}

func (qos *Qos) Release() {
	qos.Lock()
	defer qos.Unlock()
	qos.currentCount = 0
	qos.currentSize = 0
}
