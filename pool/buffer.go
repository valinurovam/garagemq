package pool

import (
	"bytes"
	"sync"
)

// BufferPool represents a thread safe buffer pool
type BufferPool struct {
	sync.Pool
}

// NewBufferPool returns a new BufferPool
func NewBufferPool(bufferSize int) *BufferPool {
	return &BufferPool{
		sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, bufferSize))
			},
		},
	}
}

// Get gets a Buffer from the pool
func (bp *BufferPool) Get() *bytes.Buffer {
	return bp.Pool.Get().(*bytes.Buffer)
}

// Put returns the given Buffer to the pool.
func (bp *BufferPool) Put(b *bytes.Buffer) {
	b.Reset()
	bp.Pool.Put(b)
}
