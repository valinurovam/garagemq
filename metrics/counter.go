package metrics

import (
	"sync/atomic"
)

// Counter implements int64 counter
type Counter interface {
	Clear()
	Count() int64
	Dec(int64)
	Inc(int64)
}

// NilCounter is a no-op Counter.
type NilCounter struct{}

// Clear is a no-op.
func (NilCounter) Clear() {}

// Count is a no-op.
func (NilCounter) Count() int64 { return 0 }

// Dec is a no-op.
func (NilCounter) Dec(i int64) {}

// Inc is a no-op.
func (NilCounter) Inc(i int64) {}

// StandardCounter implements basic int64 counter with atomic ops
type StandardCounter struct {
	count int64
}

// NewCounter returns Nil or Standard counter
func NewCounter(isNil bool) Counter {
	if isNil {
		return NilCounter{}
	}
	return &StandardCounter{0}
}

// Clear clears counter
func (c *StandardCounter) Clear() {
	atomic.StoreInt64(&c.count, 0)
}

// Count returns current counter value
func (c *StandardCounter) Count() int64 {
	return atomic.LoadInt64(&c.count)
}

// Dec decrement current counter value
func (c *StandardCounter) Dec(i int64) {
	atomic.AddInt64(&c.count, -i)
}

// Inc increment current counter value
func (c *StandardCounter) Inc(i int64) {
	atomic.AddInt64(&c.count, i)
}

// TrackCounter implement counter with tracked values
type TrackCounter struct {
	Counter Counter
	Track   *TrackBuffer
}

// NewTrackCounter returns new TrackCounter
func NewTrackCounter(trackLength int, isNil bool) *TrackCounter {
	return &TrackCounter{
		Counter: NewCounter(isNil),
		Track:   NewTrackBuffer(trackLength),
	}
}
