package metrics

import (
	"sync"
	"time"
)

// TrackItem implements tracked item with value and timestamp
type TrackItem struct {
	Value     int64 `json:"value"`
	Timestamp int64 `json:"timestamp"`
}

// TrackBuffer implements buffer of TrackItems
type TrackBuffer struct {
	track     []*TrackItem
	trackDiff []*TrackItem
	lock      sync.RWMutex
	pos       int
	length    int

	last     *TrackItem
	lastDiff *TrackItem
}

// NewTrackBuffer returns new TrackBuffer
func NewTrackBuffer(length int) *TrackBuffer {
	return &TrackBuffer{
		track:     make([]*TrackItem, length),
		trackDiff: make([]*TrackItem, length),
		pos:       0,
		length:    length,
	}
}

// Add adds counter value into track
func (t *TrackBuffer) Add(item int64) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.track[t.pos] = &TrackItem{
		Value:     item,
		Timestamp: time.Now().Unix(),
	}

	var diffItem int64
	t.trackDiff[t.pos] = &TrackItem{
		Timestamp: time.Now().Unix(),
	}
	if t.pos > 0 {
		diffItem = item - t.track[t.pos-1].Value
	} else if t.track[t.length-1] != nil {
		diffItem = item - t.track[t.length-1].Value
	} else {
		diffItem = item
	}
	t.trackDiff[t.pos].Value = diffItem

	t.last = t.track[t.pos]
	t.lastDiff = t.trackDiff[t.pos]

	t.pos = (t.pos + 1) % t.length
}

// GetTrack returns current recorded track
func (t *TrackBuffer) GetTrack() []*TrackItem {
	t.lock.RLock()
	defer t.lock.RUnlock()
	track := make([]*TrackItem, t.length)
	copy(track[0:t.length-t.pos], t.track[t.pos:])
	copy(track[t.length-t.pos:], t.track[:t.pos])

	return track
}

// GetDiffTrack returns current recorded diff-track
func (t *TrackBuffer) GetDiffTrack() []*TrackItem {
	t.lock.RLock()
	defer t.lock.RUnlock()
	track := make([]*TrackItem, t.length)
	copy(track[0:t.length-t.pos], t.trackDiff[t.pos:])
	copy(track[t.length-t.pos:], t.trackDiff[:t.pos])

	return track
}

// GetLastTrackItem returns last tracked item
func (t *TrackBuffer) GetLastTrackItem() *TrackItem {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return t.last
}

// GetLastDiffTrackItem returns last tracked diff item
func (t *TrackBuffer) GetLastDiffTrackItem() *TrackItem {
	t.lock.RLock()
	defer t.lock.RUnlock()

	return t.lastDiff
}
