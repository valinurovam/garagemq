package metrics

import (
	"sync"
	"time"
)

type TrackItem struct {
	Value     int64 `json:"value"`
	Timestamp int64 `json:"timestamp"`
}

type TrackBuffer struct {
	track     []*TrackItem
	trackDiff []*TrackItem
	lock      sync.RWMutex
	pos       int
	length    int
}

func NewTrackBuffer(length int) *TrackBuffer {
	return &TrackBuffer{
		track:     make([]*TrackItem, length),
		trackDiff: make([]*TrackItem, length),
		pos:       0,
		length:    length,
	}
}

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

	t.pos = (t.pos + 1) % t.length
}

func (t *TrackBuffer) GetTrack() []*TrackItem {
	t.lock.RLock()
	defer t.lock.RUnlock()
	track := make([]*TrackItem, t.length)
	copy(track[0:t.length-t.pos], t.track[t.pos:])
	copy(track[t.length-t.pos:], t.track[:t.pos])

	return track
}

func (t *TrackBuffer) GetDiffTrack() []*TrackItem {
	t.lock.RLock()
	defer t.lock.RUnlock()
	track := make([]*TrackItem, t.length)
	copy(track[0:t.length-t.pos], t.trackDiff[t.pos:])
	copy(track[t.length-t.pos:], t.trackDiff[:t.pos])

	return track
}
