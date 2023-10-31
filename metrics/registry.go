package metrics

import (
	"time"

	"github.com/sasha-s/go-deadlock"
)

var r *TrackRegistry

// TrackRegistry is a registry of track counters or other track metrics
type TrackRegistry struct {
	cntLock     deadlock.Mutex
	Counters    map[string]*TrackCounter
	trackLength int
	trackTick   *time.Ticker
	isNil       bool
}

// NewTrackRegistry returns new TrackRegistry
// Each counter will be tracked every d duration
// Each counter track length will be trackLength items
func NewTrackRegistry(trackLength int, d time.Duration, isNil bool) {
	r = &TrackRegistry{
		Counters:    make(map[string]*TrackCounter),
		trackLength: trackLength,
		trackTick:   time.NewTicker(d),
		isNil:       isNil,
	}
	if !isNil {
		go r.trackMetrics()
	}

}

// Destroy release current registry
func Destroy() {
	r = nil
}

// AddCounter add counter into registry andd return it
// TODO check if already exists
func AddCounter(name string) *TrackCounter {
	r.cntLock.Lock()
	defer r.cntLock.Unlock()

	c := NewTrackCounter(r.trackLength, r.isNil)
	r.Counters[name] = c
	return c
}

// GetCounter returns counter by name
func GetCounter(name string) *TrackCounter {
	return r.Counters[name]
}

func (r *TrackRegistry) trackMetrics() {
	for range r.trackTick.C {
		r.cntLock.Lock()
		for _, counter := range r.Counters {
			value := counter.Counter.Count()
			counter.Track.Add(value)
		}
		r.cntLock.Unlock()
	}
}
