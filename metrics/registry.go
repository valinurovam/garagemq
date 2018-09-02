package metrics

import "time"

var r *TrackRegistry

type TrackRegistry struct {
	Counters    map[string]*TrackCounter
	trackLength int
	trackTick   *time.Ticker
}

func NewTrackRegistry(trackLength int, d time.Duration) {
	r = &TrackRegistry{
		Counters:    make(map[string]*TrackCounter),
		trackLength: trackLength,
		trackTick:   time.NewTicker(d),
	}
	go r.trackMetrics()
}

func NewCounter(name string) *TrackCounter {
	c := NewTrackCounter(r.trackLength)
	r.Counters[name] = c
	return c
}

func GetCounter(name string) *TrackCounter {
	return r.Counters[name]
}

func (r *TrackRegistry) trackMetrics() {
	for range r.trackTick.C {
		for _, counter := range r.Counters {
			value := counter.Counter.Count()
			counter.Track.Add(value)
		}
	}
}
