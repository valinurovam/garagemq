package metrics

import "github.com/rcrowley/go-metrics"

type TrackCounter struct {
	Counter metrics.Counter
	Track   *TrackBuffer
}

func NewTrackCounter(trackLength int) *TrackCounter {
	return &TrackCounter{
		Counter: metrics.NewCounter(),
		Track:   NewTrackBuffer(trackLength),
	}
}
