package admin

import (
	"net/http"

	"github.com/valinurovam/garagemq/metrics"
	"github.com/valinurovam/garagemq/server"
)

type OverviewHandler struct {
	amqpServer *server.Server
}

type OverviewResponse struct {
	Metrics  []*Metric      `json:"metrics"`
	Counters map[string]int `json:"counters"`
}

type Metric struct {
	Name   string               `json:"name"`
	Sample []*metrics.TrackItem `json:"sample"`
}

func NewOverviewHandler(amqpServer *server.Server) http.Handler {
	return &OverviewHandler{amqpServer: amqpServer}
}

func (h *OverviewHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	response := &OverviewResponse{
		Counters: make(map[string]int),
	}
	h.populateMetrics(response)
	h.populateCounters(response)

	JSONResponse(resp, response, 200)
}

func (h *OverviewHandler) populateMetrics(response *OverviewResponse) {
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.publish",
		Sample: metrics.GetCounter("server.publish").Track.GetDiffTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.deliver",
		Sample: metrics.GetCounter("server.deliver").Track.GetDiffTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.confirm",
		Sample: metrics.GetCounter("server.confirm").Track.GetDiffTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.acknowledge",
		Sample: metrics.GetCounter("server.acknowledge").Track.GetDiffTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.traffic_in",
		Sample: metrics.GetCounter("server.traffic_in").Track.GetDiffTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.traffic_out",
		Sample: metrics.GetCounter("server.traffic_out").Track.GetDiffTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.get",
		Sample: metrics.GetCounter("server.get").Track.GetDiffTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.ready",
		Sample: metrics.GetCounter("server.ready").Track.GetTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.unacked",
		Sample: metrics.GetCounter("server.unacked").Track.GetTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.total",
		Sample: metrics.GetCounter("server.total").Track.GetTrack(),
	})
}

func (h *OverviewHandler) populateCounters(response *OverviewResponse) {
	response.Counters["connections"] = len(h.amqpServer.GetConnections())
	response.Counters["channels"] = 0
	response.Counters["exchanges"] = 0
	response.Counters["queues"] = 0
	response.Counters["consumers"] = 0

	for _, vhost := range h.amqpServer.GetVhosts() {
		response.Counters["exchanges"] = response.Counters["exchanges"] + len(vhost.GetExchanges())
		response.Counters["queues"] = response.Counters["queues"] + len(vhost.GetQueues())
	}

	for _, conn := range h.amqpServer.GetConnections() {
		response.Counters["channels"] = response.Counters["channels"] + len(conn.GetChannels())

		for _, ch := range conn.GetChannels() {
			response.Counters["consumers"] = response.Counters["consumers"] + ch.GetConsumersCount()
		}
	}
}
