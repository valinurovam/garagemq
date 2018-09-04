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
	serverMetrics := h.amqpServer.GetMetrics()
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.publish",
		Sample: serverMetrics.Publish.Track.GetDiffTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.deliver",
		Sample: serverMetrics.Deliver.Track.GetDiffTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.confirm",
		Sample: serverMetrics.Confirm.Track.GetDiffTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.acknowledge",
		Sample: serverMetrics.Ack.Track.GetDiffTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.traffic_in",
		Sample: serverMetrics.TrafficIn.Track.GetDiffTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.traffic_out",
		Sample: serverMetrics.TrafficOut.Track.GetDiffTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.get",
		Sample: serverMetrics.Get.Track.GetDiffTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.ready",
		Sample: serverMetrics.Ready.Track.GetTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.unacked",
		Sample: serverMetrics.Unacked.Track.GetTrack(),
	})
	response.Metrics = append(response.Metrics, &Metric{
		Name:   "server.total",
		Sample: serverMetrics.Total.Track.GetTrack(),
	})
}

func (h *OverviewHandler) populateCounters(response *OverviewResponse) {
	response.Counters["connections"] = len(h.amqpServer.GetConnections())
	response.Counters["channels"] = 0
	response.Counters["exchanges"] = 0
	response.Counters["queues"] = 0
	response.Counters["consumers"] = 0

	for _, vhost := range h.amqpServer.GetVhosts() {
		response.Counters["exchanges"] += len(vhost.GetExchanges())
		response.Counters["queues"] += len(vhost.GetQueues())
	}

	for _, conn := range h.amqpServer.GetConnections() {
		response.Counters["channels"] += len(conn.GetChannels())

		for _, ch := range conn.GetChannels() {
			response.Counters["consumers"] += ch.GetConsumersCount()
		}
	}
}
