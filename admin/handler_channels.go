package admin

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/valinurovam/garagemq/metrics"
	"github.com/valinurovam/garagemq/server"
)

type ChannelsHandler struct {
	amqpServer *server.Server
}

type ChannelsResponse struct {
	Items []*Channel `json:"items"`
}

type Channel struct {
	ConnID    uint64
	ChannelID uint16
	Channel   string `json:"channel"`
	Vhost     string `json:"vhost"`
	User      string `json:"user"`
	Qos       string `json:"qos"`
	Confirm   bool   `json:"confirm"`

	Counters map[string]*metrics.TrackItem `json:"counters"`
}

func NewChannelsHandler(amqpServer *server.Server) http.Handler {
	return &ChannelsHandler{amqpServer: amqpServer}
}

func (h *ChannelsHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	response := &ChannelsResponse{}
	for _, conn := range h.amqpServer.GetConnections() {
		for chID, ch := range conn.GetChannels() {
			publish := ch.GetMetrics().Publish.Track.GetLastDiffTrackItem()
			confirm := ch.GetMetrics().Confirm.Track.GetLastDiffTrackItem()
			deliver := ch.GetMetrics().Deliver.Track.GetLastDiffTrackItem()
			get := ch.GetMetrics().Get.Track.GetLastDiffTrackItem()
			ack := ch.GetMetrics().Acknowledge.Track.GetLastDiffTrackItem()
			unacked := ch.GetMetrics().Unacked.Track.GetLastTrackItem()

			response.Items = append(
				response.Items,
				&Channel{
					ConnID:    conn.GetID(),
					ChannelID: chID,
					Channel:   fmt.Sprintf("%s (%d)", conn.GetRemoteAddr().String(), chID),
					Vhost:     conn.GetVirtualHost().GetName(),
					User:      conn.GetUsername(),
					Qos:       fmt.Sprintf("%d / %d", ch.GetQos().PrefetchCount(), ch.GetQos().PrefetchSize()),
					Counters: map[string]*metrics.TrackItem{
						"publish": publish,
						"confirm": confirm,
						"deliver": deliver,
						"get":     get,
						"ack":     ack,
						"unacked": unacked,
					},
				},
			)
		}
	}

	sort.Slice(
		response.Items,
		func(i, j int) bool {
			if response.Items[i].ConnID != response.Items[j].ConnID {
				return response.Items[i].ConnID > response.Items[j].ConnID
			} else {
				return response.Items[i].ChannelID > response.Items[j].ChannelID
			}
		},
	)

	JSONResponse(resp, response, 200)
}
