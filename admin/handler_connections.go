package admin

import (
	"net/http"
	"sort"

	"github.com/valinurovam/garagemq/metrics"
	"github.com/valinurovam/garagemq/server"
)

type ConnectionsHandler struct {
	amqpServer *server.Server
}

type ConnectionsResponse struct {
	Items []*Connection `json:"items"`
}

type Connection struct {
	ID            int                `json:"id"`
	Vhost         string             `json:"vhost"`
	Addr          string             `json:"addr"`
	ChannelsCount int                `json:"channels_count"`
	User          string             `json:"user"`
	Protocol      string             `json:"protocol"`
	FromClient    *metrics.TrackItem `json:"from_client"`
	ToClient      *metrics.TrackItem `json:"to_client"`
}

func NewConnectionsHandler(amqpServer *server.Server) http.Handler {
	return &ConnectionsHandler{amqpServer: amqpServer}
}

func (h *ConnectionsHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	response := &ConnectionsResponse{}
	for _, conn := range h.amqpServer.GetConnections() {
		response.Items = append(
			response.Items,
			&Connection{
				ID:            int(conn.GetID()),
				Vhost:         conn.GetVirtualHost().GetName(),
				Addr:          conn.GetRemoteAddr().String(),
				ChannelsCount: len(conn.GetChannels()),
				User:          conn.GetUsername(),
				Protocol:      h.amqpServer.GetProtoVersion(),
				FromClient:    conn.GetMetrics().TrafficIn.Track.GetLastDiffTrackItem(),
				ToClient:      conn.GetMetrics().TrafficOut.Track.GetLastDiffTrackItem(),
			},
		)
	}

	sort.Slice(
		response.Items,
		func(i, j int) bool {
			return response.Items[i].ID > response.Items[j].ID
		},
	)

	JSONResponse(resp, response, 200)
}
