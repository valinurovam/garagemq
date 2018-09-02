package admin

import (
	"net/http"

	"github.com/valinurovam/garagemq/server"
)

type ConnectionsHandler struct {
	amqpServer *server.Server
}

type ConnectionsResponse struct {
	Items []*Connection `json:"items"`
}

type Connection struct {
	ID            int    `json:"id"`
	Vhost         string `json:"vhost"`
	Addr          string `json:"addr"`
	ChannelsCount int    `json:"channels_count"`
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
			},
		)
	}

	JSONResponse(resp, response, 200)
}
