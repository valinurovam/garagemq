package admin

import (
	"net/http"

	"github.com/valinurovam/garagemq/server"
)

type ConnectionsHandler struct {
	amqpServer *server.Server
}

type ConnectionsResponse struct {
	Items []*Queue `json:"items"`
}

type Connection struct {
}

func NewConnectionsHandler(amqpServer *server.Server) http.Handler {
	return &QueuesHandler{amqpServer: amqpServer}
}

func (h *ConnectionsHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	response := &ConnectionsResponse{}
	for range h.amqpServer.GetConnections() {
		// map connections to response
	}

	JSONResponse(resp, response, 200)
}
