package admin

import (
	"net/http"

	"github.com/valinurovam/garagemq/server"
)

type QueuesHandler struct {
	amqpServer *server.Server
}

type QueuesResponse struct {
	Items []*Queue `json:"items"`
}

type Queue struct {
	Name       string `json:"name"`
	Vhost      string `json:"vhost"`
	Length     uint64 `json:"length"`
	Durable    bool   `json:"durable"`
	AutoDelete bool   `json:"auto_delete"`
	Exclusive  bool   `json:"exclusive"`
}

func NewQueuesHandler(amqpServer *server.Server) http.Handler {
	return &QueuesHandler{amqpServer: amqpServer}
}

func (h *QueuesHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	response := &QueuesResponse{}
	for vhostName, vhost := range h.amqpServer.GetVhosts() {
		for _, queue := range vhost.GetQueues() {
			response.Items = append(
				response.Items,
				&Queue{
					Name:       queue.GetName(),
					Vhost:      vhostName,
					Durable:    queue.IsDurable(),
					AutoDelete: queue.IsAutoDelete(),
					Exclusive:  queue.IsExclusive(),
					Length:     queue.Length(),
				},
			)
		}
	}

	JSONResponse(resp, response, 200)
}
