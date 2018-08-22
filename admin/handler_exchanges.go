package admin

import (
	"net/http"

	"github.com/valinurovam/garagemq/server"
)

type ExchangesHandler struct {
	amqpServer *server.Server
}

type ExchangesResponse struct {
	Items []*Exchange `json:"items"`
}

type Exchange struct {
	Name       string `json:"name"`
	Vhost      string `json:"vhost"`
	Type       string `json:"type"`
	Durable    bool   `json:"durable"`
	Internal   bool   `json:"internal"`
	AutoDelete bool   `json:"auto_delete"`
}

func NewExchangesHandler(amqpServer *server.Server) http.Handler {
	return &ExchangesHandler{amqpServer: amqpServer}
}

func (h *ExchangesHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	response := &ExchangesResponse{}
	for vhostName, vhost := range h.amqpServer.GetVhosts() {
		for _, exchange := range vhost.GetExchanges() {
			response.Items = append(
				response.Items,
				&Exchange{
					Name:       exchange.GetName(),
					Vhost:      vhostName,
					Durable:    exchange.IsDurable(),
					Internal:   exchange.IsInternal(),
					AutoDelete: exchange.IsAutoDelete(),
					Type:       exchange.GetTypeAlias(),
				},
			)
		}
	}

	JSONResponse(resp, response, 200)
}
