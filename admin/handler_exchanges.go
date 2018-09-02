package admin

import (
	"net/http"
	"sort"

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
			name := exchange.GetName()
			if name == "" {
				name = "(AMQP default)"
			}
			response.Items = append(
				response.Items,
				&Exchange{
					Name:       name,
					Vhost:      vhostName,
					Durable:    exchange.IsDurable(),
					Internal:   exchange.IsInternal(),
					AutoDelete: exchange.IsAutoDelete(),
					Type:       exchange.GetTypeAlias(),
				},
			)
		}
	}

	sort.Slice(response.Items, func(i, j int) bool { return response.Items[i].Name < response.Items[j].Name })
	JSONResponse(resp, response, 200)
}
