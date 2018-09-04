package admin

import (
	"net/http"

	"github.com/valinurovam/garagemq/server"
)

type BindingsHandler struct {
	amqpServer *server.Server
}

type BindingsResponse struct {
	Items []*Binding `json:"items"`
}

type Binding struct {
	Queue      string `json:"queue"`
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key"`
}

func NewBindingsHandler(amqpServer *server.Server) http.Handler {
	return &BindingsHandler{amqpServer: amqpServer}
}

func (h *BindingsHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	response := &BindingsResponse{}
	req.ParseForm()
	vhName := req.Form.Get("vhost")
	exName := req.Form.Get("exchange")

	vhost := h.amqpServer.GetVhost(vhName)
	if vhost == nil {
		JSONResponse(resp, response, 200)
		return
	}

	exchange := vhost.GetExchange(exName)
	if exchange == nil {
		JSONResponse(resp, response, 200)
		return
	}

	for _, bind := range exchange.GetBindings() {
		response.Items = append(
			response.Items,
			&Binding{
				Queue:      bind.GetQueue(),
				Exchange:   bind.GetExchange(),
				RoutingKey: bind.GetExchange(),
			},
		)
	}

	JSONResponse(resp, response, 200)
}
