package admin

import (
	"fmt"
	"net/http"

	"github.com/valinurovam/garagemq/server"
)

type AdminServer struct {
	s *http.Server
}

func NewAdminServer(amqpServer *server.Server, host string, port string) *AdminServer {
	http.Handle("/", http.FileServer(http.Dir("admin-frontend/build")))
	http.Handle("/overview", NewOverviewHandler(amqpServer))
	http.Handle("/exchanges", NewExchangesHandler(amqpServer))
	http.Handle("/queues", NewQueuesHandler(amqpServer))
	http.Handle("/connections", NewConnectionsHandler(amqpServer))
	http.Handle("/bindings", NewBindingsHandler(amqpServer))
	http.Handle("/channels", NewChannelsHandler(amqpServer))

	adminServer := &AdminServer{}
	adminServer.s = &http.Server{
		Addr: fmt.Sprintf("%s:%s", host, port),
	}

	return adminServer
}

func (server *AdminServer) Start() error {
	return server.s.ListenAndServe()
}
