package server

import (
	"github.com/valinurovam/garagemq/amqp"
	"runtime"
	"os"
)

type Channel struct {
	id     uint16
	conn   *Connection
	server *Server
}

func NewChannel(id uint16, conn *Connection) (*Channel) {
	return &Channel{
		id:     id,
		conn:   conn,
		server: conn.server,
	}
}

func (channel *Channel) start()  {
	if channel.id == 0 {
		go channel.connectionStart()
	}
}

func (channel *Channel) connectionStart()  {
	var capabilities = amqp.Table{}
	capabilities.Set("publisher_confirms", false)
	capabilities.Set("basic.nack", true)
	var serverProps = amqp.Table{}
	serverProps.Set("product", []byte("garagemq"))
	serverProps.Set("version", []byte("0.1"))
	serverProps.Set("copyright", []byte("Alexander Valinurov, 2018"))
	serverProps.Set("capabilities", capabilities)
	serverProps.Set("platform", []byte(runtime.GOARCH))
	host, err := os.Hostname()
	if err != nil {
		serverProps.Set("host", []byte("UnknownHostError"))
	} else {
		serverProps.Set("host", []byte(host))
	}

	serverProps.Set("information", []byte("http://dispatchd.org"))


}
