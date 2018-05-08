package server

import (
	"github.com/valinurovam/garagemq/amqp"
	"bytes"
	"github.com/sirupsen/logrus"
)

type Channel struct {
	id       uint16
	conn     *Connection
	server   *Server
	incoming chan *amqp.Frame
	outgoing chan *amqp.Frame
}

func NewChannel(id uint16, conn *Connection) (*Channel) {
	return &Channel{
		id:       id,
		conn:     conn,
		server:   conn.server,
		incoming: make(chan *amqp.Frame, 100),
		outgoing: conn.outgoing,
	}
}

func (channel *Channel) start() {
	if channel.id == 0 {
		go channel.connectionStart()
	}
}

func (channel *Channel) sendMethod(method amqp.Method) {
	rawMethod := bytes.NewBuffer([]byte{})
	if err := amqp.WriteMethod(rawMethod, method, channel.server.protoVersion); err != nil {
		logrus.WithError(err).Error("Error")
	}
	channel.outgoing <- &amqp.Frame{Type: byte(amqp.FrameMethod), ChannelId: channel.id, Payload: rawMethod.Bytes()}
}

func (channel *Channel) connectionStart() {
	//var capabilities = amqp.NewTable()
	//capabilities.Set("publisher_confirms", false)
	//capabilities.Set("basic.nack", true)
	var serverProps = amqp.Table{}
	serverProps["product"] = "garagemq"
	//serverProps.Set("product", "garagemq")
	//serverProps.Set("version", "0.1")
	//serverProps.Set("copyright", "Alexander Valinurov, 2018")
	////serverProps.Set("capabilities", capabilities)
	//serverProps.Set("platform", runtime.GOARCH)
	//host, err := os.Hostname()
	//if err != nil {
	//	serverProps.Set("host", "UnknownHostError")
	//} else {
	//	serverProps.Set("host", host)
	//}

	//serverProps.Set("information", []byte("http://dispatchd.org"))

	var method = amqp.ConnectionStart{0, 9, &serverProps, []byte("PLAIN"), []byte("en_US")}
	channel.sendMethod(&method)
}
