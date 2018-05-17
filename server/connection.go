package server

import (
	"net"
	"fmt"
	"bytes"
	log "github.com/sirupsen/logrus"
	"sync/atomic"
	"github.com/valinurovam/garagemq/amqp"
)

const (
	ConnStart = iota
)

type Connection struct {
	id       int64
	server   *Server
	netConn  net.Conn
	logger   *log.Entry
	channels map[uint16]*Channel
	outgoing chan *amqp.Frame
}

func NewConnection(server *Server, netConn net.Conn) (connection *Connection) {
	connection = &Connection{
		id:       atomic.AddInt64(&server.connSeq, 1),
		server:   server,
		netConn:  netConn,
		channels: make(map[uint16]*Channel),
		outgoing: make(chan *amqp.Frame, 100),
	}

	connection.logger = log.WithFields(log.Fields{
		"conn_id": connection.id,
	})

	return
}

func (conn *Connection) handleConnection() {
	buf := make([]byte, 8)
	_, err := conn.netConn.Read(buf)
	if err != nil {
		fmt.Println("Error accepting: ", err.Error())
		return
	}

	// The client MUST start a new connection by sending a protocol header
	var supported = []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}
	if bytes.Compare(buf, supported) != 0 {
		conn.logger.WithFields(log.Fields{
			"given":     buf,
			"supported": supported,
		}).Warn("Unsupported protocol")
		conn.netConn.Write(supported)
		conn.netConn.Close()
		return
	}

	conn.channels[0] = NewChannel(0, conn)
	conn.channels[0].start()
	go conn.handleOutgoing()
	go conn.handleIncoming()
}

func (conn *Connection) handleOutgoing() {
	for {
		var frame = <-conn.outgoing
		amqp.WriteFrame(conn.netConn, frame)
	}
}

func (conn *Connection) handleIncoming() {
	for {
		frame, err := amqp.ReadFrame(conn.netConn)
		if err != nil {
			conn.logger.WithError(err).Error("Error")
		}

		buffer := bytes.NewReader(frame.Payload)
		method, err := amqp.ReadMethod(buffer, conn.server.protoVersion)
		if err != nil {
			conn.logger.WithError(err).Error("Error")
		}

		// @todo handle method?
		fmt.Println(method)
	}
}
