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
	ConnStart    = iota
	ConnStartOK
	ConnSecure
	ConnSecureOK
	ConnTune
	ConnTuneOK
	ConnOpen
	ConnOpenOK
	ConnClosing
	ConnClosed
)

type Connection struct {
	id               int64
	server           *Server
	netConn          net.Conn
	logger           *log.Entry
	channels         map[uint16]*Channel
	outgoing         chan *amqp.Frame
	clientProperties *amqp.Table
	maxChannels      uint16
	maxFrameSize     uint32
	status           int
}

func NewConnection(server *Server, netConn net.Conn) (connection *Connection) {
	connection = &Connection{
		id:           atomic.AddInt64(&server.connSeq, 1),
		server:       server,
		netConn:      netConn,
		channels:     make(map[uint16]*Channel),
		outgoing:     make(chan *amqp.Frame, 100),
		maxChannels:  4096,
		maxFrameSize: 65536,
	}

	connection.logger = log.WithFields(log.Fields{
		"conn_id": connection.id,
	})

	return
}

func (conn *Connection) close() {
	conn.status = ConnClosed
	conn.netConn.Close()
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
		conn.close()
		return
	}

	conn.channels[0] = NewChannel(0, conn)
	conn.channels[0].start()
	go conn.handleOutgoing()
	go conn.handleIncoming()
}

func (conn *Connection) handleOutgoing() {
	for {
		if conn.status == ConnClosed {
			break
		}

		var frame = <-conn.outgoing
		if err := amqp.WriteFrame(conn.netConn, frame); err != nil {
			conn.logger.WithError(err).Error(err.Error())
		}
	}
}

func (conn *Connection) handleIncoming() {
	for {
		if conn.status == ConnClosed {
			break
		}

		frame, err := amqp.ReadFrame(conn.netConn)
		if err != nil {
			conn.logger.WithError(err).Error("Error on reading frame. May be connection already closed.")
			conn.close()
			break
		}

		if conn.status < ConnOpen && frame.ChannelId != 0 {
			conn.logger.WithError(err).Error("Frame not allowed for unopened connection")
			conn.close()
			return
		}

		channel, ok := conn.channels[frame.ChannelId]
		if (!ok) {
			channel = NewChannel(frame.ChannelId, conn)
			conn.channels[frame.ChannelId] = channel
			conn.channels[frame.ChannelId].start()
		}

		channel.incoming <- frame
	}
}
