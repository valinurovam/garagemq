package server

import (
	"net"
	"fmt"
	"bytes"
	log "github.com/sirupsen/logrus"
	"sync/atomic"
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/vhost"
	"github.com/valinurovam/garagemq/qos"
	"bufio"
	"sync"
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
	ConnCloseOK
	ConnClosing
	ConnClosed
)

type Connection struct {
	id               int64
	server           *Server
	netConn          *net.TCPConn
	logger           *log.Entry
	channels         map[uint16]*Channel
	outgoing         chan *amqp.Frame
	clientProperties *amqp.Table
	maxChannels      uint16
	maxFrameSize     uint32
	statusLock       sync.RWMutex
	status           int
	qos              *qos.Qos
	virtualHost      *vhost.VirtualHost
}

func NewConnection(server *Server, netConn *net.TCPConn) (connection *Connection) {
	connection = &Connection{
		id:           atomic.AddInt64(&server.connSeq, 1),
		server:       server,
		netConn:      netConn,
		channels:     make(map[uint16]*Channel),
		outgoing:     make(chan *amqp.Frame, 100),
		maxChannels:  4096,
		maxFrameSize: 65536,
		qos:          qos.New(0, 0),
	}

	connection.logger = log.WithFields(log.Fields{
		"connectionId": connection.id,
	})

	return
}

func (conn *Connection) close() {
	if conn.getStatus() == ConnClosed {
		return
	}

	conn.setStatus(ConnClosed)
	for _, channel := range conn.channels {
		channel.close()
	}
	conn.netConn.Close()
	conn.server.removeConnection(conn.id)
	conn.logger.Info("AMQP connection closed")
}

func (conn *Connection) setStatus(status int) {
	conn.statusLock.Lock()
	defer conn.statusLock.Unlock()
	conn.status = status
}

func (conn *Connection) getStatus() int {
	conn.statusLock.RLock()
	defer conn.statusLock.RUnlock()
	return conn.status
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
	buffer := bufio.NewWriter(conn.netConn)
	for {
		if conn.getStatus() >= ConnClosing {
			break
		}

		var frame = <-conn.outgoing
		if err := amqp.WriteFrame(buffer, frame); err != nil {
			conn.logger.WithError(err).Warn("writing frame")
			conn.setStatus(ConnClosing)
			break
		}

		if frame.CloseAfter {
			conn.setStatus(ConnClosing)
			break
		}

		buffer.Flush()
	}

	buffer.Flush()
	conn.close()
}

func (conn *Connection) handleIncoming() {
	buffer := bufio.NewReader(conn.netConn)

	for {
		if conn.getStatus() >= ConnClosing {
			break
		}

		frame, err := amqp.ReadFrame(buffer)
		if err != nil {
			if err.Error() != "EOF" && conn.getStatus() < ConnClosing {
				conn.logger.WithError(err).Warn("reading frame")
			}
			conn.close()
			break
		}

		if frame.ChannelId != 0 && conn.getStatus() < ConnOpen {
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

func (conn *Connection) getVirtualHost() *vhost.VirtualHost {
	return conn.virtualHost
}
