package server

import (
	"net"
	"bytes"
	log "github.com/sirupsen/logrus"
	"sync/atomic"
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/vhost"
	"github.com/valinurovam/garagemq/qos"
	"sync"
	"bufio"
	"sort"
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
	id               uint64
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
	qos              *qos.AmqpQos
	virtualHost      *vhost.VirtualHost
}

func NewConnection(server *Server, netConn *net.TCPConn) (connection *Connection) {
	connection = &Connection{
		id:           atomic.AddUint64(&server.connSeq, 1),
		server:       server,
		netConn:      netConn,
		channels:     make(map[uint16]*Channel),
		outgoing:     make(chan *amqp.Frame, 1),
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
	conn.statusLock.Lock()
	defer conn.statusLock.Unlock()
	if conn.status == ConnClosed {
		return
	}
	conn.status = ConnClosed

	// channel0 should we be closed at the end
	channelIds := make([]int, 0)
	for chId, _ := range conn.channels {
		channelIds = append(channelIds, int(chId))
	}
	sort.Sort(sort.Reverse(sort.IntSlice(channelIds)))
	for _, chId := range channelIds {
		channel := conn.channels[uint16(chId)]
		channel.close()
	}
	conn.clearQueues()
	conn.netConn.Close()
	conn.logger.WithFields(log.Fields{
		"vhost": conn.virtualHost.Name(),
		"from":  conn.netConn.RemoteAddr(),
	}).Info("Connection closed")
	conn.server.removeConnection(conn.id)
}

func (conn *Connection) clearQueues() {
	virtualHost := conn.getVirtualHost()
	for _, queue := range virtualHost.GetQueues() {
		if queue.IsExclusive() && queue.ConnId() == conn.id {
			virtualHost.DeleteQueue(queue.GetName(), false, false)
		}
	}
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
		conn.logger.WithError(err).WithFields(log.Fields{
			"readed buffer": buf,
		}).Error("Error on read protocol header")
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
			conn.close()
			return
		}

		if frame.CloseAfter {
			conn.setStatus(ConnClosing)
			buffer.Flush()
			conn.close()
			break
		}

		buffer.Flush()
	}
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
