package server

import (
	"bufio"
	"bytes"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/qos"
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

// From https://github.com/rabbitmq/rabbitmq-common/blob/master/src/rabbit_writer.erl
// When the amount of protocol method data buffered exceeds
// this threshold, a socket flush is performed.
//
// This magic number is the tcp-over-ethernet MSS (1460) minus the
// minimum size of a AMQP 0-9-1 basic.deliver method frame (24) plus basic
// content header (22). The idea is that we want to flush just before
// exceeding the MSS.
const flushThreshold = 1414

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
	virtualHost      *VirtualHost
	vhostName        string
	closeCh          chan bool
}

func NewConnection(server *Server, netConn *net.TCPConn) (connection *Connection) {
	connection = &Connection{
		id:           atomic.AddUint64(&server.connSeq, 1),
		server:       server,
		netConn:      netConn,
		channels:     make(map[uint16]*Channel),
		outgoing:     make(chan *amqp.Frame, 100),
		maxChannels:  server.config.Connection.ChannelsMax,
		maxFrameSize: server.config.Connection.FrameMaxSize,
		qos:          qos.New(0, 0),
		closeCh:      make(chan bool, 1),
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
	for chId := range conn.channels {
		channelIds = append(channelIds, int(chId))
	}
	sort.Sort(sort.Reverse(sort.IntSlice(channelIds)))
	for _, chId := range channelIds {
		channel := conn.channels[uint16(chId)]
		channel.close()
		delete(conn.channels, uint16(chId))
	}
	conn.clearQueues()
	conn.netConn.Close()
	conn.logger.WithFields(log.Fields{
		"vhost": conn.vhostName,
		"from":  conn.netConn.RemoteAddr(),
	}).Info("Connection closed")
	conn.server.removeConnection(conn.id)

	conn.closeCh <- true
}

func (conn *Connection) getChannel(id uint16) *Channel {
	return conn.channels[id]
}

func (conn *Connection) safeClose(wg *sync.WaitGroup) {
	defer wg.Done()

	ch := conn.getChannel(0)
	if ch == nil {
		return
	}
	conn.channels[0].SendMethod(&amqp.ConnectionClose{
		ReplyCode: amqp.ConnectionForced,
		ReplyText: "Server shutdown",
		ClassId:   0,
		MethodId:  0,
	})

	// let clients proper handle connection closing in 10 sec
	timeOut := time.After(10 * time.Second)

	select {
	case <-timeOut:
		conn.close()
		return
	case <-conn.closeCh:
		return
	}
}

func (conn *Connection) clearQueues() {
	virtualHost := conn.getVirtualHost()
	if virtualHost == nil {
		// it is possible when conn close before open, for example login failure
		return
	}
	for _, queue := range virtualHost.GetQueues() {
		if queue.IsExclusive() && queue.ConnID() == conn.id {
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
	if !bytes.Equal(buf, supported) {
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

		if frame.Sync {
			buffer.Flush()
		} else {
			conn.mayBeFlushBuffer(buffer)
		}
	}
}

func (conn *Connection) mayBeFlushBuffer(buffer *bufio.Writer) {
	if buffer.Buffered() >= flushThreshold {
		buffer.Flush()
	}

	if len(conn.outgoing) == 0 {
		// outgoing channel is buffered and we can check is here more messages for store into buffer
		// if nothing to store into buffer - we flush
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

		if frame.ChannelID != 0 && conn.getStatus() < ConnOpen {
			conn.logger.WithError(err).Error("Frame not allowed for unopened connection")
			conn.close()
			return
		}

		channel, ok := conn.channels[frame.ChannelID]
		if !ok {
			channel = NewChannel(frame.ChannelID, conn)
			conn.channels[frame.ChannelID] = channel
			conn.channels[frame.ChannelID].start()
		}

		channel.incoming <- frame
	}
}

func (conn *Connection) getVirtualHost() *VirtualHost {
	return conn.virtualHost
}
