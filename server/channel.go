package server

import (
	"github.com/valinurovam/garagemq/amqp"
	"bytes"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
)

const (
	ChannelNew     = iota
	ChannelOpen
	ChannelClosing
	ChannelClosed
)

type Channel struct {
	id       uint16
	conn     *Connection
	server   *Server
	incoming chan *amqp.Frame
	outgoing chan *amqp.Frame
	logger   *log.Entry
	status   int
}

func NewChannel(id uint16, conn *Connection) (*Channel) {
	channel := &Channel{
		id:       id,
		conn:     conn,
		server:   conn.server,
		incoming: make(chan *amqp.Frame, 100),
		outgoing: conn.outgoing,
		status:   ChannelNew,
	}

	channel.logger = log.WithFields(log.Fields{
		"conn_id":    conn.id,
		"channel_id": id,
	})

	return channel
}

func (channel *Channel) start() {
	if channel.id == 0 {
		go channel.connectionStart()
	}

	go channel.handleIncoming()
}

func (channel *Channel) handleIncoming() {
	for {
		frame := <-channel.incoming

		switch frame.Type {
		case amqp.FrameMethod:
			buffer := bytes.NewReader(frame.Payload)
			method, err := amqp.ReadMethod(buffer, channel.conn.server.protoVersion)
			channel.logger.Info("Incoming <- " + method.Name())
			if err != nil {
				channel.logger.WithError(err).Error("Error on handling frame")
				channel.sendError(amqp.NewConnectionError(amqp.FrameError, err.Error(), 0, 0))
			}

			if err := channel.handleMethod(method); err != nil {
				channel.sendError(err);
			}
		case amqp.FrameHeader:

		case amqp.FrameBody:
		}
	}
}

func (channel *Channel) sendError(err *amqp.Error) {
	channel.logger.Error(err)
	switch err.ErrorType {
	case amqp.ErrorOnChannel:
		channel.status = ChannelClosing
		channel.sendMethod(&amqp.ChannelClose{
			ReplyCode: err.ReplyCode,
			ReplyText: err.ReplyText,
			ClassId:   err.ClassId,
			MethodId:  err.MethodId,
		})
	case amqp.ErrorOnConnection:
		channel.conn.status = ConnClosing
		channel.conn.channels[0].sendMethod(&amqp.ConnectionClose{
			ReplyCode: err.ReplyCode,
			ReplyText: err.ReplyText,
			ClassId:   err.ClassId,
			MethodId:  err.MethodId,
		})
	}
}

func (channel *Channel) handleMethod(method amqp.Method) *amqp.Error {
	switch method.ClassIdentifier() {
	case amqp.ClassConnection:
		return channel.connectionRoute(method)
	case amqp.ClassChannel:
		return channel.channelRoute(method)
	case amqp.ClassBasic:
		return channel.basicRoute(method)
	}

	return nil
}

func (channel *Channel) sendMethod(method amqp.Method) {
	rawMethod := bytes.NewBuffer([]byte{})
	if err := amqp.WriteMethod(rawMethod, method, channel.server.protoVersion); err != nil {
		logrus.WithError(err).Error("Error")
	}
	channel.logger.Info("Outgoing -> " + method.Name())
	channel.outgoing <- &amqp.Frame{Type: byte(amqp.FrameMethod), ChannelId: channel.id, Payload: rawMethod.Bytes()}
}