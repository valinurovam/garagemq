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
	id             uint16
	conn           *Connection
	server         *Server
	incoming       chan *amqp.Frame
	outgoing       chan *amqp.Frame
	logger         *log.Entry
	status         int
	protoVersion   string
	currentMessage *amqp.Message
}

func NewChannel(id uint16, conn *Connection) (*Channel) {
	channel := &Channel{
		id:           id,
		conn:         conn,
		server:       conn.server,
		incoming:     make(chan *amqp.Frame, 100),
		outgoing:     conn.outgoing,
		status:       ChannelNew,
		protoVersion: conn.server.protoVersion,
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
		channel.logger.Debug("Incoming frame <- ", frame.Type)

		switch frame.Type {
		case amqp.FrameMethod:
			buffer := bytes.NewReader(frame.Payload)
			method, err := amqp.ReadMethod(buffer, channel.protoVersion)
			channel.logger.Debug("Incoming method <- " + method.Name())
			if err != nil {
				channel.logger.WithError(err).Error("Error on handling frame")
				channel.sendError(amqp.NewConnectionError(amqp.FrameError, err.Error(), 0, 0))
			}

			if err := channel.handleMethod(method); err != nil {
				channel.sendError(err);
			}
		case amqp.FrameHeader:
			if err := channel.handleContentHeader(frame); err != nil {
				channel.sendError(err);
			}
		case amqp.FrameBody:
			if err := channel.handleContentBody(frame); err != nil {
				channel.sendError(err);
			}
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
	case amqp.ClassExchange:
		return channel.exchangeRoute(method)
	case amqp.ClassQueue:
		return channel.queueRoute(method)
	}

	return nil
}

func (channel *Channel) handleContentHeader(headerFrame *amqp.Frame) *amqp.Error {
	reader := bytes.NewReader(headerFrame.Payload)
	var err error
	if channel.currentMessage == nil {
		return amqp.NewConnectionError(amqp.FrameError, "unexpected content header frame", 0, 0)
	}

	if channel.currentMessage.Header != nil {
		return amqp.NewConnectionError(amqp.FrameError, "unexpected content header frame - header already exists", 0, 0)
	}

	if channel.currentMessage.Header, err = amqp.ReadContentHeader(reader, channel.protoVersion); err != nil {
		return amqp.NewConnectionError(amqp.FrameError, "error on parsing content header frame", 0, 0)
	}

	//channel.logger.Debug("Incoming header <- ", channel.currentMessage.Header)
	return nil
}

func (channel *Channel) handleContentBody(bodyFrame *amqp.Frame) *amqp.Error {
	channel.currentMessage.Append(bodyFrame)

	if channel.currentMessage.BodySize < channel.currentMessage.Header.BodySize {
		return nil
	}

	vhost := channel.conn.getVhost()
	message := channel.currentMessage

	exchange := vhost.GetExchange(message.Exchange)
	matchedQueues := exchange.GetMatchedQueues(message)
	if len(matchedQueues) == 0 && message.Mandatory {
		// handle error, not only method. method + header + content
		// message go back to client
		channel.sendMethod(&amqp.BasicReturn{amqp.NoConsumers, "No route", message.Exchange, message.RoutingKey})
		return nil
	}

	for _, queueName := range matchedQueues {
		queue, _ := channel.conn.getVhost().GetQueue(queueName)
		queue.Messages = append(queue.Messages, channel.currentMessage)
		if len(queue.Messages) % 1000 == 0 {
			channel.logger.WithFields(log.Fields{
				"queueName": queue.Name,
				"length": len(queue.Messages),
			}).Debug("Current queue length")
		}

	}

	//channel.logger.Debug("Incoming body <- ", bodyFrame)
	return nil
}

func (channel *Channel) sendMethod(method amqp.Method) {
	rawMethod := bytes.NewBuffer([]byte{})
	if err := amqp.WriteMethod(rawMethod, method, channel.server.protoVersion); err != nil {
		logrus.WithError(err).Error("Error")
	}

	//channel.logger.Debug("Outgoing -> " + method.Name())

	closeAfter := method.ClassIdentifier() == amqp.ClassConnection && method.MethodIdentifier() == amqp.MethodConnectionCloseOk
	channel.outgoing <- &amqp.Frame{Type: byte(amqp.FrameMethod), ChannelId: channel.id, Payload: rawMethod.Bytes(), CloseAfter: closeAfter}
}
