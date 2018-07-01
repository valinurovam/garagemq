package server

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/consumer"
	"github.com/valinurovam/garagemq/exchange"
	"github.com/valinurovam/garagemq/interfaces"
	"github.com/valinurovam/garagemq/qos"
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
	cmrLock        sync.Mutex
	consumers      map[string]interfaces.Consumer
	qos            *qos.AmqpQos
	deliveryTag    uint64
	ackLock        sync.Mutex
	ackStore       map[uint64]*UnackedMessage
}

type UnackedMessage struct {
	cTag  string
	size  uint64
	queue string
	id    uint64
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
		consumers:    make(map[string]interfaces.Consumer),
		qos:          qos.New(0, 0),
		ackStore:     make(map[uint64]*UnackedMessage),
	}

	channel.logger = log.WithFields(log.Fields{
		"connectionId": conn.id,
		"channelId":    id,
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
		channel.SendMethod(&amqp.ChannelClose{
			ReplyCode: err.ReplyCode,
			ReplyText: err.ReplyText,
			ClassId:   err.ClassId,
			MethodId:  err.MethodId,
		})
	case amqp.ErrorOnConnection:
		channel.conn.status = ConnClosing
		channel.conn.channels[0].SendMethod(&amqp.ConnectionClose{
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

	vhost := channel.conn.getVirtualHost()
	message := channel.currentMessage
	exchange := vhost.GetExchange(message.Exchange)
	matchedQueues := exchange.GetMatchedQueues(message)

	if len(matchedQueues) == 0 && message.Mandatory {
		channel.SendContent(
			&amqp.BasicReturn{ReplyCode: amqp.NoConsumers, ReplyText: "No route", Exchange: message.Exchange, RoutingKey: message.RoutingKey},
			message,
		)
		return nil
	}

	for queueName, _ := range matchedQueues {
		queue := channel.conn.getVirtualHost().GetQueue(queueName)
		queue.Push(channel.currentMessage)
	}

	channel.logger.Debug("Incoming body <- ", bodyFrame)
	return nil
}

func (channel *Channel) SendMethod(method amqp.Method) {
	rawMethod := bytes.NewBuffer([]byte{})
	if err := amqp.WriteMethod(rawMethod, method, channel.server.protoVersion); err != nil {
		logrus.WithError(err).Error("Error")
	}

	closeAfter := method.ClassIdentifier() == amqp.ClassConnection && method.MethodIdentifier() == amqp.MethodConnectionCloseOk

	channel.logger.Debug("Outgoing -> " + method.Name())
	channel.outgoing <- &amqp.Frame{Type: byte(amqp.FrameMethod), ChannelId: channel.id, Payload: rawMethod.Bytes(), CloseAfter: closeAfter}
}

func (channel *Channel) SendContent(method amqp.Method, message *amqp.Message) {
	channel.SendMethod(method)

	rawHeader := bytes.NewBuffer([]byte{})
	amqp.WriteContentHeader(rawHeader, message.Header, channel.server.protoVersion)
	channel.outgoing <- &amqp.Frame{Type: byte(amqp.FrameHeader), ChannelId: channel.id, Payload: rawHeader.Bytes(), CloseAfter: false}

	for _, payload := range message.Body {
		payload.ChannelId = channel.id
		channel.outgoing <- payload
	}
}
func (channel *Channel) addConsumer(method *amqp.BasicConsume) (cmr interfaces.Consumer, err *amqp.Error) {
	channel.cmrLock.Lock()
	defer channel.cmrLock.Unlock()

	var qu interfaces.AmqpQueue
	if qu, err = channel.getQueueWithError(method.Queue, method); err != nil {
		return nil, err
	}

	cmr = consumer.New(method.Queue, method.ConsumerTag, method.NoAck, channel, qu, []*qos.AmqpQos{channel.qos, channel.conn.qos})
	if _, ok := channel.consumers[cmr.Tag()]; ok {
		return nil, amqp.NewChannelError(amqp.NotAllowed, fmt.Sprintf("Consumer with tag '%s' already exists", cmr.Tag()), method.ClassIdentifier(), method.MethodIdentifier())
	}

	if quErr := qu.AddConsumer(cmr, method.Exclusive); quErr != nil {
		return nil, amqp.NewChannelError(amqp.AccessRefused, quErr.Error(), method.ClassIdentifier(), method.MethodIdentifier())
	}
	channel.consumers[cmr.Tag()] = cmr

	return cmr, nil
}

func (channel *Channel) removeConsumer(cTag string) {
	channel.cmrLock.Lock()
	defer channel.cmrLock.Unlock()
	if cmr, ok := channel.consumers[cTag]; ok {
		cmr.Stop()
		delete(channel.consumers, cmr.Tag())
	}
}

func (channel *Channel) close() {
	channel.cmrLock.Lock()
	defer channel.cmrLock.Unlock()
	for _, cmr := range channel.consumers {
		cmr.Stop()
		delete(channel.consumers, cmr.Tag())
		channel.logger.WithFields(log.Fields{
			"consumerTag": cmr.Tag(),
		}).Info("Consumer stopped")
	}
}

func (channel *Channel) updateQos(prefetchCount uint16, prefetchSize uint32, global bool) {
	if global {
		channel.qos.Update(prefetchCount, prefetchSize)
	} else {
		channel.conn.qos.Update(prefetchCount, prefetchSize)
	}
}

func (channel *Channel) NextDeliveryTag() uint64 {
	return atomic.AddUint64(&channel.deliveryTag, 1)
}

func (channel *Channel) AddUnackedMessage(dTag uint64, cTag string, queue string, message *amqp.Message) {
	channel.ackLock.Lock()
	defer channel.ackLock.Unlock()
	channel.ackStore[dTag] = &UnackedMessage{
		cTag:  cTag,
		size:  message.BodySize,
		queue: queue,
		id:    message.Id,
	}
}

func (channel *Channel) handleAck(method *amqp.BasicAck) *amqp.Error {
	channel.ackLock.Lock()
	defer channel.ackLock.Unlock()
	var uMsg *UnackedMessage
	var msgFound bool

	if uMsg, msgFound = channel.ackStore[method.DeliveryTag]; !msgFound {
		return amqp.NewChannelError(406, fmt.Sprintf("Delivery tag [%d] not found", method.DeliveryTag), method.ClassIdentifier(), method.MethodIdentifier())
	}

	delete(channel.ackStore, method.DeliveryTag)
	channel.conn.getVirtualHost().GetQueue(uMsg.queue).AckMsg(uMsg.id)

	channel.qos.Dec(1, uint32(uMsg.size))
	channel.conn.qos.Dec(1, uint32(uMsg.size))

	if cmr, ok := channel.consumers[uMsg.cTag]; ok {
		cmr.Consume()
	}

	return nil
}

func (channel *Channel) getExchangeWithError(exchangeName string, method amqp.Method) (exchange *exchange.Exchange, err *amqp.Error) {
	ex := channel.conn.getVirtualHost().GetExchange(exchangeName)
	if ex == nil {
		return nil, amqp.NewChannelError(
			amqp.NotFound,
			fmt.Sprintf("exchange '%s' not found", exchangeName),
			method.ClassIdentifier(),
			method.MethodIdentifier(),
		)
	}
	return ex, nil
}

func (channel *Channel) getQueueWithError(queueName string, method amqp.Method) (queue interfaces.AmqpQueue, err *amqp.Error) {
	qu := channel.conn.getVirtualHost().GetQueue(queueName)
	if qu == nil || !qu.IsActive() {
		return nil, amqp.NewChannelError(
			amqp.NotFound,
			fmt.Sprintf("queue '%s' not found", queueName),
			method.ClassIdentifier(),
			method.MethodIdentifier(),
		)
	}
	return qu, nil
}

func (channel *Channel) checkQueueLockWithError(qu interfaces.AmqpQueue, method amqp.Method) *amqp.Error {
	if qu == nil {
		return nil
	}
	if qu.IsExclusive() && qu.ConnId() != channel.conn.id {
		return amqp.NewChannelError(
			amqp.ResourceLocked,
			fmt.Sprintf("queue '%s' is locked to another connection", qu.GetName()),
			method.ClassIdentifier(),
			method.MethodIdentifier(),
		)
	}

	return nil
}
