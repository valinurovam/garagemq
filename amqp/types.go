package amqp

import (
	"bytes"
	"sync/atomic"
	"time"
)

// Table - simple amqp-table implementation
type Table map[string]interface{}

// Decimal represents amqp-decimal data
type Decimal struct {
	Scale uint8
	Value int32
}

// Frame is raw frame
type Frame struct {
	Type       byte
	ChannelID  uint16
	Payload    []byte
	CloseAfter bool
	Sync       bool
}

// ContentHeader represents amqp-message content-header
type ContentHeader struct {
	ClassID       uint16
	Weight        uint16
	BodySize      uint64
	propertyFlags uint16
	PropertyList  *BasicPropertyList
}

// ConfirmMeta store information for check confirms and send confirm-acks
type ConfirmMeta struct {
	ChanID           uint16
	ConnID           uint64
	DeliveryTag      uint64
	ExpectedConfirms int
	ActualConfirms   int
}

// CanConfirm returns is message can be confirmed
func (meta *ConfirmMeta) CanConfirm() bool {
	return meta.ActualConfirms == meta.ExpectedConfirms
}

// Message represents amqp-message and meta-data
type Message struct {
	ID            uint64
	Header        *ContentHeader
	Exchange      string
	RoutingKey    string
	Mandatory     bool
	Immediate     bool
	BodySize      uint64
	Body          []*Frame
	DeliveryCount uint32
	ConfirmMeta   ConfirmMeta
}

// when server restart we can't start again count messages from 0
var msgID = uint64(time.Now().Unix())

// NewMessage returns new message instance
func NewMessage(method *BasicPublish) *Message {
	return &Message{
		ID:            atomic.AddUint64(&msgID, 1),
		Exchange:      method.Exchange,
		RoutingKey:    method.RoutingKey,
		Mandatory:     method.Mandatory,
		Immediate:     method.Immediate,
		BodySize:      0,
		DeliveryCount: 0,
	}
}

// IsPersistent check if message should be persisted
func (message *Message) IsPersistent() bool {
	deliveryMode := message.Header.PropertyList.DeliveryMode
	return deliveryMode != nil && *deliveryMode == 2
}

// Append appends new body-frame into message and increase bodySize
func (message *Message) Append(body *Frame) {
	message.Body = append(message.Body, body)
	message.BodySize += uint64(len(body.Payload))
}

// Marshal converts message into bytes to store into db
func (message *Message) Marshal(protoVersion string) (data []byte, err error) {
	buffer := bytes.NewBuffer([]byte{})
	if err = WriteLonglong(buffer, message.ID); err != nil {
		return nil, err
	}

	if err = WriteContentHeader(buffer, message.Header, protoVersion); err != nil {
		return nil, err
	}
	if err = WriteShortstr(buffer, message.Exchange); err != nil {
		return nil, err
	}
	if err = WriteShortstr(buffer, message.RoutingKey); err != nil {
		return nil, err
	}
	if err = WriteLonglong(buffer, message.BodySize); err != nil {
		return nil, err
	}

	body := bytes.NewBuffer([]byte{})
	for _, frame := range message.Body {
		if err = WriteFrame(body, frame); err != nil {
			return nil, err
		}
	}
	WriteLongstr(buffer, body.Bytes())

	if err = WriteLong(buffer, message.DeliveryCount); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// Unmarshal restore message entity from bytes
func (message *Message) Unmarshal(buffer []byte, protoVersion string) (err error) {
	reader := bytes.NewReader(buffer)
	if message.ID, err = ReadLonglong(reader); err != nil {
		return err
	}

	if message.Header, err = ReadContentHeader(reader, protoVersion); err != nil {
		return err
	}
	if message.Exchange, err = ReadShortstr(reader); err != nil {
		return err
	}
	if message.RoutingKey, err = ReadShortstr(reader); err != nil {
		return err
	}
	if message.BodySize, err = ReadLonglong(reader); err != nil {
		return err
	}

	rawBody, err := ReadLongstr(reader)
	bodyBuffer := bytes.NewReader(rawBody)

	for bodyBuffer.Len() != 0 {
		body, _ := ReadFrame(bodyBuffer)
		message.Body = append(message.Body, body)
	}

	if message.DeliveryCount, err = ReadLong(reader); err != nil {
		return err
	}
	return nil
}

// Constants to detect connection or channel error thrown
const (
	ErrorOnConnection = iota
	ErrorOnChannel
)

// Error represents AMQP-error data
type Error struct {
	ReplyCode uint16
	ReplyText string
	ClassID   uint16
	MethodID  uint16
	ErrorType int
}

// NewConnectionError returns new connection error. If caused - connection should be closed
func NewConnectionError(code uint16, text string, classID uint16, methodID uint16) *Error {
	err := &Error{
		ReplyCode: code,
		ReplyText: ConstantsNameMap[code] + " - " + text,
		ClassID:   classID,
		MethodID:  methodID,
		ErrorType: ErrorOnConnection,
	}

	return err
}

// NewChannelError returns new channel error& If caused - channel should be closed
func NewChannelError(code uint16, text string, classID uint16, methodID uint16) *Error {
	err := &Error{
		ReplyCode: code,
		ReplyText: ConstantsNameMap[code] + " - " + text,
		ClassID:   classID,
		MethodID:  methodID,
		ErrorType: ErrorOnChannel,
	}

	return err
}
