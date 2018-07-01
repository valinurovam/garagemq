package amqp

import (
	"sync/atomic"
	"bytes"
)

type Table map[string]interface{}

type Decimal struct {
	Scale uint8
	Value int32
}

type Frame struct {
	Type       byte
	ChannelId  uint16
	Payload    []byte
	CloseAfter bool
}

type ContentHeader struct {
	ClassId       uint16
	Weight        uint16
	BodySize      uint64
	propertyFlags uint16
	PropertyList  *BasicPropertyList
}

type Message struct {
	Id         uint64
	Header     *ContentHeader
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
	BodySize   uint64
	Body       []*Frame
}

var msgId uint64

func NewMessage(method *BasicPublish) *Message {
	return &Message{
		Id:         atomic.AddUint64(&msgId, 1),
		Exchange:   method.Exchange,
		RoutingKey: method.RoutingKey,
		Mandatory:  method.Mandatory,
		Immediate:  method.Immediate,
		BodySize:   0,
	}
}

func (message *Message) Append(body *Frame) {
	message.Body = append(message.Body, body)
	message.BodySize += uint64(len(body.Payload))
}

func (message *Message) Marshal() (data []byte, err error) {
	buffer := bytes.NewBuffer([]byte{})
	if err = WriteLonglong(buffer, message.Id); err != nil {
		return nil, err
	}
	// TODO Remove ProtoRabbit
	if err = WriteContentHeader(buffer, message.Header, ProtoRabbit); err != nil {
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
	return buffer.Bytes(), nil
}

func (message *Message) Unmarshal(buffer []byte) (err error) {
	reader := bytes.NewReader(buffer)
	if message.Id, err = ReadLonglong(reader); err != nil {
		return err
	}
	// TODO Remove ProtoRabbit
	if message.Header, err = ReadContentHeader(reader, ProtoRabbit); err != nil {
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
	return nil
}

const (
	ErrorOnConnection = iota
	ErrorOnChannel
)

type Error struct {
	ReplyCode uint16
	ReplyText string
	ClassId   uint16
	MethodId  uint16
	ErrorType int
}

func NewConnectionError(code uint16, text string, classId uint16, methodId uint16) *Error {
	err := &Error{
		ReplyCode: code,
		ReplyText: ConstantsNameMap[code] + " - " + text,
		ClassId:   classId,
		MethodId:  methodId,
		ErrorType: ErrorOnConnection,
	}

	return err
}

func NewChannelError(code uint16, text string, classId uint16, methodId uint16) *Error {
	err := &Error{
		ReplyCode: code,
		ReplyText: ConstantsNameMap[code] + " - " + text,
		ClassId:   classId,
		MethodId:  methodId,
		ErrorType: ErrorOnChannel,
	}

	return err
}
