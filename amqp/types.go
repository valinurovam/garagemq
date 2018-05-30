package amqp

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
	PropertyFlags uint16
	PropertyList  *BasicPropertyList
}

type Message struct {
	Header     *ContentHeader
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
	BodySize   uint64
	Body       []*Frame
}

func NewMessage(method *BasicPublish) *Message {
	return &Message{
		Exchange:   method.Exchange,
		RoutingKey: method.RoutingKey,
		Mandatory:  method.Mandatory,
		Immediate:  method.Immediate,
		BodySize:   0,
	}
}

func (msg *Message) Append(body *Frame) {
	msg.Body = append(msg.Body, body)
	msg.BodySize += uint64(len(body.Payload))
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
