package amqp

type Table map[string]interface{}

type Decimal struct {
	Scale uint8
	Value int32
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
		ReplyText: text,
		ClassId:   classId,
		MethodId:  methodId,
		ErrorType: ErrorOnConnection,
	}

	return err
}

func NewChannelError(code uint16, text string, classId uint16, methodId uint16) *Error {
	err := &Error{
		ReplyCode: code,
		ReplyText: text,
		ClassId:   classId,
		MethodId:  methodId,
		ErrorType: ErrorOnChannel,
	}

	return err
}
