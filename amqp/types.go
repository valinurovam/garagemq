package amqp

type Table map[string]interface{}

type Decimal struct {
	Scale uint8
	Value int32
}