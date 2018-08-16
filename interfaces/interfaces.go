package interfaces

import (
	"github.com/valinurovam/garagemq/amqp"
)

// Channel represents base channel public interface
type Channel interface {
	SendContent(method amqp.Method, message *amqp.Message)
	SendMethod(method amqp.Method)
	NextDeliveryTag() uint64
	AddUnackedMessage(dTag uint64, cTag string, queue string, message *amqp.Message)
}

// Consumer represents base consumer public interface
type Consumer interface {
	Consume()
	Tag() string
	Cancel()
}

// OpSet identifier for set data into storeage
const OpSet = 1
// OpDel identifier for delete data from storage
const OpDel = 2

// Operation represents structure to set/del from storage
type Operation struct {
	Key   string
	Value []byte
	Op    byte
}

// DbStorage represent base db storage interface
type DbStorage interface {
	Set(key string, value []byte) (err error)
	Del(key string) (err error)
	Get(key string) (value []byte, err error)
	Iterate(fn func(key []byte, value []byte))
	ProcessBatch(batch []*Operation) (err error)
	Close() error
}
