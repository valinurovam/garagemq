package queue

import (
	"github.com/patrickwalker/garagemq/qos"
)

// ConsumerMock implements AMQP consumer mock
type ConsumerMock struct {
	tag    string
	cancel bool
}

// Consume send signal into consumer channel, than consumer can try to pop message from queue
func (consumer *ConsumerMock) Consume() bool {
	return true
}

// Stop stops consumer and remove it from queue consumers list
func (consumer *ConsumerMock) Stop() {

}

// Cancel stops consumer and send basic.cancel method to the client
func (consumer *ConsumerMock) Cancel() {
	consumer.cancel = true
}

// Tag returns consumer tag
func (consumer *ConsumerMock) Tag() string {
	return consumer.tag
}

// Qos returns consumer qos rules
func (consumer *ConsumerMock) Qos() []*qos.AmqpQos {
	return []*qos.AmqpQos{}
}
