package queue

import (
	"github.com/valinurovam/garagemq/qos"
)

// ConsumerMock implements AMQP consumer mock
type ConsumerMock struct {
	tag string
}

// Consume send signal into consumer channel, than consumer can try to pop message from queue
func (consumer *ConsumerMock) Consume() {

}

// Stop stops consumer and remove it from queue consumers list
func (consumer *ConsumerMock) Stop() {

}

// Cancel stops consumer and send basic.cancel method to the client
func (consumer *ConsumerMock) Cancel() {

}

// Tag returns consumer tag
func (consumer *ConsumerMock) Tag() string {
	return consumer.tag
}

// Qos returns consumer qos rules
func (consumer *ConsumerMock) Qos() []*qos.AmqpQos {
	return []*qos.AmqpQos{}
}
