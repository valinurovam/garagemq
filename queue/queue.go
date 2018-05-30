package queue

import "github.com/valinurovam/garagemq/amqp"

type Queue struct {
	Messages []*amqp.Message
	Name     string
}
