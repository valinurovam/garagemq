package server

import (
	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/exchange"
	"fmt"
	"strings"
)

func (channel *Channel) exchangeRoute(method amqp.Method) *amqp.Error {
	switch method := method.(type) {
	case *amqp.ExchangeDeclare:
		return channel.exchangeDeclare(method)
	}

	return amqp.NewConnectionError(amqp.NotImplemented, "unable to route queue method "+method.Name(), method.ClassIdentifier(), method.MethodIdentifier())
}

func (channel *Channel) exchangeDeclare(method *amqp.ExchangeDeclare) *amqp.Error {
	exTypeId, err := exchange.GetExchangeTypeId(method.Type)
	if err != nil {
		return amqp.NewChannelError(amqp.NotImplemented, err.Error(), method.ClassIdentifier(), method.MethodIdentifier())
	}

	existingExchange := channel.conn.getVhost().GetExchange(method.Exchange)
	if method.Passive {
		if method.NoWait {
			return nil
		}

		if existingExchange == nil {
			return amqp.NewChannelError(
				amqp.NotFound,
				fmt.Sprintf("exchange '%s' not found", method.Exchange),
				method.ClassIdentifier(),
				method.MethodIdentifier(),
			)
		} else {
			channel.sendMethod(&amqp.ExchangeDeclareOk{})
		}

		return nil
	}

	// @todo check exchanges are equal

	if method.Exchange == "" {
		return amqp.NewChannelError(
			amqp.CommandInvalid,
			"exchange name is requred",
			method.ClassIdentifier(),
			method.MethodIdentifier(),
		)
	}

	if strings.HasPrefix(method.Exchange, "amq.") {
		return amqp.NewChannelError(
			amqp.AccessRefused,
			fmt.Sprintf("exchange name '%s' contains reserved prefix 'amq.*'", method.Exchange),
			method.ClassIdentifier(),
			method.MethodIdentifier(),
		)
	}

	if existingExchange != nil {
		channel.sendMethod(&amqp.ExchangeDeclareOk{})
		return nil
	}

	ex := exchange.New(
		method.Exchange,
		exTypeId,
		method.Durable,
		method.AutoDelete,
		method.Internal,
		false,
		method.Arguments,
	)

	channel.conn.getVhost().AppendExchange(ex)
	channel.sendMethod(&amqp.ExchangeDeclareOk{})

	return nil
}
