package server

import (
	"github.com/valinurovam/garagemq/amqp"
	"runtime"
	"os"
	"github.com/valinurovam/garagemq/auth"
)

func (channel *Channel) connectionRoute(method amqp.Method) *amqp.Error {
	switch method := method.(type) {
	case *amqp.ConnectionStartOk:
		return channel.connectionStartOk(method)
	case *amqp.ConnectionTuneOk:
		return channel.connectionTuneOk(method)
	case *amqp.ConnectionOpen:
		return channel.connectionOpen(method)
	case *amqp.ConnectionClose:
		return channel.connectionClose(method)
	case *amqp.ConnectionSecureOk:
		return channel.connectionSecureOk(method)
	case *amqp.ConnectionCloseOk:
		return channel.connectionCloseOk(method)
	case *amqp.ConnectionBlocked:
		return channel.connectionBlocked(method)
	case *amqp.ConnectionUnblocked:
		return channel.connectionUnblocked(method)
	}

	return amqp.NewConnectionError(amqp.NotImplemented, "unable to route connection method", method.ClassIdentifier(), method.MethodIdentifier())
}

func (channel *Channel) connectionStart() {
	var capabilities = amqp.Table{}
	capabilities["publisher_confirms"] = false
	capabilities["basic.nack"] = true
	var serverProps = amqp.Table{}
	serverProps["product"] = "garagemq"
	serverProps["version"] = "0.1"
	serverProps["copyright"] = "Alexander Valinurov, 2018"
	serverProps["platform"] = runtime.GOARCH
	serverProps["capabilities"] = capabilities
	host, err := os.Hostname()
	if err != nil {
		serverProps["host"] = "UnknownHostError"
	} else {
		serverProps["host"] = host
	}

	var method = amqp.ConnectionStart{0, 9, &serverProps, []byte("PLAIN"), []byte("en_US")}
	channel.sendMethod(&method)

	channel.conn.status = ConnStart
}

func (channel *Channel) connectionStartOk(method *amqp.ConnectionStartOk) *amqp.Error {
	channel.conn.status = ConnStartOK

	var saslData auth.SaslData
	var err error
	if saslData, err = auth.ParsePlain(method.Response); err != nil {
		return amqp.NewConnectionError(amqp.NotAllowed, "login failure", method.ClassIdentifier(), method.MethodIdentifier())
	}

	if method.Mechanism != auth.SaslPlain {
		channel.conn.close()
	}

	if !channel.server.checkAuth(saslData) {
		return amqp.NewConnectionError(amqp.NotAllowed, "login failure", method.ClassIdentifier(), method.MethodIdentifier())
	}

	channel.conn.clientProperties = method.ClientProperties

	// @todo Send HeartBeat 0 cause not supported yet
	channel.sendMethod(&amqp.ConnectionTune{channel.conn.maxChannels, channel.conn.maxFrameSize, 0})
	channel.conn.status = ConnTune

	return nil
}

func (channel *Channel) connectionTuneOk(method *amqp.ConnectionTuneOk) *amqp.Error {
	channel.conn.status = ConnTuneOK

	if method.ChannelMax > channel.conn.maxChannels || method.FrameMax > channel.conn.maxFrameSize {
		channel.conn.close()
		return nil
	}

	channel.conn.maxChannels = method.ChannelMax
	channel.conn.maxFrameSize = method.FrameMax

	if method.Heartbeat > 0 {
		channel.conn.close()
	}

	return nil
}

func (channel *Channel) connectionOpen(method *amqp.ConnectionOpen) *amqp.Error {
	channel.conn.status = ConnOpen
	var vhostFound bool
	if channel.conn.virtualHost, vhostFound = channel.server.vhosts[method.VirtualHost]; !vhostFound {
		return amqp.NewConnectionError(amqp.InvalidPath, "virtualHost '"+method.VirtualHost+"' does not exist", method.ClassIdentifier(), method.MethodIdentifier())
	}

	channel.sendMethod(&amqp.ConnectionOpenOk{})
	channel.conn.status = ConnOpenOK

	channel.logger.Info("AMQP connection open")
	return nil
}

func (channel *Channel) connectionClose(method *amqp.ConnectionClose) *amqp.Error {
	channel.sendMethod(&amqp.ConnectionCloseOk{})
	return nil
}

func (channel *Channel) connectionCloseOk(method *amqp.ConnectionCloseOk) *amqp.Error {
	channel.conn.close()
	return nil
}

func (channel *Channel) connectionSecureOk(method *amqp.ConnectionSecureOk) *amqp.Error {
	return amqp.NewConnectionError(amqp.NotImplemented, "method ConnectionSecureOk does not implemented yet", method.ClassIdentifier(), method.MethodIdentifier())
}

func (channel *Channel) connectionBlocked(method *amqp.ConnectionBlocked) *amqp.Error {
	return amqp.NewConnectionError(amqp.NotImplemented, "method ConnectionBlocked does not implemented yet", method.ClassIdentifier(), method.MethodIdentifier())
}

func (channel *Channel) connectionUnblocked(method *amqp.ConnectionUnblocked) *amqp.Error {
	return amqp.NewConnectionError(amqp.NotImplemented, "method ConnectionUnblocked does not implemented yet", method.ClassIdentifier(), method.MethodIdentifier())
}
