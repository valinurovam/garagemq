package amqp

type Frame struct {
	Type      byte
	ChannelId uint16
	Payload   []byte
}
