package amqp

import (
	"reflect"
	"testing"
)

func TestNewMessage(t *testing.T) {
	method := &BasicPublish{
		Exchange:   "ex",
		RoutingKey: "rk",
		Mandatory:  false,
		Immediate:  true,
	}

	message := NewMessage(method)

	if message.Exchange != method.Exchange {
		t.Fatalf("Expected Exchange %s, actual %s", method.Exchange, message.Exchange)
	}

	if message.RoutingKey != method.RoutingKey {
		t.Fatalf("Expected RoutingKey %s, actual %s", method.RoutingKey, message.RoutingKey)
	}

	if message.Mandatory != method.Mandatory {
		t.Fatalf("Expected Mandatory %t, actual %t", method.Mandatory, message.Mandatory)
	}

	if message.Immediate != method.Immediate {
		t.Fatalf("Expected Immediate %t, actual %t", method.Immediate, message.Immediate)
	}
}

func TestMessage_Append(t *testing.T) {
	m := &Message{
		BodySize: 0,
		Body:     make([]*Frame, 0),
	}

	m.Append(&Frame{
		Type:       0,
		ChannelID:  0,
		Payload:    []byte{'t', 'e', 's', 't'},
		CloseAfter: false,
	})

	if m.BodySize != 4 {
		t.Fatalf("Expected BodySize %d, actual %d", 4, m.BodySize)
	}

	m.Append(&Frame{
		Type:       0,
		ChannelID:  0,
		Payload:    []byte{'t', 'e', 's', 't'},
		CloseAfter: false,
	})

	if m.BodySize != 8 {
		t.Fatalf("Expected BodySize %d, actual %d", 8, m.BodySize)
	}

	if len(m.Body) != 2 {
		t.Fatalf("Expected Body len %d, actual %d", 2, len(m.Body))
	}
}

func TestMessage_Marshal_Unmarshal(t *testing.T) {
	ctype := "text/plain"

	mM := &Message{
		ID: 1,
		Header: &ContentHeader{
			ClassID:       ClassBasic,
			Weight:        0,
			BodySize:      4,
			propertyFlags: 32768,
			PropertyList: &BasicPropertyList{
				ContentType:     &ctype,
				ContentEncoding: nil,
				Headers:         nil,
				DeliveryMode:    nil,
				Priority:        nil,
				CorrelationID:   nil,
				ReplyTo:         nil,
				Expiration:      nil,
				MessageID:       nil,
				Timestamp:       nil,
				Type:            nil,
				UserID:          nil,
				AppID:           nil,
				Reserved:        nil,
			},
		},
		Exchange:   "",
		RoutingKey: "test",
		Mandatory:  false,
		Immediate:  false,
		BodySize:   4,
		Body: []*Frame{
			{
				Type:       3,
				ChannelID:  1,
				Payload:    []byte{'t', 'e', 's', 't'},
				CloseAfter: false,
			},
		},
	}

	bytes, err := mM.Marshal(ProtoRabbit)
	if err != nil {
		t.Fatal(err)
	}

	mU := &Message{}
	mU.Unmarshal(bytes, ProtoRabbit)
	if !reflect.DeepEqual(mM, mU) {
		t.Fatalf("Marshaled and unmarshaled structures not equal")
	}
}

func TestMessage_IsPersistent(t *testing.T) {
	var dMode byte = 2
	message := &Message{
		ID: 1,
		Header: &ContentHeader{
			ClassID:       ClassBasic,
			Weight:        0,
			BodySize:      4,
			propertyFlags: 32768,
			PropertyList: &BasicPropertyList{
				DeliveryMode: &dMode,
			},
		},
	}
	if !message.IsPersistent() {
		t.Fatalf("Expected persistent message")
	}
}

func TestConfirmMeta_CanConfirm(t *testing.T) {
	meta := &ConfirmMeta{
		ExpectedConfirms: 5,
		ActualConfirms:   5,
	}

	if !meta.CanConfirm() {
		t.Fatalf("Expected CanConfirm true")
	}
}

func TestNewChannelError(t *testing.T) {
	er := NewChannelError(PreconditionFailed, "text", 0, 0)

	if er.ErrorType != ErrorOnChannel {
		t.Fatal("Expected channel error")
	}
}

func TestNewConnectionError(t *testing.T) {
	er := NewConnectionError(PreconditionFailed, "text", 0, 0)

	if er.ErrorType != ErrorOnConnection {
		t.Fatal("Expected connection error")
	}
}
