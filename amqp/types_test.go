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
		ChannelId:  0,
		Payload:    []byte{'t', 'e', 's', 't'},
		CloseAfter: false,
	})

	if m.BodySize != 4 {
		t.Fatalf("Expected BodySize %d, actual %d", 4, m.BodySize)
	}

	m.Append(&Frame{
		Type:       0,
		ChannelId:  0,
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
		Id: 1,
		Header: &ContentHeader{
			ClassId:       ClassBasic,
			Weight:        0,
			BodySize:      4,
			propertyFlags: 32768,
			PropertyList: &BasicPropertyList{
				ContentType:     &ctype,
				ContentEncoding: nil,
				Headers:         nil,
				DeliveryMode:    nil,
				Priority:        nil,
				CorrelationId:   nil,
				ReplyTo:         nil,
				Expiration:      nil,
				MessageId:       nil,
				Timestamp:       nil,
				Type:            nil,
				UserId:          nil,
				AppId:           nil,
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
				ChannelId:  1,
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
