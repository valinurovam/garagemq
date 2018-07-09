package server

import (
	"testing"
	"time"

	"github.com/streadway/amqp"
)

func Test_BasicQos_Channel_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	prefetchCount := 128
	prefetchSize := 4096
	if err := ch.Qos(prefetchCount, prefetchSize, false); err != nil {
		t.Fatal(err)
	}

	channel := getServerChannel(sc, 1)
	if channel.qos.PrefetchCount() != uint16(prefetchCount) {
		t.Fatalf("Expected %d, actual %d", prefetchCount, channel.qos.PrefetchCount())
	}

	if channel.qos.PrefetchSize() != uint32(prefetchSize) {
		t.Fatalf("Expected %d, actual %d", prefetchSize, channel.qos.PrefetchSize())
	}
}

func Test_BasicQos_Global_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	prefetchCount := 128
	prefetchSize := 4096
	if err := ch.Qos(prefetchCount, prefetchSize, true); err != nil {
		t.Fatal(err)
	}

	channel := getServerChannel(sc, 1)
	if channel.conn.qos.PrefetchCount() != uint16(prefetchCount) {
		t.Fatalf("Expected %d, actual %d", prefetchCount, channel.qos.PrefetchCount())
	}

	if channel.conn.qos.PrefetchSize() != uint32(prefetchSize) {
		t.Fatalf("Expected %d, actual %d", prefetchSize, channel.qos.PrefetchSize())
	}
}

func Test_BasicPublish_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	queue, _ := ch.QueueDeclare("testQu", false, false, false, false, emptyTable)

	if err := ch.Publish(
		"testEx",
		queue.Name,
		false, false,
		amqp.Publishing{ContentType: "text/plain", Body: []byte("test")},
	); err != nil {
		t.Fatal(err)
	}
}

func Test_BasicPublish_Failed_ExchangeNotFound(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	c := make(chan *amqp.Error)
	ch.NotifyClose(c)

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	queue, _ := ch.QueueDeclare("testQu", false, false, false, false, emptyTable)

	if err := ch.Publish(
		"test",
		queue.Name,
		false, false,
		amqp.Publishing{ContentType: "text/plain", Body: []byte("test")},
	); err != nil {
		t.Fatal(err)
	}

	time.Sleep(5 * time.Millisecond)
	select {
	case <-c:
	default:
		t.Fatal("Expected exhchange not found error")
	}
}

func Test_BasicPublish_Failed_Immediate(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	c := make(chan *amqp.Error)
	ch.NotifyClose(c)

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	queue, _ := ch.QueueDeclare("testQu", false, false, false, false, emptyTable)

	if err := ch.Publish(
		"test",
		queue.Name,
		false, true,
		amqp.Publishing{ContentType: "text/plain", Body: []byte("test")},
	); err != nil {
		t.Fatal(err)
	}

	time.Sleep(5 * time.Millisecond)
	select {
	case <-c:
	default:
		t.Fatal("Expected Immediate not implemented error")
	}
}

func Test_BasicPublish_Failed_Mandatory(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	r := make(chan amqp.Return)
	ch.NotifyReturn(r)

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	queue, _ := ch.QueueDeclare("testQu", false, false, false, false, emptyTable)

	if err := ch.Publish(
		"testEx",
		queue.Name,
		true, false,
		amqp.Publishing{ContentType: "text/plain", Body: []byte("test")},
	); err != nil {
		t.Fatal(err)
	}

	time.Sleep(5 * time.Millisecond)
	select {
	case <-r:
	default:
		t.Fatal("Expected No route error")
	}
}

func Test_BasicConsume_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	queue, _ := ch.QueueDeclare("testQu", false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	cmr, err := ch.Consume("testQu", "tag", false, false, false, false, emptyTable)
	if err != nil {
		t.Fatal(err)
	}

	tick := time.After(10 * time.Millisecond)
	count := 0;
	leave := false
	for {
		select {
		case msg := <-cmr:
			err := ch.Ack(msg.DeliveryTag, false)
			if err != nil {
				t.Fatal(err)
			}
			count++
		case <-tick:
			leave = true
		default:

		}
		if leave {
			break
		}
	}

	time.Sleep(50 * time.Millisecond)
	if count != msgCount {
		t.Fatalf("Expected %d messages, received %d", msgCount, count)
	}
}
