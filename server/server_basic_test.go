package server

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/streadway/amqp"
	amqp2 "github.com/valinurovam/garagemq/amqp"
)

func Test_BasicQos_Channel_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	prefetchCount := 128
	prefetchSize := 4096
	if err := ch.Qos(prefetchCount, prefetchSize, false); err != nil {
		t.Error(err)
	}

	channel := getServerChannel(sc, 1)
	if channel.qos.PrefetchCount() != 0 {
		t.Errorf("Expected %d, actual %d", prefetchCount, channel.qos.PrefetchCount())
	}

	if channel.qos.PrefetchSize() != 0 {
		t.Errorf("Expected %d, actual %d", prefetchSize, channel.qos.PrefetchSize())
	}
}

func Test_BasicQos_Global_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	prefetchCount := 128
	prefetchSize := 4096
	if err := ch.Qos(prefetchCount, prefetchSize, true); err != nil {
		t.Error(err)
	}

	channel := getServerChannel(sc, 1)
	if channel.qos.PrefetchCount() != uint16(prefetchCount) {
		t.Errorf("Expected %d, actual %d", prefetchCount, channel.qos.PrefetchCount())
	}

	if channel.qos.PrefetchSize() != uint32(prefetchSize) {
		t.Errorf("Expected %d, actual %d", prefetchSize, channel.qos.PrefetchSize())
	}
}

func Test_BasicQos_Check_Global_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	prefetchCount := 5
	if err := ch.Qos(prefetchCount, 0, true); err != nil {
		t.Error(err)
	}

	// For consumer we set more than channel qos, for test only global trigger
	if err := ch.Qos(prefetchCount*2, 0, false); err != nil {
		t.Error(err)
	}
	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	cmr, err := ch.Consume(t.Name(), "tag", false, false, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	if len(getServerChannel(sc, 1).consumers) != 1 {
		t.Error("Expected consumers on channel consumers map")
	}

	tick := time.After(100 * time.Millisecond)
	count := 0
	leave := false
	for {
		select {
		case <-cmr:
			count++
		case <-tick:
			leave = true
		default:

		}
		if leave {
			break
		}
	}

	time.Sleep(100 * time.Millisecond)
	if count != prefetchCount {
		t.Errorf("Expected %d messages, received %d", prefetchCount, count)
	}
}

func Test_BasicQos_Check_NonGlobal_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	prefetchCountGlobal := 10
	if err := ch.Qos(prefetchCountGlobal, 0, true); err != nil {
		t.Error(err)
	}

	// For consumer we set less than channel qos, for test only consumer trigger
	prefetchCountCmr := 5
	if err := ch.Qos(prefetchCountCmr, 0, false); err != nil {
		t.Error(err)
	}
	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	cmr, err := ch.Consume(t.Name(), "tag", false, false, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	if len(getServerChannel(sc, 1).consumers) != 1 {
		t.Error("Expected consumers on channel consumers map")
	}

	tick := time.After(100 * time.Millisecond)
	count := 0
	leave := false
	for {
		select {
		case <-cmr:
			count++
		case <-tick:
			leave = true
		default:

		}
		if leave {
			break
		}
	}

	time.Sleep(100 * time.Millisecond)
	if count != prefetchCountCmr {
		t.Errorf("Expected %d messages, received %d", prefetchCountCmr, count)
	}
}

func Test_BasicPublish_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	if err := ch.Publish(
		"testEx",
		queue.Name,
		false, false,
		amqp.Publishing{ContentType: "text/plain", Body: []byte("test")},
	); err != nil {
		t.Error(err)
	}
}

func Test_BasicPublish_Persistent_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	qu, _ := ch.QueueDeclare(t.Name(), true, false, false, false, emptyTable)

	if err := ch.Publish(
		"",
		qu.Name,
		false, false,
		amqp.Publishing{ContentType: "text/plain", Body: []byte("testMessage"), DeliveryMode: amqp.Persistent},
	); err != nil {
		t.Error(err)
	}

	// wait call persistStorage()
	time.Sleep(100 * time.Millisecond)
	sc.server.Stop()

	sc, _ = getNewSC(getDefaultTestConfig())
	ch, _ = sc.client.Channel()

	msg, ok, errGet := ch.Get(t.Name(), true)
	if errGet != nil {
		t.Error(errGet)
	}

	if !ok {
		t.Error("Persistent message not found after server restart")
	}

	if !bytes.Equal(msg.Body, []byte("testMessage")) {
		t.Error("Received strange message after server restart")
	}
}

func Test_BasicPublish_Persistent_Failed_QueueNonDurable(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	qu, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	if err := ch.Publish(
		"",
		qu.Name,
		false, false,
		amqp.Publishing{ContentType: "text/plain", Body: []byte("test"), DeliveryMode: amqp.Persistent},
	); err != nil {
		t.Error(err)
	}

	time.Sleep(100 * time.Millisecond)

	found := false
	vhost := sc.server.getVhost("/")
	storage := vhost.msgStorageP
	storage.Iterate(func(queue string, message *amqp2.Message) {
		found = true
	})

	if found {
		t.Fatal("Persistent message found after server restart on message store")
	}
}

func Test_BasicPublish_Failed_ExchangeNotFound(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	c := make(chan *amqp.Error)
	ch.NotifyClose(c)

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	if err := ch.Publish(
		"test",
		queue.Name,
		false, false,
		amqp.Publishing{ContentType: "text/plain", Body: []byte("test")},
	); err != nil {
		t.Error(err)
	}

	time.Sleep(100 * time.Millisecond)
	select {
	case <-c:
	default:
		t.Error("Expected exhchange not found error")
	}
}

func Test_BasicPublish_Failed_Immediate(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	c := make(chan *amqp.Error)
	ch.NotifyClose(c)

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	if err := ch.Publish(
		"test",
		queue.Name,
		false, true,
		amqp.Publishing{ContentType: "text/plain", Body: []byte("test")},
	); err != nil {
		t.Error(err)
	}

	time.Sleep(100 * time.Millisecond)
	select {
	case <-c:
	default:
		t.Error("Expected Immediate not implemented error")
	}
}

func Test_BasicPublish_Failed_Mandatory(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	r := make(chan amqp.Return)
	ch.NotifyReturn(r)

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	if err := ch.Publish(
		"testEx",
		queue.Name,
		true, false,
		amqp.Publishing{ContentType: "text/plain", Body: []byte("test")},
	); err != nil {
		t.Error(err)
	}

	time.Sleep(100 * time.Millisecond)
	select {
	case <-r:
	default:
		t.Error("Expected No route error")
	}
}

func Test_BasicConsume_WithOrderCheck_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test" + strconv.Itoa(i))})
	}

	cmr, err := ch.Consume(t.Name(), "tag", false, false, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	if len(getServerChannel(sc, 1).consumers) != 1 {
		t.Error("Expected consumers on channel consumers map")
	}

	tick := time.After(100 * time.Millisecond)
	count := 0
	leave := false
	for {
		select {
		case msg := <-cmr:
			err := ch.Ack(msg.DeliveryTag, false)
			if err != nil {
				t.Error(err)
			}
			// order check
			expectedBody := []byte("test" + strconv.Itoa(count))
			if !bytes.Equal(msg.Body, expectedBody) {
				t.Errorf("Expected body %s, actual %s", expectedBody, msg.Body)
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

	time.Sleep(100 * time.Millisecond)
	if count != msgCount {
		t.Errorf("Expected %d messages, received %d", msgCount, count)
	}
}

func Test_BasicConsume_Failed_QueueNotFound(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	_, err := ch.Consume("test", "tag", false, false, false, false, emptyTable)
	if err == nil {
		t.Error("Expected NOT_FOUND error")
	}
}

func Test_BasicConsume_Failed_SameTag(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	_, err := ch.Consume(t.Name(), "tag", false, false, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	_, err = ch.Consume(t.Name(), "tag", false, false, false, false, emptyTable)
	if err == nil {
		t.Error("Expected NOT_ALLOWED error")
	}
}

func Test_BasicConsume_Failed_ExclusiveQueue(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch1, _ := sc.client.Channel()
	ch2, _ := sc.client.Channel()

	ch1.QueueDeclare("testQu_Ex1", false, false, false, false, emptyTable)
	ch1.QueueDeclare("testQu_Ex2", false, false, false, false, emptyTable)

	_, err := ch1.Consume("testQu_Ex1", "tag_ex1", false, true, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	_, err = ch1.Consume("testQu_Ex1", "tag1", false, false, false, false, emptyTable)
	if err == nil {
		t.Error("Expected ACCESS_REFUSED error")
	}

	_, err = ch2.Consume("testQu_Ex2", "tag_ex2", false, false, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	_, err = ch2.Consume("testQu_Ex2", "tag2", false, true, false, false, emptyTable)
	if err == nil {
		t.Error("Expected ACCESS_REFUSED error")
	}
}

func Test_BasicConsume_Success_Exclusive_Reconsume(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	_, err := ch.Consume(t.Name(), "tag", false, true, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	if err = ch.Cancel("tag", false); err != nil {
		t.Error(err)
	}

	// after cancel exclusive consumer server must allow us to consume again
	_, err = ch.Consume(t.Name(), "tag", false, true, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}
}

func Test_BasicCancel_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	_, err := ch.Consume(t.Name(), "tag", false, false, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	if len(getServerChannel(sc, 1).consumers) != 1 {
		t.Error("Expected consumers on channel consumers map")
	}

	if err = ch.Cancel("tag", false); err != nil {
		t.Error(err)
	}

	if len(getServerChannel(sc, 1).consumers) != 0 {
		t.Error("Expected empty channel consumers map")
	}
}

func Test_BasicCancel_Failed_Unknown_Tag(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	_, err := ch.Consume(t.Name(), "tag", false, false, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	if err = ch.Cancel("tag_unknown", false); err == nil {
		t.Error("Expected NOT_FOUND error")
	}
}

func Test_BasicAck_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	cmr, err := ch.Consume(t.Name(), "tag", false, false, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	tick := time.After(100 * time.Millisecond)
	count := 0
	leave := false
	deliveries := make([]amqp.Delivery, 0, 10)
	for {
		select {
		case msg := <-cmr:
			deliveries = append(deliveries, msg)
			count++
		case <-tick:
			leave = true
		default:

		}
		if leave {
			break
		}
	}

	unackedLength := len(getServerChannel(sc, 1).ackStore)
	if unackedLength != msgCount {
		t.Errorf("Expected %d unacked, actual %d", msgCount, unackedLength)
	}
	for _, dlv := range deliveries {
		ch.Ack(dlv.DeliveryTag, false)
	}

	time.Sleep(100 * time.Millisecond)

	unackedLength = len(getServerChannel(sc, 1).ackStore)
	if unackedLength != 0 {
		t.Errorf("Expected %d unacked, actual %d", 0, unackedLength)
	}
}

func Test_BasicAckMultiple_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	cmr, err := ch.Consume(t.Name(), "tag", false, false, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	tick := time.After(100 * time.Millisecond)
	count := 0
	leave := false
	deliveries := make([]amqp.Delivery, 0, 10)
	for {
		select {
		case msg := <-cmr:
			deliveries = append(deliveries, msg)
			count++
		case <-tick:
			leave = true
		default:

		}
		if leave {
			break
		}
	}

	unackedLength := len(getServerChannel(sc, 1).ackStore)
	if unackedLength != msgCount {
		t.Errorf("Expected %d unacked, actual %d", msgCount, unackedLength)
	}

	dlv := deliveries[len(deliveries)-1]
	ch.Ack(dlv.DeliveryTag, true)

	time.Sleep(100 * time.Millisecond)

	unackedLength = len(getServerChannel(sc, 1).ackStore)
	if unackedLength != 0 {
		t.Errorf("Expected %d unacked, actual %d", 0, unackedLength)
	}
}

func Test_BasicNack_RequeueTrue_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	cmr, err := ch.Consume(t.Name(), "tag", false, false, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	tick := time.After(100 * time.Millisecond)
	count := 0
	leave := false
	deliveries := make([]amqp.Delivery, 0, 10)
	for {
		select {
		case msg := <-cmr:
			deliveries = append(deliveries, msg)
			count++
		case <-tick:
			leave = true
		default:

		}
		if leave {
			break
		}
	}

	ch.Cancel("tag", false)

	unackedLength := len(getServerChannel(sc, 1).ackStore)
	if unackedLength != msgCount {
		t.Errorf("Expected %d unacked, actual %d", msgCount, unackedLength)
	}
	for _, dlv := range deliveries {
		ch.Nack(dlv.DeliveryTag, false, true)
	}

	time.Sleep(100 * time.Millisecond)

	unackedLength = len(getServerChannel(sc, 1).ackStore)
	if unackedLength != 0 {
		t.Errorf("Expected %d unacked, actual %d", 0, unackedLength)
	}

	queueLength := sc.server.getVhost("/").GetQueue(queue.Name).Length()

	if int(queueLength) != msgCount {
		t.Errorf("Expected %d queue length, actual %d", msgCount, queueLength)
	}
}

func Test_BasicNack_RequeueTrue_Multiple_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	cmr, err := ch.Consume(t.Name(), "tag", false, false, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	tick := time.After(100 * time.Millisecond)
	count := 0
	leave := false
	deliveries := make([]amqp.Delivery, 0, 10)
	for {
		select {
		case msg := <-cmr:
			deliveries = append(deliveries, msg)
			count++
		case <-tick:
			leave = true
		default:

		}
		if leave {
			break
		}
	}

	ch.Cancel("tag", false)

	unackedLength := len(getServerChannel(sc, 1).ackStore)
	if unackedLength != msgCount {
		t.Errorf("Expected %d unacked, actual %d", msgCount, unackedLength)
	}

	dlv := deliveries[len(deliveries)-1]
	ch.Nack(dlv.DeliveryTag, true, true)

	time.Sleep(100 * time.Millisecond)

	unackedLength = len(getServerChannel(sc, 1).ackStore)
	if unackedLength != 0 {
		t.Errorf("Expected %d unacked, actual %d", 0, unackedLength)
	}

	queueLength := sc.server.getVhost("/").GetQueue(queue.Name).Length()

	if int(queueLength) != msgCount {
		t.Errorf("Expected %d queue length, actual %d", msgCount, queueLength)
	}
}

func Test_BasicNack_RequeueFalse_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	cmr, err := ch.Consume(t.Name(), "tag", false, false, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	tick := time.After(100 * time.Millisecond)
	count := 0
	leave := false
	deliveries := make([]amqp.Delivery, 0, 10)
	for {
		select {
		case msg := <-cmr:
			deliveries = append(deliveries, msg)
			count++
		case <-tick:
			leave = true
		default:

		}
		if leave {
			break
		}
	}

	unackedLength := len(getServerChannel(sc, 1).ackStore)
	if unackedLength != msgCount {
		t.Errorf("Expected %d unacked, actual %d", msgCount, unackedLength)
	}
	for _, dlv := range deliveries {
		ch.Nack(dlv.DeliveryTag, false, false)
	}

	time.Sleep(100 * time.Millisecond)

	unackedLength = len(getServerChannel(sc, 1).ackStore)
	if unackedLength != 0 {
		t.Errorf("Expected %d unacked, actual %d", 0, unackedLength)
	}

	queueLength := sc.server.getVhost("/").GetQueue(queue.Name).Length()

	if queueLength != 0 {
		t.Errorf("Expected empty queue after nack with requeue false")
	}
}

func Test_BasicNack_RequeueFalse_Multiple_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	cmr, err := ch.Consume(t.Name(), "tag", false, false, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	tick := time.After(100 * time.Millisecond)
	count := 0
	leave := false
	deliveries := make([]amqp.Delivery, 0, 10)
	for {
		select {
		case msg := <-cmr:
			deliveries = append(deliveries, msg)
			count++
		case <-tick:
			leave = true
		default:

		}
		if leave {
			break
		}
	}

	unackedLength := len(getServerChannel(sc, 1).ackStore)
	if unackedLength != msgCount {
		t.Errorf("Expected %d unacked, actual %d", msgCount, unackedLength)
	}
	dlv := deliveries[len(deliveries)-1]
	ch.Nack(dlv.DeliveryTag, true, false)

	time.Sleep(100 * time.Millisecond)

	unackedLength = len(getServerChannel(sc, 1).ackStore)
	if unackedLength != 0 {
		t.Errorf("Expected %d unacked, actual %d", 0, unackedLength)
	}

	queueLength := sc.server.getVhost("/").GetQueue(queue.Name).Length()

	if queueLength != 0 {
		t.Errorf("Expected empty queue after nack with requeue false")
	}
}

func Test_BasicReject_RequeueTrue_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	cmr, err := ch.Consume(t.Name(), "tag", false, false, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	tick := time.After(100 * time.Millisecond)
	count := 0
	leave := false
	deliveries := make([]amqp.Delivery, 0, 10)
	for {
		select {
		case msg := <-cmr:
			deliveries = append(deliveries, msg)
			count++
		case <-tick:
			leave = true
		default:

		}
		if leave {
			break
		}
	}

	ch.Cancel("tag", false)

	unackedLength := len(getServerChannel(sc, 1).ackStore)
	if unackedLength != msgCount {
		t.Errorf("Expected %d unacked, actual %d", msgCount, unackedLength)
	}
	for _, dlv := range deliveries {
		ch.Reject(dlv.DeliveryTag, true)
	}

	time.Sleep(100 * time.Millisecond)

	unackedLength = len(getServerChannel(sc, 1).ackStore)
	if unackedLength != 0 {
		t.Errorf("Expected %d unacked, actual %d", 0, unackedLength)
	}

	queueLength := sc.server.getVhost("/").GetQueue(queue.Name).Length()

	if int(queueLength) != msgCount {
		t.Errorf("Expected %d queue length, actual %d", msgCount, queueLength)
	}
}

func Test_BasicReject_RequeueFalse_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	cmr, err := ch.Consume(t.Name(), "tag", false, false, false, false, emptyTable)
	if err != nil {
		t.Error(err)
	}

	tick := time.After(100 * time.Millisecond)
	count := 0
	leave := false
	deliveries := make([]amqp.Delivery, 0, 10)
	for {
		select {
		case msg := <-cmr:
			deliveries = append(deliveries, msg)
			count++
		case <-tick:
			leave = true
		default:

		}
		if leave {
			break
		}
	}

	unackedLength := len(getServerChannel(sc, 1).ackStore)
	if unackedLength != msgCount {
		t.Errorf("Expected %d unacked, actual %d", msgCount, unackedLength)
	}
	for _, dlv := range deliveries {
		ch.Reject(dlv.DeliveryTag, false)
	}

	time.Sleep(100 * time.Millisecond)

	unackedLength = len(getServerChannel(sc, 1).ackStore)
	if unackedLength != 0 {
		t.Errorf("Expected %d unacked, actual %d", 0, unackedLength)
	}

	queueLength := sc.server.getVhost("/").GetQueue(queue.Name).Length()

	if queueLength != 0 {
		t.Errorf("Expected empty queue after nack with requeue false")
	}
}

func Test_BasicGet_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	qu, _ := ch.QueueDeclare(t.Name(), true, false, false, false, emptyTable)

	if err := ch.Publish(
		"",
		qu.Name,
		false, false,
		amqp.Publishing{ContentType: "text/plain", Body: []byte("testMessage")},
	); err != nil {
		t.Error(err)
	}

	msg, ok, errGet := ch.Get(t.Name(), true)
	if errGet != nil {
		t.Error(errGet)
	}

	if !ok {
		t.Error("Message not found")
	}

	if !bytes.Equal(msg.Body, []byte("testMessage")) {
		t.Error("Received strange message")
	}
}

func Test_BasicGet_Success_Empty(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.QueueDeclare(t.Name(), true, false, false, false, emptyTable)

	_, ok, errGet := ch.Get(t.Name(), true)
	if errGet != nil {
		t.Error(errGet)
	}

	if ok {
		t.Error("Expected BasicEmpty")
	}
}

func Test_BasicGet_Success_WithAck(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	qu, _ := ch.QueueDeclare(t.Name(), true, false, false, false, emptyTable)

	if err := ch.Publish(
		"",
		qu.Name,
		false, false,
		amqp.Publishing{ContentType: "text/plain", Body: []byte("testMessage")},
	); err != nil {
		t.Error(err)
	}

	ch.Get(t.Name(), false)
	unackedLength := len(getServerChannel(sc, 1).ackStore)
	if unackedLength != 1 {
		t.Errorf("Expected %d unacked, actual %d", 1, unackedLength)
	}

}

func Test_BasicGet_Succ(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.QueueDeclare(t.Name(), true, false, false, false, emptyTable)

	_, _, errGet := ch.Get("testQu_unknown", true)
	if errGet == nil {
		t.Error("Expected NOT_FOUND error")
	}
}

func Test_BasicGet_Failed_QueueNotFound(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.QueueDeclare(t.Name(), true, false, false, false, emptyTable)

	_, _, errGet := ch.Get("testQu_unknown", true)
	if errGet == nil {
		t.Error("Expected NOT_FOUND error")
	}
}
