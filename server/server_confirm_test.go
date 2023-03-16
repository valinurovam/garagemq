package server

import (
	"context"
	"testing"
	"time"

	amqpclient "github.com/rabbitmq/amqp091-go"
)

func Test_Confirm_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	err := ch.Confirm(false)

	if err != nil {
		t.Error(err)
	}

	channel := getServerChannel(sc, 1)
	if channel.confirmMode == false {
		t.Error("Channel non confirm mode")
	}
}

func Test_ConfirmReceive_Acks_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	ch.Confirm(false)

	msgCount := 10
	acks := make(chan uint64, msgCount)
	nacks := make(chan uint64, msgCount)
	ch.NotifyConfirm(acks, nacks)

	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	for i := 0; i < msgCount; i++ {
		ch.PublishWithContext(context.Background(), "", queue.Name, false, false, amqpclient.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	tick := time.After(50 * time.Millisecond)
	confirmsCount := 0
	leave := false
	for {
		select {
		case <-acks:
			confirmsCount++
		case <-nacks:
			confirmsCount--
		case <-tick:
			leave = true
		default:

		}
		if leave {
			break
		}
	}

	if confirmsCount != msgCount {
		t.Errorf("Expected %d confirms, actual %d", msgCount, confirmsCount)
	}
}

func Test_ConfirmReceive_Acks_NoRoute_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	ch.Confirm(false)

	msgCount := 10
	acks := make(chan uint64, msgCount)
	nacks := make(chan uint64, msgCount)
	ch.NotifyConfirm(acks, nacks)

	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	for i := 0; i < msgCount; i++ {
		ch.PublishWithContext(context.Background(), "", "bad-route", false, false, amqpclient.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	tick := time.After(50 * time.Millisecond)
	confirmsCount := 0
	leave := false
	for {
		select {
		case <-acks:
			confirmsCount++
		case <-nacks:
			confirmsCount--
		case <-tick:
			leave = true
		default:

		}
		if leave {
			break
		}
	}

	if confirmsCount != msgCount {
		t.Errorf("Expected %d confirms, actual %d", msgCount, confirmsCount)
	}
}

func Test_ConfirmReceive_Acks_Persistent_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	ch.Confirm(false)

	msgCount := 10
	acks := make(chan uint64, msgCount)
	nacks := make(chan uint64, msgCount)
	ch.NotifyConfirm(acks, nacks)

	queue, _ := ch.QueueDeclare(t.Name(), true, false, false, false, emptyTable)

	for i := 0; i < msgCount; i++ {
		ch.PublishWithContext(context.Background(), "", queue.Name, false, false, amqpclient.Publishing{ContentType: "text/plain", Body: []byte("test"), DeliveryMode: amqpclient.Persistent})
	}

	tick := time.After(50 * time.Millisecond)
	confirmsCount := 0
	leave := false
	for {
		select {
		case <-acks:
			confirmsCount++
		case <-nacks:
			confirmsCount--
		case <-tick:
			leave = true
		default:

		}
		if leave {
			break
		}
	}

	if confirmsCount != msgCount {
		t.Errorf("Expected %d confirms, actual %d", msgCount, confirmsCount)
	}
}
