package server

import (
	"testing"

	"github.com/valinurovam/garagemq/amqp"
)

func Test_ChannelOpen_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	_, err := sc.client.Channel()
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ChannelOpen_FailedReopen(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	sc.client.Channel()

	// Is here way to test without internal source of server?
	channel := getServerChannel(sc, 1)
	amqpErr := channel.handleMethod(&amqp.ChannelOpen{})
	if amqpErr == nil {
		t.Fatal("Expected 'channel already open' error")
	}
}

func Test_ChannelClose_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, err := sc.client.Channel()
	if err != nil {
		t.Fatal(err)
	}

	err = ch.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ChannelFlow_Active_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	err := ch.Flow(true)

	if err != nil {
		t.Fatal(err)
	}

	channel := getServerChannel(sc, 1)
	if channel.isActive() == false {
		t.Fatal("Channel inactive after change flow 'true'")
	}
}

func Test_ChannelFlow_InActive_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	err := ch.Flow(false)

	if err != nil {
		t.Fatal(err)
	}

	channel := getServerChannel(sc, 1)
	if channel.isActive() == true {
		t.Fatal("Channel active after change flow 'false'")
	}
}