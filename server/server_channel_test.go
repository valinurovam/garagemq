package server

import (
	"fmt"
	"testing"
	"time"

	amqp2 "github.com/streadway/amqp"
	"github.com/valinurovam/garagemq/amqp"
)

func Test_ChannelOpen_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	_, err := sc.client.Channel()
	if err != nil {
		t.Error(err)
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
		t.Error("Expected 'channel already open' error")
	}
}

func Test_ChannelClose_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, err := sc.client.Channel()
	if err != nil {
		t.Error(err)
	}

	err = ch.Close()
	if err != nil {
		t.Error(err)
	}
}

func Test_ChannelFlow_Active_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	err := ch.Flow(true)

	if err != nil {
		t.Error(err)
	}

	channel := getServerChannel(sc, 1)
	if channel.isActive() == false {
		t.Error("Channel inactive after change flow 'true'")
	}
}

func Test_ChannelFlow_InActive_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	err := ch.Flow(false)

	if err != nil {
		t.Error(err)
	}

	channel := getServerChannel(sc, 1)
	if channel.isActive() == true {
		t.Error("Channel active after change flow 'false'")
	}
}

// useless, for coverage only
func Test_ChannelFlow_Failed_FlowOkSend(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	flowChan := make(chan bool)
	closeChan := make(chan *amqp2.Error, 1)
	ch.NotifyFlow(flowChan)
	ch.NotifyClose(closeChan)

	channel := getServerChannel(sc, 1)
	channel.SendMethod(&amqp.ChannelFlow{Active: false})

	select {
	case <-flowChan:
	case <-time.After(100 * time.Millisecond):
	}

	for notify := range flowChan {
		fmt.Println(notify)
	}

	var closeErr *amqp2.Error

	select {
	case closeErr = <-closeChan:
	case <-time.After(100 * time.Millisecond):
	}

	if closeErr == nil {
		t.Error("Expected NOT_IMPLEMENTED error")
	}
}
