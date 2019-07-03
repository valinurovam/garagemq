package server

import (
	"testing"

	"github.com/valinurovam/garagemq/exchange"
)

func Test_ServerPersist_Queue_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.QueueDeclare(t.Name(), true, false, false, false, emptyTable)
	sc.server.Stop()

	sc, _ = getNewSC(getDefaultTestConfig())
	ch, _ = sc.client.Channel()

	if _, err := ch.QueueDeclarePassive(t.Name(), false, false, false, false, emptyTable); err != nil {
		t.Error("Expected queue exists after server restart", err)
	}
}

func Test_ServerPersist_Exchange_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("testExDirect", "direct", true, false, false, false, emptyTable)
	ch.ExchangeDeclare("testExTopic", "topic", true, false, false, false, emptyTable)
	sc.server.Stop()

	sc, _ = getNewSC(getDefaultTestConfig())
	ch, _ = sc.client.Channel()

	if err := ch.ExchangeDeclarePassive("testExDirect", "direct", true, false, false, false, emptyTable); err != nil {
		t.Error("Expected exchange exists after server restart", err)
	}
	ex := sc.server.getVhost("/").GetExchange("testExDirect")
	if ex.ExType() != exchange.ExTypeDirect {
		t.Error("Expected direct exchange")
	}

	if err := ch.ExchangeDeclarePassive("testExTopic", "topic", true, false, false, false, emptyTable); err != nil {
		t.Error("Expected exchange exists after server restart", err)
	}
	ex = sc.server.getVhost("/").GetExchange("testExTopic")
	if ex.ExType() != exchange.ExTypeTopic {
		t.Error("Expected topic exchange")
	}
}
