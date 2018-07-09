package server

import (
	"testing"

	"github.com/valinurovam/garagemq/exchange"
)

func Test_DefaultExchanges(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	vhost := sc.server.GetVhost("/")

	exchanges := []string{"direct", "fanout", "headers", "topic"}
	for _, name := range exchanges {
		name = "amq." + name
		if vhost.GetExchange(name) == nil {
			t.Fatalf("Default exchange '%s' does not exists", name)
		}
	}

	systemExchange := vhost.GetDefaultExchange()
	if systemExchange == nil {
		t.Fatal("Sytem exchange does not exists")
	}

	if systemExchange.ExType() != exchange.EX_TYPE_DIRECT {
		t.Fatalf("Expected: 'direct' system exchange kind")
	}
}

func Test_ExchangeDeclare_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	if err := ch.ExchangeDeclare("test", "direct", false, false, false, false, emptyTable); err != nil {
		t.Fatal(err)
	}

	if sc.server.GetVhost("/").GetExchange("test") == nil {
		t.Fatal("Exchange does not exists after 'ExchangeDeclare'")
	}
}

func Test_ExchangeDeclareDurable_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	if err := ch.ExchangeDeclare("test", "direct", true, false, false, false, emptyTable); err != nil {
		t.Fatal(err)
	}

	if sc.server.GetVhost("/").GetExchange("test") == nil {
		t.Fatal("Exchange does not exists after 'ExchangeDeclareDurable'")
	}

	storedExchanges := sc.server.storage.GetVhostExchanges("/")
	if len(storedExchanges) == 0 {
		t.Fatal("Queue does not exists into storage after 'ExchangeDeclareDurable'")
	}
	found := false
	for _, ex := range storedExchanges {
		if ex.GetName() == "test" {
			found = true
		}
	}

	if !found {
		t.Fatal("Exchange does not exists into storage after 'ExchangeDeclareDurable'")
	}
}

func Test_ExchangeDeclare_Success_RedeclareEqual(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("test", "direct", false, false, false, false, emptyTable)

	if err := ch.ExchangeDeclare("test", "direct", false, false, false, false, emptyTable); err != nil {
		t.Fatal(err)
	}
}

func Test_ExchangeDeclare_Failed_WrongType(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	if err := ch.ExchangeDeclare("test", "test", false, false, false, false, emptyTable); err == nil {
		t.Fatal("Expected NotImplemented error")
	}
}

func Test_ExchangeDeclare_Failed_RedeclareNotEqual(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("test", "direct", false, false, false, false, emptyTable)

	if err := ch.ExchangeDeclare("test", "direct", false, true, false, false, emptyTable); err == nil {
		t.Fatal("Expected: args inequivalent error")
	}
}

func Test_ExchangeDeclare_Failed_EmptyName(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	if err := ch.ExchangeDeclare("", "direct", false, false, false, false, emptyTable); err == nil {
		t.Fatal("Expected: exchange name is requred error")
	}
}

func Test_ExchangeDeclare_Failed_DefaultName(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	if err := ch.ExchangeDeclare("amq.direct", "direct", false, false, false, false, emptyTable); err == nil {
		t.Fatal("Expected: access refused error")
	}
}

func Test_ExchangeDeclarePassive_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("test", "direct", false, false, false, false, emptyTable)

	if err := ch.ExchangeDeclarePassive("test", "direct", false, false, false, false, emptyTable); err != nil {
		t.Fatal(err)
	}
}

func Test_ExchangeDeclarePassive_Failed_NotExists(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("test", "direct", false, false, false, false, emptyTable)

	if err := ch.ExchangeDeclarePassive("test2", "direct", false, false, false, false, emptyTable); err == nil {
		t.Fatal("Expected: exchange not found error")
	}
}
