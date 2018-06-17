package server

import (
	"testing"
)

func Test_QueueDeclare_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()

	if _, err := ch.QueueDeclare("test", false, false, false, false, emptyTable); err != nil {
		t.Fatal(err)
	}

	if sc.server.GetVhost("/").GetQueue("test") == nil {
		t.Fatal("Queue does not exists after 'QueueDeclare'")
	}
}

func Test_QueueDeclare_HasDefaultRoute(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()

	if _, err := ch.QueueDeclare("test", false, false, false, false, emptyTable); err != nil {
		t.Fatal(err)
	}

	bindings := sc.server.GetVhost("/").GetDefaultExchange().GetBindings()
	found := false
	for _, bind := range bindings {
		if bind.GetQueue() == "test" && bind.GetRoutingKey() == "test" {
			found = true
		}
	}

	if !found {
		t.Fatal("Default binding does not exists after QueueDeclare")
	}
}

func Test_QueueDeclare_Success_RedeclareEqual(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()

	ch.QueueDeclare("test", false, false, false, false, emptyTable)

	if _, err := ch.QueueDeclare("test", false, false, false, false, emptyTable); err != nil {
		t.Fatal(err)
	}
}

func Test_QueueDeclare_Failed_RedeclareNotEqual(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()

	ch.QueueDeclare("test", false, false, false, false, emptyTable)

	if _, err := ch.QueueDeclare("test", true, false, false, false, emptyTable); err == nil {
		t.Fatal("Expected: args inequivalent error")
	}
}

func Test_QueueDeclare_Failed_EmptyName(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()

	if _, err := ch.QueueDeclare("", false, false, false, false, emptyTable); err == nil {
		t.Fatal("Expected: queue name is requred error")
	}
}

func Test_QueueDeclarePassive_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()

	ch.QueueDeclare("test", false, false, false, false, emptyTable)

	if _, err := ch.QueueDeclarePassive("test", false, false, false, false, emptyTable); err != nil {
		t.Fatal(err)
	}
}

func Test_QueueDeclarePassive_Failed_NotExists(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()

	ch.QueueDeclare("test", false, false, false, false, emptyTable)

	if _, err := ch.QueueDeclarePassive("test2", false, false, false, false, emptyTable); err == nil {
		t.Fatal("Expected: queue not found error")
	}
}

func Test_QueueDeclareExclusive_Failed_Locked(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()
	exCH, _ := sc.clientEx.Channel()

	ch.QueueDeclare("test", false, false, true, false, emptyTable)

	if _, err := exCH.QueueDeclare("test", false, false, false, false, emptyTable); err == nil {
		t.Fatal("Expected: queue is locked error")
	}
}

func Test_QueueDeclareNotExclusive_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()
	exCH, _ := sc.clientEx.Channel()

	ch.QueueDeclare("test", false, false, false, false, emptyTable)

	if _, err := exCH.QueueDeclare("test", false, false, false, false, emptyTable); err != nil {
		t.Fatal(err)
	}
}

func Test_QueueBind_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare("testQu", false, false, false, false, emptyTable)

	if err := ch.QueueBind("testQu", "key", "testEx", false, emptyTable); err != nil {
		t.Fatal(err)
	}
}
