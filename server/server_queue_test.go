package server

import (
	"testing"
	"github.com/streadway/amqp"
	"time"
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

	found := false
	for _, bind := range sc.server.GetVhost("/").GetExchange("testEx").GetBindings() {
		if bind.GetQueue() == "testQu" && bind.GetRoutingKey() == "key" && bind.GetExchange() == "testEx" {
			found = true
		}
	}
	if !found {
		t.Fatal("Binding does not exists after QueueBind")
	}
}

func Test_QueueBind_Success_Rebind(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare("testQu", false, false, false, false, emptyTable)

	ch.QueueBind("testQu", "key", "testEx", false, emptyTable)
	if err := ch.QueueBind("testQu", "key", "testEx", false, emptyTable); err != nil {
		t.Fatal(err)
	}

	foundCount := 0
	for _, bind := range sc.server.GetVhost("/").GetExchange("testEx").GetBindings() {
		if bind.GetQueue() == "testQu" && bind.GetRoutingKey() == "key" && bind.GetExchange() == "testEx" {
			foundCount++
		}
	}
	if foundCount != 1 {
		t.Fatal("Binding does not exists after QueueBind or duplicated after rebind")
	}
}

func Test_QueueBind_Failed_OnExclusiveQueue(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()
	chEx, _ := sc.clientEx.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare("testQu", false, false, true, false, emptyTable)

	if err := chEx.QueueBind("testQu", "key", "testEx", false, emptyTable); err == nil {
		t.Fatal("Expected: queue is locked error")
	}
}

func Test_QueueUnbind_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare("testQu", false, false, false, false, emptyTable)

	ch.QueueBind("testQu", "key", "testEx", false, emptyTable)
	ch.QueueUnbind("testQu", "key", "testEx", emptyTable)

	found := false
	for _, bind := range sc.server.GetVhost("/").GetExchange("testEx").GetBindings() {
		if bind.GetQueue() == "testQu" && bind.GetRoutingKey() == "key" && bind.GetExchange() == "testEx" {
			found = true
		}
	}
	if found {
		t.Fatal("Binding exists after QueueUnbind")
	}
}

func Test_QueuePurge_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()
	queue, _ := ch.QueueDeclare("test", false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	time.Sleep(time.Millisecond)
	length := sc.server.GetVhost("/").GetQueue("test").Length()
	if length != uint64(msgCount) {
		t.Fatalf("Expected: queue.length = %d, %d given", msgCount, length)
	}

	purgedCount, err := ch.QueuePurge("test", false)
	if err != nil {
		t.Fatal(err)
	}

	if purgedCount != msgCount {
		t.Fatalf("Expected: purgedCount = %d, %d given", msgCount, length)
	}
	length = sc.server.GetVhost("/").GetQueue("test").Length()
	if length != 0 {
		t.Fatalf("Queue is not empty after QueuePurge")
	}
}

func Test_QueueDelete_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()
	queue, _ := ch.QueueDeclare("test", false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	time.Sleep(time.Millisecond)
	var deletedCount, err = ch.QueueDelete("test", false, false, false)
	if err != nil {
		t.Fatal(err)
	}

	if deletedCount != msgCount {
		t.Fatalf("Expected: deleteCount = %d, %d given", msgCount, deletedCount)
	}
}

func Test_QueueDelete_Failed_NotEmpty(t *testing.T) {
	sc, _ := getNewSC(getDefaultServerConfig())
	ch, _ := sc.client.Channel()
	queue, _ := ch.QueueDeclare("test", false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	time.Sleep(time.Millisecond)
	var _, err = ch.QueueDelete("test", false, true, false)
	if err == nil {
		t.Fatal("Expected: queue has messages error")
	}
}
