package server

import (
	"testing"
	"time"

	"github.com/streadway/amqp"
)

func Test_QueueDeclare_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	if _, err := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable); err != nil {
		t.Error(err)
	}

	if sc.server.getVhost("/").GetQueue(t.Name()) == nil {
		t.Error("Queue does not exists after 'QueueDeclare'")
	}
}

func Test_QueueDeclareDurable_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	if _, err := ch.QueueDeclare(t.Name(), true, false, false, false, emptyTable); err != nil {
		t.Error(err)
	}

	if sc.server.getVhost("/").GetQueue(t.Name()) == nil {
		t.Error("Queue does not exists after 'QueueDeclare'")
	}

	storedQueues := sc.server.storage.GetVhostQueues("/")
	if len(storedQueues) == 0 {
		t.Error("Queue does not exists into storage after 'QueueDeclareDurable'")
	}
	found := false
	for _, q := range storedQueues {
		if q.GetName() == t.Name() {
			found = true
		}
	}

	if !found {
		t.Error("Queue does not exists into storage after 'QueueDeclareDurable'")
	}
}

func Test_QueueDeclare_HasDefaultRoute(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	if _, err := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable); err != nil {
		t.Error(err)
	}

	bindings := sc.server.getVhost("/").GetDefaultExchange().GetBindings()
	found := false
	for _, bind := range bindings {
		if bind.GetQueue() == t.Name() && bind.GetRoutingKey() == t.Name() {
			found = true
		}
	}

	if !found {
		t.Error("Default binding does not exists after QueueDeclare")
	}
}

func Test_QueueDeclare_Success_RedeclareEqual(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	if _, err := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable); err != nil {
		t.Error(err)
	}
}

func Test_QueueDeclare_Failed_RedeclareNotEqual(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	if _, err := ch.QueueDeclare(t.Name(), true, false, false, false, emptyTable); err == nil {
		t.Error("Expected: args inequivalent error")
	}
}

func Test_QueueDeclare_Failed_EmptyName(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	if _, err := ch.QueueDeclare("", false, false, false, false, emptyTable); err == nil {
		t.Error("Expected: queue name is required error")
	}
}

func Test_QueueDeclarePassive_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	if _, err := ch.QueueDeclarePassive(t.Name(), false, false, false, false, emptyTable); err != nil {
		t.Error(err)
	}
}

func Test_QueueDeclarePassive_Failed_NotExists(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	if _, err := ch.QueueDeclarePassive(t.Name() + "new", false, false, false, false, emptyTable); err == nil {
		t.Error("Expected: queue not found error")
	}
}

func Test_QueueDeclarePassive_Failed_Locked(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	exCH, _ := sc.clientEx.Channel()

	ch.QueueDeclare(t.Name(), false, false, true, false, emptyTable)

	if _, err := exCH.QueueDeclarePassive("test", false, false, false, false, emptyTable); err == nil {
		t.Error("Expected: queue is locked error")
	}
}

func Test_QueueDeclareExclusive_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.QueueDeclare(t.Name(), false, false, true, false, emptyTable)
	ch.Close()
	sc.client.Close()

	time.Sleep(100 * time.Millisecond)
	if sc.server.getVhost("/").GetQueue("test") != nil {
		t.Error("Exclusive queue exists after connection close")
	}
}

func Test_QueueDeclareExclusive_Failed_Locked(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	exCH, _ := sc.clientEx.Channel()

	ch.QueueDeclare(t.Name(), false, false, true, false, emptyTable)

	if _, err := exCH.QueueDeclare(t.Name(), false, false, false, false, emptyTable); err == nil {
		t.Error("Expected: queue is locked error")
	}
}

func Test_QueueDeclareNotExclusive_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	exCH, _ := sc.clientEx.Channel()

	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	if _, err := exCH.QueueDeclare(t.Name(), false, false, false, false, emptyTable); err != nil {
		t.Error(err)
	}
}

func Test_QueueBind_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	if err := ch.QueueBind(t.Name(), "key", "testEx", false, emptyTable); err != nil {
		t.Error(err)
	}

	found := false
	for _, bind := range sc.server.getVhost("/").GetExchange("testEx").GetBindings() {
		if bind.GetQueue() == t.Name() && bind.GetRoutingKey() == "key" && bind.GetExchange() == "testEx" {
			found = true
		}
	}
	if !found {
		t.Error("Binding does not exists after QueueBind")
	}
}

func Test_QueueBind_Success_Rebind(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	ch.QueueBind(t.Name(), "key", "testEx", false, emptyTable)
	if err := ch.QueueBind(t.Name(), "key", "testEx", false, emptyTable); err != nil {
		t.Error(err)
	}

	foundCount := 0
	for _, bind := range sc.server.getVhost("/").GetExchange("testEx").GetBindings() {
		if bind.GetQueue() == t.Name() && bind.GetRoutingKey() == "key" && bind.GetExchange() == "testEx" {
			foundCount++
		}
	}
	if foundCount != 1 {
		t.Error("Binding does not exists after QueueBind or duplicated after rebind")
	}
}

func Test_QueueBind_Failed_ExchangeNotExists(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare(t.Name(), false, false, true, false, emptyTable)

	if err := ch.QueueBind(t.Name(), "key", "test_Ex", false, emptyTable); err == nil {
		t.Error("Expected: exchange does not exists")
	}
}

func Test_QueueBind_Failed_QueueNotExists(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare(t.Name(), false, false, true, false, emptyTable)

	if err := ch.QueueBind("test_Qu", "key", "testEx", false, emptyTable); err == nil {
		t.Error("Expected: queue does not exists")
	}
}

func Test_QueueBind_Failed_OnExclusiveQueue(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	chEx, _ := sc.clientEx.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare(t.Name(), false, false, true, false, emptyTable)

	if err := chEx.QueueBind(t.Name(), "key", "testEx", false, emptyTable); err == nil {
		t.Error("Expected: queue is locked error")
	}
}

func Test_QueueUnbind_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	ch.QueueBind(t.Name(), "key", "testEx", false, emptyTable)
	ch.QueueUnbind(t.Name(), "key", "testEx", emptyTable)

	found := false
	for _, bind := range sc.server.getVhost("/").GetExchange("testEx").GetBindings() {
		if bind.GetQueue() == t.Name() && bind.GetRoutingKey() == "key" && bind.GetExchange() == "testEx" {
			found = true
		}
	}
	if found {
		t.Error("Binding exists after QueueUnbind")
	}
}

func Test_QueueUnbind_FailedExchangeNotExists(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	ch.QueueBind(t.Name(), "key", "testEx", false, emptyTable)
	if err := ch.QueueUnbind(t.Name(), "key", "test_Ex", emptyTable); err == nil {
		t.Error("Expected: exchange does not exists")
	}
}

func Test_QueueUnbind_FailedQueueNotExists(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	ch.QueueBind(t.Name(), "key", "testEx", false, emptyTable)
	if err := ch.QueueUnbind("test_Qu", "key", "testEx", emptyTable); err == nil {
		t.Error("Expected: queue does not exists")
	}
}

func Test_QueueUnbind_Failed_OnExclusiveQueue(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	chEx, _ := sc.clientEx.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare(t.Name(), false, false, true, false, emptyTable)

	if err := chEx.QueueUnbind(t.Name(), "key", "testEx", emptyTable); err == nil {
		t.Error("Expected: queue is locked error")
	}
}

func Test_QueuePurge_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	time.Sleep(5 * time.Millisecond)
	length := sc.server.getVhost("/").GetQueue(t.Name()).Length()
	if length != uint64(msgCount) {
		t.Errorf("Expected: queue.length = %d, %d given", msgCount, length)
	}

	purgedCount, err := ch.QueuePurge(t.Name(), false)
	if err != nil {
		t.Error(err)
	}

	if purgedCount != msgCount {
		t.Errorf("Expected: purgedCount = %d, %d given", msgCount, length)
	}
	length = sc.server.getVhost("/").GetQueue(t.Name()).Length()
	if length != 0 {
		t.Errorf("Queue is not empty after QueuePurge")
	}
}

func Test_QueuePurge_Failed_QueueNotExists(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare(t.Name(), false, false, true, false, emptyTable)

	if _, err := ch.QueuePurge("test_Qu", false); err == nil {
		t.Error("Expected: queue does not exists")
	}
}

func Test_QueuePurge_Failed_OnExclusiveQueue(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	chEx, _ := sc.clientEx.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare(t.Name(), false, false, true, false, emptyTable)

	if _, err := chEx.QueuePurge(t.Name(), false); err == nil {
		t.Error("Expected: queue is locked error")
	}
}

func Test_QueueDelete_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	time.Sleep(5 * time.Millisecond)
	var deletedCount, err = ch.QueueDelete(queue.Name, false, false, false)
	if err != nil {
		t.Error(err)
	}

	if deletedCount != msgCount {
		t.Errorf("Expected: deleteCount = %d, %d given", msgCount, deletedCount)
	}

	if q := sc.server.getVhost("/").GetQueue("test"); q != nil {
		t.Errorf("Queue exists after delete")
	}
}

func Test_QueueDeleteDurable_Success(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	queue, _ := ch.QueueDeclare(t.Name(), true, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	time.Sleep(5 * time.Millisecond)
	var deletedCount, err = ch.QueueDelete(queue.Name, false, false, false)
	if err != nil {
		t.Error(err)
	}

	if deletedCount != msgCount {
		t.Errorf("Expected: deleteCount = %d, %d given", msgCount, deletedCount)
	}

	if q := sc.server.getVhost("/").GetQueue("test"); q != nil {
		t.Errorf("Queue exists after delete")
	}

	storedQueues := sc.server.storage.GetVhostQueues("/")
	if len(storedQueues) != 0 {
		t.Error("Durable queue exists into storage after 'QueueDelete'")
	}

	found := false
	for _, q := range storedQueues {
		if q.GetName() == "test" {
			found = true
		}
	}

	if found {
		t.Error("Durable queue exists into storage after 'QueueDelete'")
	}
}

func Test_QueueDelete_Failed_NotEmpty(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	queue, _ := ch.QueueDeclare(t.Name(), false, false, false, false, emptyTable)

	msgCount := 10
	for i := 0; i < msgCount; i++ {
		ch.Publish("", queue.Name, false, false, amqp.Publishing{ContentType: "text/plain", Body: []byte("test")})
	}

	time.Sleep(5 * time.Millisecond)
	var _, err = ch.QueueDelete("test", false, true, false)
	if err == nil {
		t.Error("Expected: queue has messages error")
	}
}

func Test_QueueDelete_Failed_QueueNotExists(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare(t.Name(), false, false, true, false, emptyTable)

	if _, err := ch.QueueDelete("test_Qu", false, false, false); err == nil {
		t.Error("Expected: queue does not exists")
	}
}

func Test_QueueDelete_Failed_OnExclusiveQueue(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()
	chEx, _ := sc.clientEx.Channel()

	ch.ExchangeDeclare("testEx", "direct", false, false, false, false, emptyTable)
	ch.QueueDeclare(t.Name(), false, false, true, false, emptyTable)

	if _, err := chEx.QueueDelete(t.Name(), false, false, false); err == nil {
		t.Error("Expected: queue is locked error")
	}
}

func Test_Basic_AutoDelete(t *testing.T) {
	sc, _ := getNewSC(getDefaultTestConfig())
	defer sc.clean()
	ch, _ := sc.client.Channel()

	ch.QueueDeclare(t.Name(), false, true, false, false, emptyTable)

	ch.Consume(t.Name(), "tag1", false, false, false, false, emptyTable)
	ch.Consume(t.Name(), "tag2", false, false, false, false, emptyTable)

	ch.Cancel("tag2", false)

	queues := sc.server.GetVhost("/").GetQueues()
	time.Sleep(50 * time.Millisecond)
	if len(queues) == 0 {
		t.Error("Expected non-empty queues")
	}

	ch.Cancel("tag1", false)

	queues = sc.server.GetVhost("/").GetQueues()
	time.Sleep(time.Second)
	if len(queues) != 0 {
		t.Error("Expected empty queues")
	}
}
