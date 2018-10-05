package queue

import (
	"testing"

	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/config"
	"github.com/valinurovam/garagemq/qos"
)

const SIZE = 32

var baseConfig = config.Queue{ShardSize: SIZE, MaxMessagesInRam: 10000}

func TestQueue_Property(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	if queue.GetName() != "test" {
		t.Fatalf("Expected GetName %s, actual %s", "test", queue.GetName())
	}

	if queue.IsDurable() != false {
		t.Fatalf("Expected IsDurable %t, actual %t", false, queue.IsDurable())
	}

	if queue.IsExclusive() != false {
		t.Fatalf("Expected IsExclusive %t, actual %t", false, queue.IsExclusive())
	}

	if queue.ConnID() != 0 {
		t.Fatalf("Expected ConnID %d, actual %d", 0, queue.ConnID())
	}

	if queue.IsActive() != false {
		t.Fatalf("Expected IsActive %t, actual %t", false, queue.IsActive())
	}

	if queue.IsAutoDelete() != false {
		t.Fatalf("Expected IsAutoDelete %t, actual %t", false, queue.IsAutoDelete())
	}

	queue.Start()

	if queue.IsActive() != true {
		t.Fatalf("Expected IsActive %t, actual %t", true, queue.IsActive())
	}
}

func TestQueue_PushPop(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	queue.Start()
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item + 1)}
		queue.Push(message)
	}

	queue.Stop()
	if queue.Pop() != nil {
		t.Fatal("Expected nil from non-active queue")
	}
	queue.Start()

	if queue.Length() != uint64(queueLength) {
		t.Fatalf("expected %d elements, have %d", queueLength, queue.Length())
	}

	for item := 0; item < queueLength; item++ {
		pop := queue.Pop()
		message := &amqp.Message{ID: uint64(item + 1)}
		if message.ID != pop.ID {
			t.Fatalf("Pop: expected %v, actual %v", item, pop)
		}
	}

	if queue.Length() != 0 {
		t.Fatalf("expected %d elements, have %d", 0, queue.Length())
	}

	if queue.Pop() != nil {
		t.Fatal("Expected nil on empty queue")
	}
}

func TestQueue_Requeue(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	queue.Start()
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item + 1)}
		queue.Push(message)
		queue.Requeue(message)
	}

	if queue.Length() != uint64(queueLength*2) {
		t.Fatalf("expected %d elements, have %d", queueLength, queue.Length())
	}

	var expected int
	for item := 0; item < queueLength*2; item++ {
		pop := queue.Pop()
		if queueLength > item {
			expected = queueLength - item
		} else {
			expected = item + 1 - queueLength
		}
		if expected != int(pop.ID) {
			t.Fatalf("Pop: expected %d, actual %d", expected, pop.ID)
		}
	}

	if queue.Length() != 0 {
		t.Fatalf("expected %d elements, have %d", 0, queue.Length())
	}
}

func TestQueue_PopQos_Empty(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	queue.Start()
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message)
	}

	rcvCount := 0
	for item := 0; item < queueLength; item++ {
		message := queue.PopQos([]*qos.AmqpQos{})
		if message != nil {
			rcvCount++
		}
	}

	if rcvCount != queueLength {
		t.Fatalf("Expected %d messages, actual %d", queueLength, rcvCount)
	}
}

func TestQueue_PopQos_Single(t *testing.T) {
	prefetchCount := 10
	qosRule := qos.NewAmqpQos(uint16(prefetchCount), 0)

	queue := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	queue.Start()

	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message)
	}

	queue.Stop()
	if queue.PopQos([]*qos.AmqpQos{qosRule}) != nil {
		t.Fatal("Expected nil from non-active queue")
	}
	queue.Start()

	rcvCount := 0
	for item := 0; item < queueLength; item++ {
		message := queue.PopQos([]*qos.AmqpQos{qosRule})
		if message != nil {
			rcvCount++
		}
	}

	if rcvCount != prefetchCount {
		t.Fatalf("Expected %d messages, actual %d", prefetchCount, rcvCount)
	}
}

func TestQueue_PopQos_Multiple(t *testing.T) {
	prefetchCount := 28
	qosRules := []*qos.AmqpQos{
		qos.NewAmqpQos(0, 0),
		qos.NewAmqpQos(uint16(prefetchCount), 0),
		qos.NewAmqpQos(uint16(prefetchCount*2), 0),
	}

	queue := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	queue.Start()
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message)
	}

	queue.Stop()
	if queue.PopQos(qosRules) != nil {
		t.Fatal("Expected nil from non-active queue")
	}
	queue.Start()

	rcvCount := 0
	for item := 0; item < queueLength; item++ {
		message := queue.PopQos(qosRules)
		if message != nil {
			rcvCount++
		}
	}

	if rcvCount != prefetchCount {
		t.Fatalf("Expected %d messages, actual %d", prefetchCount, rcvCount)
	}
}

func TestQueue_Purge(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	queue.Start()
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message)
	}

	if cnt := queue.Purge(); int(cnt) != queueLength {
		t.Fatalf("Expected %d purged messages, actual %d", queueLength, cnt)
	}

	if queue.Length() != 0 {
		t.Fatalf("expected %d elements, have %d", 0, queue.Length())
	}
}

func TestQueue_AddConsumer(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	if queue.AddConsumer(&ConsumerMock{}, false) == nil {
		t.Fatalf("Expected error on non-active queue")
	}
	queue.Start()

	if err := queue.AddConsumer(&ConsumerMock{}, false); err != nil {
		t.Fatal(err)
	}

	if queue.wasConsumed == false {
		t.Fatalf("Expected wasConsumed true")
	}

	if queue.ConsumersCount() != 1 {
		t.Fatalf("Expected %d consumers, actual %d", 1, queue.ConsumersCount())
	}

	if queue.AddConsumer(&ConsumerMock{}, true) == nil {
		t.Fatalf("Expected error, queue is busy")
	}
}

func TestQueue_RemoveConsumer(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	queue.Start()

	queue.AddConsumer(&ConsumerMock{tag: "test"}, false)

	queue.RemoveConsumer("bad-tag")

	if queue.ConsumersCount() != 1 {
		t.Fatalf("Expected %d consumers, actual %d", 1, queue.ConsumersCount())
	}

	queue.RemoveConsumer("test")

	if queue.ConsumersCount() != 0 {
		t.Fatalf("Expected %d consumers, actual %d", 0, queue.ConsumersCount())
	}
}

func TestQueue_EqualWithErr_Success(t *testing.T) {
	queue1 := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	queue2 := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)

	if err := queue1.EqualWithErr(queue2); err != nil {
		t.Fatal(err)
	}
}

func TestQueue_EqualWithErr_Failed_Durable(t *testing.T) {
	queue1 := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	queue2 := NewQueue("test", 0, false, false, true, baseConfig, nil, nil)

	if err := queue1.EqualWithErr(queue2); err == nil {
		t.Fatal("Expected error about durable")
	}
}

func TestQueue_EqualWithErr_Failed_Autodelete(t *testing.T) {
	queue1 := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	queue2 := NewQueue("test", 0, false, true, false, baseConfig, nil, nil)

	if err := queue1.EqualWithErr(queue2); err == nil {
		t.Fatal("Expected error about autodelete")
	}
}

func TestQueue_EqualWithErr_Failed_Exclusive(t *testing.T) {
	queue1 := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	queue2 := NewQueue("test", 0, true, false, false, baseConfig, nil, nil)

	if err := queue1.EqualWithErr(queue2); err == nil {
		t.Fatal("Expected error about exclusive")
	}
}

func TestQueue_Delete_Success(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	if _, err := queue.Delete(false, false); err != nil {
		t.Fatal(err)
	}
}

func TestQueue_Delete_Failed_IfEmpty(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	queue.Start()
	if _, err := queue.Delete(false, true); err != nil {
		t.Fatal(err)
	}

	// after previous delete queue will be stopped
	queue.Start()
	message := &amqp.Message{}
	queue.Push(message)
	if _, err := queue.Delete(false, true); err == nil {
		t.Fatal("Expected error on non empty queue")
	}
}

func TestQueue_Delete_Failed_IfUnused(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	message := &amqp.Message{}
	queue.Push(message)
	if _, err := queue.Delete(true, false); err != nil {
		t.Fatal(err)
	}
	queue.Start()
	queue.AddConsumer(&ConsumerMock{tag: "test"}, false)
	if _, err := queue.Delete(true, false); err == nil {
		t.Fatal("Expected error on consumed queue")
	}
}

func TestQueue_Marshal(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	marshaled, err := queue.Marshal(amqp.ProtoRabbit)
	if err != nil {
		t.Fatal(err)
	}

	uQueue := &Queue{}
	if err = uQueue.Unmarshal(marshaled, amqp.ProtoRabbit); err != nil || uQueue.name != "test" {
		t.Fatal("Error on unmarshal queue")
	}
}

// useless, for coverage only
func TestQueue_Unmarshal_FailedEmpty(t *testing.T) {
	queue := &Queue{}
	if queue.Unmarshal([]byte{}, amqp.ProtoRabbit) == nil {
		t.Fatal("Expected unmarshal error")
	}
}

// useless, for coverage only
func TestQueue_Unmarshal_FailedNameOnly(t *testing.T) {
	queue := &Queue{}
	if queue.Unmarshal([]byte{4, 't', 'e', 's', 't'}, amqp.ProtoRabbit) == nil {
		t.Fatal("Expected unmarshal error")
	}
}

func TestQueue_Stop(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	queue.Start()

	if !queue.IsActive() {
		t.Fatal("Queue is not active after start")
	}

	queue.Stop()

	if queue.IsActive() {
		t.Fatal("Queue is still active after stop")
	}
}

func TestQueue_Push_Durable_Persistent(t *testing.T) {
	storage := &MsgStorageMock{}
	queue := NewQueue("test", 0, false, false, true, baseConfig, storage, nil)
	queue.Start()
	var dMode byte = 2
	message := &amqp.Message{
		ID: 1,
		Header: &amqp.ContentHeader{
			PropertyList: &amqp.BasicPropertyList{
				DeliveryMode: &dMode,
			},
		},
	}

	queue.Push(message)
	if !storage.add {
		t.Fatal("Storage.Add not called message push")
	}
}

func TestQueue_Push_Durable_NonPersistent(t *testing.T) {
	storage := &MsgStorageMock{}
	queue := NewQueue("test", 0, false, false, true, baseConfig, storage, nil)
	var dMode byte = 1
	message := &amqp.Message{
		ID: 1,
		Header: &amqp.ContentHeader{
			PropertyList: &amqp.BasicPropertyList{
				DeliveryMode: &dMode,
			},
		},
	}
	queue.Push(message)
	if storage.add {
		t.Fatal("Storage.Add called on non persistent message")
	}

	queue.Push(message)
	if storage.add {
		t.Fatal("Storage.Add called on non persistent message")
	}
}

func TestQueue_AckMsg_Persistent(t *testing.T) {
	storage := &MsgStorageMock{}
	queue := NewQueue("test", 0, false, false, true, baseConfig, storage, nil)
	queue.Start()
	var dMode byte = 2
	message := &amqp.Message{
		ID: 1,
		Header: &amqp.ContentHeader{
			PropertyList: &amqp.BasicPropertyList{
				DeliveryMode: &dMode,
			},
		},
	}

	queue.AckMsg(message)
	if !storage.del {
		t.Fatal("Storage.Del not called on ACK persistent message")
	}
}

func TestQueue_AckMsg_NonPersistent(t *testing.T) {
	storage := &MsgStorageMock{}
	queue := NewQueue("test", 0, false, false, true, baseConfig, storage, nil)
	var dMode byte = 1
	message := &amqp.Message{
		ID: 1,
		Header: &amqp.ContentHeader{
			PropertyList: &amqp.BasicPropertyList{
				DeliveryMode: &dMode,
			},
		},
	}

	queue.AckMsg(message)
	if storage.del {
		t.Fatal("Storage.Del called on ACK non-persistent message")
	}
}

func TestQueue_Purge_Durable(t *testing.T) {
	storage := &MsgStorageMock{}
	queue := NewQueue("test", 0, false, false, true, baseConfig, storage, nil)
	queue.Purge()

	if !storage.purged {
		t.Fatal("Storage.Purge not called on purge durable queue")
	}
}

func TestQueue_Delete_Durable(t *testing.T) {
	storage := &MsgStorageMock{}
	queue := NewQueue("test", 0, false, false, true, baseConfig, storage, nil)
	queue.Delete(false, false)

	if !storage.purged {
		t.Fatal("Storage.Purge not called on delete durable queue")
	}
}

func TestQueue_Requeue_Durable(t *testing.T) {
	storage := &MsgStorageMock{}
	queue := NewQueue("test", 0, false, false, true, baseConfig, storage, nil)
	queue.Start()

	initDeliveryCount := 1
	var dMode byte = 2
	message := &amqp.Message{
		DeliveryCount: uint32(initDeliveryCount),
		ID:            1,
		Header: &amqp.ContentHeader{
			PropertyList: &amqp.BasicPropertyList{
				DeliveryMode: &dMode,
			},
		},
	}

	queue.Requeue(message)
	if message.DeliveryCount != uint32(initDeliveryCount+1) {
		t.Fatal("Delivery count not not incremented")
	}

	if !storage.update {
		t.Fatal("Storage.Update not called on requeue persistent message")
	}
}

// useless, for coverage only
func TestQueue_SetMetrics(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, baseConfig, nil, nil)
	queue.SetMetrics(nil)
	if queue.GetMetrics() != nil {
		t.Fatal("Expected nil metrics")
	}
}
