package queue

import (
	"bytes"
	"testing"

	"github.com/valinurovam/garagemq/amqp"
	"github.com/valinurovam/garagemq/qos"
)

const SIZE = 4096

func TestQueue_Property(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, SIZE, nil)
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

	queue.Start()

	if queue.IsActive() != true {
		t.Fatalf("Expected IsActive %t, actual %t", true, queue.IsActive())
	}
}

func TestQueue_PushPop(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, SIZE, nil)
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message, false)
	}

	if queue.Pop() != nil {
		t.Fatal("Expected nil from non-active queue")
	}

	queue.Start()

	if queue.Length() != uint64(queueLength) {
		t.Fatalf("expected %d elements, have %d", queueLength, queue.Length())
	}

	for item := 0; item < queueLength; item++ {
		pop := queue.Pop()
		message := &amqp.Message{ID: uint64(item)}
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
	queue := NewQueue("test", 0, false, false, false, SIZE, nil)
	queue.Start()
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message, false)
		queue.Requeue(message)
	}

	if queue.Length() != uint64(queueLength*2) {
		t.Fatalf("expected %d elements, have %d", queueLength, queue.Length())
	}

	var expected int
	for item := 0; item < queueLength*2; item++ {
		pop := queue.Pop()
		if queueLength > item {
			expected = queueLength - item - 1
		} else {
			expected = item - queueLength
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
	queue := NewQueue("test", 0, false, false, false, SIZE, nil)
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message, false)
	}
	queue.Start()

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

	queue := NewQueue("test", 0, false, false, false, SIZE, nil)
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message, false)
	}

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

	queue := NewQueue("test", 0, false, false, false, SIZE, nil)
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message, false)
	}

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
	queue := NewQueue("test", 0, false, false, false, SIZE, nil)
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message, false)
	}

	if cnt := queue.Purge(); int(cnt) != queueLength {
		t.Fatalf("Expected %d purged messages, actual %d", queueLength, cnt)
	}

	if queue.Length() != 0 {
		t.Fatalf("expected %d elements, have %d", 0, queue.Length())
	}
}

func TestQueue_AddConsumer(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, SIZE, nil)
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
	queue := NewQueue("test", 0, false, false, false, SIZE, nil)
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
	queue1 := NewQueue("test", 0, false, false, false, SIZE, nil)
	queue2 := NewQueue("test", 0, false, false, false, SIZE, nil)

	if err := queue1.EqualWithErr(queue2); err != nil {
		t.Fatal(err)
	}
}

func TestQueue_EqualWithErr_Failed_Durable(t *testing.T) {
	queue1 := NewQueue("test", 0, false, false, false, SIZE, nil)
	queue2 := NewQueue("test", 0, false, false, true, SIZE, nil)

	if err := queue1.EqualWithErr(queue2); err == nil {
		t.Fatal("Expected error about durable")
	}
}

func TestQueue_EqualWithErr_Failed_Autodelete(t *testing.T) {
	queue1 := NewQueue("test", 0, false, false, false, SIZE, nil)
	queue2 := NewQueue("test", 0, false, true, false, SIZE, nil)

	if err := queue1.EqualWithErr(queue2); err == nil {
		t.Fatal("Expected error about autodelete")
	}
}

func TestQueue_EqualWithErr_Failed_Exclusive(t *testing.T) {
	queue1 := NewQueue("test", 0, false, false, false, SIZE, nil)
	queue2 := NewQueue("test", 0, true, false, false, SIZE, nil)

	if err := queue1.EqualWithErr(queue2); err == nil {
		t.Fatal("Expected error about exclusive")
	}
}

func TestQueue_Delete_Success(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, SIZE, nil)
	if _, err := queue.Delete(false, false); err != nil {
		t.Fatal(err)
	}
}

func TestQueue_Delete_Failed_IfEmpty(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, SIZE, nil)
	if _, err := queue.Delete(false, true); err != nil {
		t.Fatal(err)
	}
	message := &amqp.Message{}
	queue.Push(message, false)
	if _, err := queue.Delete(false, true); err == nil {
		t.Fatal("Expected error on non empty queue")
	}
}

func TestQueue_Delete_Failed_IfUnused(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, SIZE, nil)
	message := &amqp.Message{}
	queue.Push(message, false)
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
	queue := NewQueue("test", 0, false, false, false, SIZE, nil)
	marshaled := queue.Marshal(amqp.ProtoRabbit)
	if !bytes.Equal(marshaled, []byte("test")) {
		t.Fatal("Error on marshal queue")
	}

	if queue.Unmarshal(marshaled, amqp.ProtoRabbit) != "test" {
		t.Fatal("Error on unmarshal queue")
	}
}

func TestQueue_Stop(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, SIZE, nil)
	queue.Start()

	if !queue.IsActive() {
		t.Fatal("Queue is not active after start")
	}

	queue.Stop()

	if queue.IsActive() {
		t.Fatal("Queue is still active after stop")
	}
}
