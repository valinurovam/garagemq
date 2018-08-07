package queue

import (
	"testing"

	"github.com/valinurovam/garagemq/amqp"
)

const SIZE = 4096

func TestQueue(t *testing.T) {
	queue := NewQueue("test", 0, false, false, false, SIZE, nil)
	queue.Start()
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message, false)
	}

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
