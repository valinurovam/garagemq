package safequeue

import (
	"testing"

	"github.com/valinurovam/garagemq/amqp"
)

const SIZE = 65536

func TestSafeQueue(t *testing.T) {
	queue := NewSafeQueue(SIZE)
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message)
	}

	if queue.Length() != uint64(queueLength) {
		t.Fatalf("expected %d elements, have %d", queueLength, queue.Length())
	}

	for item := 0; item < queueLength; item++ {
		pop := queue.Pop()
		if uint64(item) != pop.ID {
			t.Fatalf("Pop: expected %d, actual %d", item, pop.ID)
		}
	}

	if queue.Length() != 0 {
		t.Fatalf("expected %d elements, have %d", 0, queue.Length())
	}
}

func TestSafeQueue_PushHead(t *testing.T) {
	queue := NewSafeQueue(SIZE)
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message)
		queue.PushHead(message)
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
		if uint64(expected) != pop.ID {
			t.Fatalf("Pop: expected %d, actual %d", expected, pop.ID)
		}
	}

	if queue.Length() != 0 {
		t.Fatalf("expected %d elements, have %d", 0, queue.Length())
	}
}

func TestSafeQueue_HeadItem(t *testing.T) {
	queue := NewSafeQueue(SIZE)
	queueLength := SIZE
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message)
	}

	if h := queue.HeadItem(); h.ID != 0 {
		t.Fatalf("expected head %v, actual %v", 0, h)
	}
}

func TestSafeQueue_DirtyLength(t *testing.T) {
	queue := NewSafeQueue(SIZE)
	queueLength := SIZE
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message)
	}

	if queue.Length() != queue.DirtyLength() {
		t.Fatal("Single thread DirtyLength must be equal Length")
	}
}

func TestSafeQueue_Purge(t *testing.T) {
	queue := NewSafeQueue(SIZE)
	queueLength := SIZE
	for item := 0; item < queueLength; item++ {
		message := &amqp.Message{ID: uint64(item)}
		queue.Push(message)
	}

	queue.Purge()

	if queue.Length() != 0 {
		t.Fatalf("expected %d elements, actual %d", 0, queue.Length())
	}

	pop := queue.Pop()
	if nil != pop {
		t.Fatalf("Pop: expected %v, actual %v", nil, pop)
	}
}
