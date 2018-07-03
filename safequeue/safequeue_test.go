package safequeue_test

import (
	"testing"

	"github.com/valinurovam/garagemq/safequeue"
)

const SIZE = 4096

var safeQueueTest = safequeue.NewSafeQueue(SIZE)

func BenchmarkSafeQueue_Push(b *testing.B) {
	for n := 0; n < b.N; n++ {
		safeQueueTest.Push(n)
	}
}

func BenchmarkSafeQueue_Pop(b *testing.B) {
	for n := 0; n < b.N; n++ {
		safeQueueTest.Pop()
	}
}

func TestSafeQueue(t *testing.T) {
	queue := safequeue.NewSafeQueue(SIZE)
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		queue.Push(item)
	}

	if queue.Length() != uint64(queueLength) {
		t.Fatalf("expected %d elements, have %d", queueLength, queue.Length())
	}

	for item := 0; item < queueLength; item++ {
		pop := queue.Pop()
		if item != pop {
			t.Fatalf("Pop: expected %d, actual %d", item, pop)
		}
	}

	if queue.Length() != 0 {
		t.Fatalf("expected %d elements, have %d", 0, queue.Length())
	}
}

func TestSafeQueue_HeadItem(t *testing.T) {
	queue := safequeue.NewSafeQueue(SIZE)
	queueLength := SIZE
	for item := 0; item < queueLength; item++ {
		queue.Push(item)
	}

	if h := queue.HeadItem(); h != 0 {
		t.Fatalf("expected head %v, actual %v", 0, h)
	}
}

func TestSafeQueue_DirtyLength(t *testing.T) {
	queue := safequeue.NewSafeQueue(SIZE)
	queueLength := SIZE
	for item := 0; item < queueLength; item++ {
		queue.Push(item)
	}

	if queue.Length() != queue.DirtyLength() {
		t.Fatal("Single thread DirtyLength must be equal Length")
	}
}

func TestSafeQueue_Purge(t *testing.T) {
	queue := safequeue.NewSafeQueue(SIZE)
	queueLength := SIZE
	for item := 0; item < queueLength; item++ {
		queue.Push(item)
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