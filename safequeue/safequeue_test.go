package safequeue

import (
"testing"
)

const SIZE = 4096

var safeQueueTest = NewSafeQueue(SIZE)

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
	queue := NewSafeQueue(SIZE)
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

func TestSafeQueue_PushHead(t *testing.T) {
	queue := NewSafeQueue(SIZE)
	queueLength := SIZE * 8
	for item := 0; item < queueLength; item++ {
		queue.Push(item)
		queue.PushHead(item)
	}

	if queue.Length() != uint64(queueLength * 2) {
		t.Fatalf("expected %d elements, have %d", queueLength, queue.Length())
	}

	var expected int
	for item := 0; item < queueLength * 2; item++ {
		pop := queue.Pop()
		if queueLength > item {
			expected = queueLength-item-1
		} else {
			expected = item-queueLength
		}
		if expected != pop {
			t.Fatalf("Pop: expected %d, actual %d", expected, pop)
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
		queue.Push(item)
	}

	if h := queue.HeadItem(); h != 0 {
		t.Fatalf("expected head %v, actual %v", 0, h)
	}
}

func TestSafeQueue_DirtyLength(t *testing.T) {
	queue := NewSafeQueue(SIZE)
	queueLength := SIZE
	for item := 0; item < queueLength; item++ {
		queue.Push(item)
	}

	if queue.Length() != queue.DirtyLength() {
		t.Fatal("Single thread DirtyLength must be equal Length")
	}
}

func TestSafeQueue_Purge(t *testing.T) {
	queue := NewSafeQueue(SIZE)
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
