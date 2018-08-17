package qos

import "testing"

func TestAmqpQos_IsActive(t *testing.T) {
	q := NewAmqpQos(1, 10)

	if q.PrefetchSize() != 10 {
		t.Fatalf("PrefetchSize: Expected %d, actual %d", 10, q.PrefetchSize())
	}

	if q.PrefetchCount() != 1 {
		t.Fatalf("PrefetchCount: Expected %d, actual %d", 1, q.PrefetchCount())
	}

	if !q.IsActive() {
		t.Fatalf("Expected active qos")
	}

	q = NewAmqpQos(0, 0)

	if q.IsActive() {
		t.Fatalf("Expected inactive qos")
	}
}

func TestAmqpQos_Dec(t *testing.T) {
	q := NewAmqpQos(5, 10)
	q.Dec(1, 1)

	if q.currentCount != 0 {
		t.Fatalf("Dec: Expected currentCount %d, actual %d", 0, q.currentCount)
	}

	if q.currentSize != 0 {
		t.Fatalf("Dec: Expected currentSize %d, actual %d", 0, q.currentCount)
	}

	q.Inc(4, 8)
	q.Dec(1, 1)

	if q.currentCount != 3 {
		t.Fatalf("Dec: Expected currentCount %d, actual %d", 3, q.currentCount)
	}

	if q.currentSize != 7 {
		t.Fatalf("Dec: Expected currentSize %d, actual %d", 7, q.currentCount)
	}
}

func TestAmqpQos_Inc(t *testing.T) {
	q := NewAmqpQos(5, 10)
	res := q.Inc(1, 1)

	if !res {
		t.Fatalf("Inc: Expected successful inc")
	}
	if q.currentCount != 1 {
		t.Fatalf("Inc: Expected currentCount %d, actual %d", 1, q.currentCount)
	}

	if q.currentSize != 1 {
		t.Fatalf("Inc: Expected currentSize %d, actual %d", 1, q.currentCount)
	}

	q = NewAmqpQos(5, 10)
	if q.Inc(6, 1) {
		t.Fatalf("Inc: Expected failed inc")
	}
}

func TestAmqpQos_Update(t *testing.T) {
	q := NewAmqpQos(5, 10)
	q.Update(10, 20)

	if q.prefetchCount != 10 {
		t.Fatalf("Update: Expected prefetchCount %d, actual %d", 10, q.prefetchCount)
	}

	if q.prefetchSize != 20 {
		t.Fatalf("Update: Expected prefetchSize %d, actual %d", 20, q.prefetchSize)
	}
}

func TestAmqpQos_Release(t *testing.T) {
	q := NewAmqpQos(5, 10)
	q.Inc(1, 1)
	q.Release()

	if q.currentCount != 0 {
		t.Fatalf("Release: Expected currentCount %d, actual %d", 0, q.currentCount)
	}

	if q.currentSize != 0 {
		t.Fatalf("Release: Expected currentSize %d, actual %d", 0, q.currentCount)
	}
}
