package safequeue

import (
	"testing"
	"runtime"
	"fmt"
)

var queue = NewSafeQueue(4096)

func BenchmarkSafeQueue_Push(b *testing.B) {
	for n := 0; n < b.N; n++ {
		queue.Push(n)
	}
}

func BenchmarkSafeQueue_Pop(b *testing.B) {
	for n := 0; n < b.N; n++ {
		queue.Pop()
	}
}

//func BenchmarkList(b *testing.B) {
//	var l = list.List{}
//	for n := 0; n < b.N; n++ {
//		l.PushBack(n)
//	}
//}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}