package batcher_test

import (
	"testing"
	"time"

	"github.com/makasim/batcher"
)

func Benchmark_NoBatch(b *testing.B) {
	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if batch := batch(1); len(batch) == 0 {
				panic("empty batch")
			}
		}
	})
}

func BenchmarkBatcher_BatchSizeOne(b *testing.B) {
	bb := batcher.New[int](1, time.Millisecond*100)

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkBatcher_BatchSizeTwo(b *testing.B) {
	bb := batcher.New[int](2, time.Millisecond*100)

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkBatcher_BatchSizeThree(b *testing.B) {
	bb := batcher.New[int](3, time.Millisecond*100)

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkBatcher_BatchSizeFive(b *testing.B) {
	bb := batcher.New[int](5, time.Millisecond*100)

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkBatcher_BatchSizeTen(b *testing.B) {
	bb := batcher.New[int](10, time.Millisecond*100)

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkBatcher_BatchSizeTwenty(b *testing.B) {
	bb := batcher.New[int](20, time.Millisecond*100)

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkBatcher_BatchSizeThirty(b *testing.B) {
	bb := batcher.New[int](30, time.Millisecond*100)

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

//go:noinline
func batch(i int) []int {
	return []int{i}
}
