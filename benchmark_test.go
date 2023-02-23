package batcher_test

import (
	"testing"
	"time"

	"github.com/makasim/batcher"
)

func BenchmarkNoBatch(b *testing.B) {
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

func BenchmarkBatchSizeOne(b *testing.B) {
	bb := batcher.New[int](1, time.Millisecond*100)

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkBatchSizeTwo(b *testing.B) {
	bb := batcher.New[int](2, time.Millisecond*100)

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkBatchSizeThree(b *testing.B) {
	bb := batcher.New[int](3, time.Millisecond*100)

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkBatchSizeFive(b *testing.B) {
	bb := batcher.New[int](5, time.Millisecond*100)

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkBatchSizeTen(b *testing.B) {
	bb := batcher.New[int](10, time.Millisecond*100)

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkBatchSizeTwenty(b *testing.B) {
	bb := batcher.New[int](20, time.Millisecond*100)

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkBatchSizeThirty(b *testing.B) {
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
