package batcher_test

import (
	"context"
	"testing"
	"time"

	"github.com/makasim/batcher"
)

//go:noinline
func batch(i int) []int {
	return []int{i}
}

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

// Sync

func BenchmarkSyncBatcher_BatchSizeOne(b *testing.B) {
	bb := batcher.NewSync[int](1, time.Millisecond*100)
	defer bb.Shutdown(context.Background())

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkSyncBatcher_BatchSizeTwo(b *testing.B) {
	bb := batcher.NewSync[int](2, time.Millisecond*100)
	defer bb.Shutdown(context.Background())

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkSyncBatcher_BatchSizeThree(b *testing.B) {
	bb := batcher.NewSync[int](3, time.Millisecond*100)
	defer bb.Shutdown(context.Background())

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkSyncBatcher_BatchSizeFive(b *testing.B) {
	bb := batcher.NewSync[int](5, time.Millisecond*100)
	defer bb.Shutdown(context.Background())

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkSyncBatcher_BatchSizeTen(b *testing.B) {
	bb := batcher.NewSync[int](10, time.Millisecond*100)
	defer bb.Shutdown(context.Background())

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkSyncBatcher_BatchSizeTwenty(b *testing.B) {
	bb := batcher.NewSync[int](20, time.Millisecond*100)
	defer bb.Shutdown(context.Background())

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}

func BenchmarkSyncBatcher_BatchSizeThirty(b *testing.B) {
	bb := batcher.NewSync[int](30, time.Millisecond*100)
	defer bb.Shutdown(context.Background())

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb.Batch(1)
		}
	})
}
