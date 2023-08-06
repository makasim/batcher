package batcher_test

import (
	"testing"
	"time"

	"github.com/makasim/batcher"
)

func BenchmarkAsyncBatcher_BatchSizeOne(b *testing.B) {

	bb := batcher.NewAsync[int](1, time.Millisecond*100, func(items []int) {
	})

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = bb.Batch(1)
		}
	})
}

func BenchmarkAsyncBatcher_BatchSizeTwo(b *testing.B) {
	bb := batcher.NewAsync[int](2, time.Millisecond*100, func(items []int) {

	})

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = bb.Batch(1)
		}
	})
}

func BenchmarkAsyncBatcher_BatchSizeThree(b *testing.B) {
	bb := batcher.NewAsync[int](3, time.Millisecond*100, func(items []int) {

	})

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = bb.Batch(1)
		}
	})
}

func BenchmarkAsyncBatcher_BatchSizeFive(b *testing.B) {
	bb := batcher.NewAsync[int](5, time.Millisecond*100, func(items []int) {

	})

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = bb.Batch(1)
		}
	})
}

func BenchmarkAsyncBatcher_BatchSizeTen(b *testing.B) {
	bb := batcher.NewAsync[int](10, time.Millisecond*100, func(items []int) {

	})

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = bb.Batch(1)
		}
	})
}

func BenchmarkAsyncBatcher_BatchSizeTwenty(b *testing.B) {
	bb := batcher.NewAsync[int](20, time.Millisecond*100, func(items []int) {

	})

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = bb.Batch(1)
		}
	})
}

func BenchmarkAsyncBatcher_BatchSizeThirty(b *testing.B) {
	bb := batcher.NewAsync[int](30, time.Millisecond*100, func(items []int) {

	})

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = bb.Batch(1)
		}
	})
}
