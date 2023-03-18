package batcher_test

import (
	"context"
	"testing"
	"time"

	"github.com/makasim/batcher"
)

func BenchmarkWorkerBatcher_BatchSizeOne(b *testing.B) {
	bb := batcher.NewWorkerBatcher[int](1, time.Millisecond*100, func(items []int) {

	})

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := bb.Batch(context.Background(), 1); err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkWorkerBatcher_BatchSizeTwo(b *testing.B) {
	bb := batcher.NewWorkerBatcher[int](2, time.Millisecond*100, func(items []int) {

	})

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := bb.Batch(context.Background(), 1); err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkWorkerBatcher_BatchSizeThree(b *testing.B) {
	bb := batcher.NewWorkerBatcher[int](3, time.Millisecond*100, func(items []int) {

	})

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := bb.Batch(context.Background(), 1); err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkWorkerBatcher_BatchSizeFive(b *testing.B) {
	bb := batcher.NewWorkerBatcher[int](5, time.Millisecond*100, func(items []int) {

	})

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := bb.Batch(context.Background(), 1); err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkWorkerBatcher_BatchSizeTen(b *testing.B) {
	bb := batcher.NewWorkerBatcher[int](10, time.Millisecond*100, func(items []int) {

	})

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := bb.Batch(context.Background(), 1); err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkWorkerBatcher_BatchSizeTwenty(b *testing.B) {
	bb := batcher.NewWorkerBatcher[int](20, time.Millisecond*100, func(items []int) {

	})

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := bb.Batch(context.Background(), 1); err != nil {
				panic(err)
			}
		}
	})
}

func BenchmarkWorkerBatcher_BatchSizeThirty(b *testing.B) {
	bb := batcher.NewWorkerBatcher[int](30, time.Millisecond*100, func(items []int) {

	})

	b.SetParallelism(10)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := bb.Batch(context.Background(), 1); err != nil {
				panic(err)
			}
		}
	})
}
