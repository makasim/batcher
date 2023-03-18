package batcher

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type AsyncBatchFunc[Item any] func(items []Item)

type AsyncBatcher[Item any] struct {
	bf      AsyncBatchFunc[Item]
	wg      *sync.WaitGroup
	size    int64
	count   int64
	timeout time.Duration
	batches [100]chan Item
}

func NewWorkerBatcher[Item any](size int64, timeout time.Duration, batchFunc AsyncBatchFunc[Item]) *AsyncBatcher[Item] {
	if size <= 0 {
		panic("size must be greater than zero")
	}

	b := &AsyncBatcher[Item]{
		size:  size,
		count: -1,
		wg:    &sync.WaitGroup{},
	}

	for i := 0; i < len(b.batches); i++ {
		i := i

		b.batches[i] = make(chan Item, size*2)

		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			collect(b.batches[i], size, timeout, batchFunc)
		}()
	}

	return b
}

func (b *AsyncBatcher[Item]) Batch(ctx context.Context, item Item) error {
	idx := atomic.AddInt64(&b.count, 1)
	batchIdx := (idx / b.size) % 100

	select {
	case b.batches[batchIdx] <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *AsyncBatcher[Item]) Shutdown() {
	for i := range b.batches {
		close(b.batches[i])
	}
	b.wg.Wait()
}

func collect[Item any](batchCh <-chan Item, size int64, timeout time.Duration, wbf AsyncBatchFunc[Item]) {
	items := make([]Item, size)

	t := time.NewTimer(timeout)

	for {
		select {
		case item, ok := <-batchCh:
			if !ok {
				if len(items) > 0 {
					wbf(items)
					items = items[:0]
				}

				return
			}

			items = append(items, item)
			if len(items) >= int(size) {
				wbf(items)
				items = items[:0]
			}

			if !t.Stop() {
				t = time.NewTimer(timeout)
				continue
			}

			t.Reset(timeout)
		case <-t.C:
			if len(items) > 0 {
				wbf(items)
				items = items[:0]
			}
		}
	}
}
