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

	wb := &AsyncBatcher[Item]{
		size:  size,
		count: -1,
		wg:    &sync.WaitGroup{},
	}

	for i := 0; i < len(wb.batches); i++ {
		i := i

		wb.batches[i] = make(chan Item, size*2)

		wb.wg.Add(1)
		go func() {
			defer wb.wg.Done()
			collect(wb.batches[i], size, timeout, batchFunc)
		}()
	}

	return wb
}

func (wb *AsyncBatcher[Item]) Batch(ctx context.Context, item Item) error {
	idx := atomic.AddInt64(&wb.count, 1)
	batchIdx := (idx / wb.size) % 100

	select {
	case wb.batches[batchIdx] <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (wb *AsyncBatcher[Item]) Shutdown() {
	for i := range wb.batches {
		close(wb.batches[i])
	}
	wb.wg.Wait()
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
