package batcher

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type WorkerBatchFunc[Item any] func(items []Item)

type WorkerBatcher[Item any] struct {
	wbf     WorkerBatchFunc[Item]
	wg      *sync.WaitGroup
	size    int64
	count   int64
	timeout time.Duration
	batches [100]chan Item
}

func NewWorkerBatcher[Item any](size int64, timeout time.Duration, batchFunc WorkerBatchFunc[Item]) *WorkerBatcher[Item] {
	if size <= 0 {
		panic("size must be greater than zero")
	}

	wb := &WorkerBatcher[Item]{
		size:    size,
		count:   -1,
		timeout: timeout,
		wbf:     batchFunc,
		wg:      &sync.WaitGroup{},
	}

	for i := range wb.batches {
		wb.batches[i] = make(chan Item, size)
	}

	for i := 0; i < len(wb.batches); i++ {
		i := i

		wb.wg.Add(1)
		go func() {
			defer wb.wg.Done()
			wb.collect(i)
		}()
	}

	return wb
}

func (wb *WorkerBatcher[Item]) Batch(ctx context.Context, item Item) error {
	idx := atomic.AddInt64(&wb.count, 1)
	batchIdx := int64(idx/wb.size) % 100

	select {
	case wb.batches[batchIdx] <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (wb *WorkerBatcher[Item]) Shutdown() {
	for i := range wb.batches {
		close(wb.batches[i])
	}
	wb.wg.Wait()
}

func (wb *WorkerBatcher[Item]) collect(i int) {
	size := int(wb.size)
	items := make([]Item, size)

	t := time.NewTimer(wb.timeout)

	for {
		select {
		case item, ok := <-wb.batches[i]:
			if !ok {
				return
			}

			items = append(items, item)
			if len(items) >= size {
				wb.wbf(items)
				items = items[:0]
			}

			if !t.Stop() {
				t = time.NewTimer(wb.timeout)
				continue
			}

			t.Reset(wb.timeout)
		case <-t.C:
			if len(items) > 0 {
				wb.wbf(items)
				items = items[:0]
			}
		}
	}
}
