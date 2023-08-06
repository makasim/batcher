package batcher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var ErrBufferFull = errors.New("buffer is full")

type AsyncBatchFunc[Item any] func(items []Item)

type AsyncBatcher[Item any] struct {
	bf      AsyncBatchFunc[Item]
	wg      *sync.WaitGroup
	size    int64
	count   int64
	timeout time.Duration
	batches [100]chan Item
}

func NewAsync[Item any](size int64, timeout time.Duration, batchFunc AsyncBatchFunc[Item]) *AsyncBatcher[Item] {
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

		b.batches[i] = make(chan Item, size)

		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			collect(b.batches[i], size, timeout, batchFunc)
		}()
	}

	return b
}

func (b *AsyncBatcher[Item]) Batch(item Item) error {
	idx := atomic.AddInt64(&b.count, 1)
	batchIdx := (idx / b.size) % 100

	select {
	case b.batches[batchIdx] <- item:
		return nil
	default:
		return ErrBufferFull
	}
}

func (b *AsyncBatcher[Item]) Shutdown(ctx context.Context) error {
	for i := range b.batches {
		close(b.batches[i])
	}

	stoppedCh := make(chan struct{})

	go func() {
		defer close(stoppedCh)
		b.wg.Wait()
	}()

	select {
	case <-stoppedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func collect[Item any](batchCh <-chan Item, size int64, timeout time.Duration, wbf AsyncBatchFunc[Item]) {
	items := make([]Item, 0, size)

	leftSize := size

	t := acquireTimer(timeout)
	t.Stop()

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

			releaseTimer(t)
			t = acquireTimer(timeout)

			items = append(items, item)
			leftSize--

			if leftSize == 0 {
				t.Stop()

				wbf(items)
				items = items[:0]
				leftSize = size
			}
		case <-t.C:
			releaseTimer(t)
			t = acquireTimer(timeout)
			t.Stop()

			if len(items) > 0 {
				wbf(items)
				items = items[:0]
				// leftSize is unchanged
			}
		}
	}
}
