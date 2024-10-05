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

type asyncBatchItems[Item any] struct {
	items  []Item
	mux    sync.Mutex
	timer  *time.Timer
	fullCh chan struct{}
}

type AsyncBatcher[Item any] struct {
	bf      AsyncBatchFunc[Item]
	wg      *sync.WaitGroup
	size    int64
	count   int64
	timeout time.Duration
	batches [100]*asyncBatchItems[Item]
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

		b.batches[i] = &asyncBatchItems[Item]{
			items:  make([]Item, 0, size),
			mux:    sync.Mutex{},
			timer:  acquireTimer(timeout),
			fullCh: make(chan struct{}, 1),
		}

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

	bi := b.batches[batchIdx]

	bi.mux.Lock()
	defer bi.mux.Unlock()

	if len(bi.items) >= cap(bi.items) {
		return ErrBufferFull
	}

	bi.items = append(bi.items, item)

	if len(bi.items) == cap(bi.items) {
		select {
		case bi.fullCh <- struct{}{}:
		default:
			return ErrBufferFull
		}
	}

	return nil
}

func (b *AsyncBatcher[Item]) Shutdown(ctx context.Context) error {
	for i := range b.batches {
		close(b.batches[i].fullCh)
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

func collect[Item any](bi *asyncBatchItems[Item], size int64, timeout time.Duration, wbf AsyncBatchFunc[Item]) {
	items := make([]Item, 0, size)
	var exit bool

	for {
		if exit {
			return
		}

		select {
		case <-bi.timer.C:
		case _, ok := <-bi.fullCh:
			if !ok {
				exit = true
			}
		}

		bi.mux.Lock()
		if len(bi.items) > 0 {
			items = items[0:len(bi.items)]
			copy(items, bi.items)
			bi.items = bi.items[:0]
		}
		releaseTimer(bi.timer)
		bi.timer = acquireTimer(timeout)
		bi.mux.Unlock()

		if len(items) > 0 {
			wbf(items)
			items = items[:0]
		}
	}
}
