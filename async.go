package batcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var ErrBufferFull = errors.New("buffer is full")

type AsyncBatchFunc[Item any] func(items []Item)

type asyncBatchItems[Item any] struct {
	items  []Item
	mux    sync.Mutex
	fullCh chan struct{}
}

type AsyncBatcher[Item any] struct {
	bf      AsyncBatchFunc[Item]
	wg      *sync.WaitGroup
	size    int64
	count   int64
	timeout time.Duration
	buckets []*asyncBatchItems[Item]
}

func NewAsync[Item any](batch int64, buckets int, timeout time.Duration, batchFunc AsyncBatchFunc[Item]) *AsyncBatcher[Item] {
	if batch <= 0 {
		panic("batch must be greater than zero")
	}
	if buckets <= 0 {
		panic("buckets must be greater than zero")
	}

	b := &AsyncBatcher[Item]{
		size:    batch,
		count:   -1,
		wg:      &sync.WaitGroup{},
		buckets: make([]*asyncBatchItems[Item], buckets),
	}

	for i := 0; i < len(b.buckets); i++ {
		i := i

		fullCh := make(chan struct{}, 1)

		b.buckets[i] = &asyncBatchItems[Item]{
			items:  make([]Item, 0, batch),
			mux:    sync.Mutex{},
			fullCh: fullCh,
		}

		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			collect(b.buckets[i], fullCh, batch, timeout, batchFunc)
		}()
	}

	return b
}

func (b *AsyncBatcher[Item]) Batch(item Item) error {
	idx := atomic.AddInt64(&b.count, 1)
	batchIdx := (idx / b.size) % int64(len(b.buckets))

	bi := b.buckets[batchIdx]

	bi.mux.Lock()
	defer bi.mux.Unlock()

	if bi.fullCh == nil {
		return fmt.Errorf("batcher is closed")
	}

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
	for _, bi := range b.buckets {
		bi.mux.Lock()
		close(bi.fullCh)
		bi.fullCh = nil
		bi.mux.Unlock()
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

func collect[Item any](bi *asyncBatchItems[Item], fullCh chan struct{}, size int64, timeout time.Duration, wbf AsyncBatchFunc[Item]) {
	items := make([]Item, 0, size)
	var exit bool

	timer := acquireTimer(timeout)

	for {
		if exit {
			return
		}

		select {
		case <-timer.C:
		case _, ok := <-fullCh:
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
		bi.mux.Unlock()

		releaseTimer(timer)
		timer = acquireTimer(timeout)

		if len(items) > 0 {
			wbf(items)
			items = items[:0]
		}
	}
}
