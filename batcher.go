package batcher

import (
	"sync"
	"sync/atomic"
	"time"
)

type batch[Item any] struct {
	sync.Mutex
	items []Item

	blockCh chan struct{}
}

type Batcher[Item any] struct {
	batch   int64
	timeout time.Duration
	count   int64
	batches [100]*batch[Item]
}

func New[Item any](size int64, timeout time.Duration) *Batcher[Item] {
	if size <= 0 {
		panic("size must be greater than zero")
	}
	if timeout <= 0 {
		panic("timeout must be greater than zero")
	}

	b := &Batcher[Item]{
		batch:   size,
		timeout: timeout,
		count:   -1,
	}

	for i := range b.batches {
		b.batches[i] = &batch[Item]{
			items: make([]Item, 0, b.batch),
		}
	}

	return b
}

func (b *Batcher[Item]) Batch(item Item) []Item {
	idx := atomic.AddInt64(&b.count, 1)

	batchIdx := int64(idx/b.batch) % 100

	bb := b.batches[batchIdx]

	bb.Lock()
	if bb.blockCh != nil {
		close(bb.blockCh)
		bb.blockCh = nil
	}

	bb.items = append(bb.items, item)
	if len(bb.items) == int(b.batch) {
		items := append([]Item{}, bb.items...)
		bb.items = bb.items[:0]
		bb.Unlock()
		return items
	}

	blockCh := make(chan struct{})
	bb.blockCh = blockCh
	bb.Unlock()

	timeoutT := acquireTimer(b.timeout)
	defer releaseTimer(timeoutT)

	select {
	case <-blockCh:
		return nil
	case <-timeoutT.C:
		bb.Lock()
		defer bb.Unlock()
		if bb.blockCh != nil {
			close(bb.blockCh)
			bb.blockCh = nil
		}

		items := append([]Item{}, bb.items...)
		bb.items = bb.items[:0]
		return items
	}
}
