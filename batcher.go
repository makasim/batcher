package batcher

import (
	"sync"
	"sync/atomic"
	"time"
)

type batch[Item any] struct {
	sync.Mutex
	items []Item

	blockCh  chan struct{}
	timeoutT *time.Timer
}

type Batcher[Item any] struct {
	batch   int64
	timeout time.Duration
	count   int64
	batches [100]*batch[Item]
}

func New[Item any](size int64, timeout time.Duration) *Batcher[Item] {
	if size <= 0 {
		panic("batch must be greater than zero")
	}

	b := &Batcher[Item]{
		batch:   size,
		timeout: timeout,
	}

	for i := range b.batches {
		b.batches[i] = &batch[Item]{
			timeoutT: time.NewTimer(timeout),
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

	resetTimer(bb.timeoutT, b.timeout)

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

	select {
	case <-blockCh:
		return nil
	case <-bb.timeoutT.C:
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

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:

		}
	}
	t.Reset(d)
}
