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
	promoteT *time.Timer
}

type Batcher[Item any] struct {
	batch   int64
	count   int64
	batches [100]*batch[Item]
}

func New[Item any](size int64) *Batcher[Item] {
	if size <= 0 {
		panic("batch must be greater than zero")
	}

	b := &Batcher[Item]{
		batch: size,
	}

	for i := range b.batches {
		b.batches[i] = &batch[Item]{
			promoteT: time.NewTimer(time.Millisecond * 100),
		}
	}

	return b
}

func (b *Batcher[Item]) Batch(item Item) []Item {
	idx := atomic.AddInt64(&b.count, 1)

	batchIdx := int64(idx/b.batch) % 100

	bb := b.batches[batchIdx]

	bb.Lock()
	resetTimer(bb.promoteT)
	bb.items = append(bb.items, item)

	if bb.blockCh != nil {
		close(bb.blockCh)
		bb.blockCh = nil
	}

	if len(bb.items) == int(b.batch) {
		items := append([]Item{}, bb.items...)
		bb.items = bb.items[:0]
		bb.Unlock()
		return items
	}

	unblockCh := make(chan struct{})
	bb.blockCh = unblockCh
	bb.Unlock()

	select {
	case <-unblockCh:
		return nil
	case <-bb.promoteT.C:
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

func resetTimer(t *time.Timer) {
	t.Stop()
	select {
	case <-t.C:
	default:
	}
	t.Reset(time.Millisecond * 200)
}
