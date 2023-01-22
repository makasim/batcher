package batcher

import (
	"sync/atomic"
	"time"
)

type Batcher[Item any] struct {
	collectorCh chan Item
	promoteT    *time.Timer

	counter int64
	batch   int64
	active  int64
}

func New[Item any](batch int64) *Batcher[Item] {
	if batch <= 0 {
		panic("batch must be greater than zero")
	}

	return &Batcher[Item]{
		collectorCh: make(chan Item, batch*10),
		promoteT:    time.NewTimer(time.Millisecond * 100),
		batch:       batch,
	}
}

func (b *Batcher[Item]) Batch(item Item) []Item {
	idx := atomic.AddInt64(&b.counter, 1)

	if idx%(b.batch) == 0 {
		return b.collect(item)
	}

	select {
	case b.collectorCh <- item:
		return nil
	case <-b.promoteT.C:
		return b.collect(item)
	}
}

func (b *Batcher[Item]) collect(item Item) []Item {
	if cnt := atomic.AddInt64(&b.active, 1); cnt == 1 {
		stopAndDrainTimer(b.promoteT)
	}

	defer func() {
		if cnt := atomic.AddInt64(&b.active, -1); cnt == 0 {
			stopAndDrainTimer(b.promoteT)
			b.promoteT.Reset(time.Millisecond * 100)
		}
	}()

	res := make([]Item, 0, b.batch)
	res = append(res, item)
	for {
		if int64(len(res)) >= b.batch {
			return res
		}

		select {
		case item := <-b.collectorCh:
			res = append(res, item)
		default:
			return res
		}
	}
}

func stopAndDrainTimer(t *time.Timer) {
	t.Stop()
	select {
	case <-t.C:
	default:
	}
}
