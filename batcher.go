package batcher

import (
	"sync/atomic"
	"time"
)

type Batcher[T any] struct {
	collectorCh chan T
	promoteT    *time.Timer

	counter int64
	batch   int64
	active  int64
}

func New[T any](batch int64) *Batcher[T] {
	if batch <= 0 {
		panic("batch must be greater than zero")
	}

	return &Batcher[T]{
		collectorCh: make(chan T, batch*10),
		promoteT:    time.NewTimer(time.Millisecond * 100),
		batch:       batch,
	}
}

func (b *Batcher[T]) Batch(t T) []T {
	idx := atomic.AddInt64(&b.counter, 1)

	if idx%(b.batch) == 0 {
		return b.collect(t)
	}

	select {
	case b.collectorCh <- t:
		return nil
	case <-b.promoteT.C:
		return b.collect(t)
	}
}

func (b *Batcher[T]) collect(t T) []T {
	if cnt := atomic.AddInt64(&b.active, 1); cnt == 1 {
		stopAndDrainTimer(b.promoteT)
	}

	defer func() {
		if cnt := atomic.AddInt64(&b.active, -1); cnt == 0 {
			stopAndDrainTimer(b.promoteT)
			b.promoteT.Reset(time.Millisecond * 100)
		}
	}()

	res := make([]T, 0, b.batch)
	res = append(res, t)
	for {
		if int64(len(res)) >= b.batch {
			return res
		}

		select {
		case t := <-b.collectorCh:
			res = append(res, t)
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
