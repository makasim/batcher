package batcher

import (
	"sync"
	"sync/atomic"
	"time"
)

type syncItem[Item any] struct {
	item  Item
	resCh chan []Item
}

type Batcher[Item any] struct {
	wg      *sync.WaitGroup
	size    int64
	count   int64
	timeout time.Duration
	batches [100]chan syncItem[Item]
}

func NewSync[Item any](size int64, timeout time.Duration) *Batcher[Item] {
	if size <= 0 {
		panic("size must be greater than zero")
	}

	b := &Batcher[Item]{
		size:  size,
		count: -1,
		wg:    &sync.WaitGroup{},
	}

	for i := 0; i < len(b.batches); i++ {
		i := i

		b.batches[i] = make(chan syncItem[Item], size*2)

		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			collectSync(b.batches[i], size, timeout)
		}()
	}

	return b
}

func (b *Batcher[Item]) Batch(item Item) []Item {
	idx := atomic.AddInt64(&b.count, 1)
	batchIdx := (idx / b.size) % 100

	si := syncItem[Item]{
		item:  item,
		resCh: make(chan []Item, 1),
	}

	select {
	case b.batches[batchIdx] <- si:
		return <-si.resCh
	}
}

func (b *Batcher[Item]) Shutdown() {
	for i := range b.batches {
		close(b.batches[i])
	}
	b.wg.Wait()
}

func collectSync[Item any](batchCh <-chan syncItem[Item], size int64, timeout time.Duration) {
	syncItems := make([]syncItem[Item], 0, size)

	t := acquireTimer(timeout)

	toItems := func(syncItems []syncItem[Item]) []Item {
		items := make([]Item, len(syncItems))
		for i, si := range syncItems {
			items[i] = si.item
		}

		return items
	}

	for {
		select {
		case si, ok := <-batchCh:
			if !ok {
				if len(syncItems) > 0 {
					syncItems[len(syncItems)-1].resCh <- toItems(syncItems)
					syncItems = syncItems[:0]
				}

				return
			}

			syncItems = append(syncItems, si)
			if len(syncItems) > 1 {
				syncItems[len(syncItems)-2].resCh <- nil
			}

			if len(syncItems) >= int(size) {
				syncItems[len(syncItems)-1].resCh <- toItems(syncItems)
				syncItems = syncItems[:0]
			}

			releaseTimer(t)
			t = acquireTimer(timeout)
		case <-t.C:
			if len(syncItems) > 0 {
				t := toItems(syncItems)
				syncItems[len(syncItems)-1].resCh <- t
				syncItems = syncItems[:0]
			}

			releaseTimer(t)
			t = acquireTimer(timeout)
		}
	}
}
