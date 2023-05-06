package batcher

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type syncItem[Item any] struct {
	item  Item
	resCh chan []Item
}

type SyncBatcher[Item any] struct {
	wg      *sync.WaitGroup
	size    int64
	count   int64
	timeout time.Duration
	batches [100]chan syncItem[Item]
}

func NewSync[Item any](size int64, timeout time.Duration) *SyncBatcher[Item] {
	if size <= 0 {
		panic("size must be greater than zero")
	}

	b := &SyncBatcher[Item]{
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

func (b *SyncBatcher[Item]) Batch(item Item) []Item {
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

func (b *SyncBatcher[Item]) Shutdown(ctx context.Context) error {
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

			releaseTimer(t)
			t = acquireTimer(timeout)

			syncItems = append(syncItems, si)
			if len(syncItems) > 1 {
				syncItems[len(syncItems)-2].resCh <- nil
			}

			if len(syncItems) >= int(size) {
				syncItems[len(syncItems)-1].resCh <- toItems(syncItems)
				syncItems = syncItems[:0]
				t.Stop()
			}
		case <-t.C:
			if len(syncItems) > 0 {
				syncItems[len(syncItems)-1].resCh <- toItems(syncItems)
				syncItems = syncItems[:0]
			}
			t.Stop()
		}
	}
}
