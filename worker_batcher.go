package batcher

import (
	"context"
	"sync"
)

type WorkerBatchFunc[Item any] func(items []Item)

type WorkerBatcher[Item any] struct {
	bufferCh chan Item
	b        Batcher[Item]
	wbf      WorkerBatchFunc[Item]
	wg       *sync.WaitGroup
}

func NewWorkerBatcher[Item any](b Batcher[Item], bufferSize, workerSize int64, batchFunc WorkerBatchFunc[Item]) *WorkerBatcher[Item] {
	if bufferSize <= 0 {
		panic("batch must be greater than zero")
	}
	if workerSize <= 0 {
		panic("workerSize must be greater than zero")
	}

	wb := &WorkerBatcher[Item]{
		b:        b,
		bufferCh: make(chan Item, bufferSize),
		wbf:      batchFunc,
		wg:       &sync.WaitGroup{},
	}

	for i := 0; i < int(workerSize); i++ {
		wb.wg.Add(1)
		go func() {
			defer wb.wg.Done()

			for item := range wb.bufferCh {
				items := wb.b.Batch(item)
				if items == nil {
					continue
				}

				wb.wbf(items)
			}
		}()
	}

	return wb
}

func (wb *WorkerBatcher[Item]) Batch(ctx context.Context, item Item) error {
	select {
	case wb.bufferCh <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (wb *WorkerBatcher[Item]) Shutdown() {
	close(wb.bufferCh)
	wb.wg.Wait()
}
