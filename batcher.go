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
	size       int64
	count      int64
	buffer     []Item
	rangeStart []chan int64
	timeout    time.Duration
}

func New[Item any](size int64, timeout time.Duration) *Batcher[Item] {
	if size <= 0 {
		panic("batch must be greater than zero")
	}
	if timeout <= 0 {
		panic("timeout must be greater than zero")
	}

	b := &Batcher[Item]{
		size:       size,
		count:      -1,
		buffer:     make([]Item, size*100),
		rangeStart: make([]chan int64, size*100),
		timeout:    timeout,
	}

	for i := range b.rangeStart {
		b.rangeStart[i] = make(chan int64)
	}

	return b
}

func (b *Batcher[Item]) Batch(item Item) []Item {
	idx := atomic.AddInt64(&b.count, 1)
	bufferIdx := idx % int64(len(b.buffer))

	//log.Println(idx, bufferIdx)
	b.buffer[bufferIdx] = item

	start := bufferIdx
	t := acquireTimer(b.timeout)
	defer releaseTimer(t)

	for {
		//log.Printf("item: %v; bufferIdx: %v; safeRangeStart: %v", item, bufferIdx, b.safeRangeStart(start))
		select {
		case start = <-b.rangeStart[b.safeRangeStart(start)]:
			//log.Println("got start", start)
			//log.Println(2)
			//log.Println(bufferIdx - start)
			if bufferIdx-start >= (b.size - 1) {

				res := b.formBuffer(start, bufferIdx)
				//log.Println("limit", len(res))
				return res
			}

			continue
		case b.rangeStart[bufferIdx] <- start:
			//log.Println("send start", start)
			//log.Println(3)
			return nil
		case <-t.C:
			//log.Println(4)
			res := b.formBuffer(start, bufferIdx)
			//log.Println("timeout", bufferIdx, len(res))
			return res
		}
	}
}

func (b *Batcher[Item]) safeRangeStart(start int64) int64 {
	if start == 0 {
		return int64(len(b.rangeStart) - 1)
	}

	return start - 1
}

func (b *Batcher[Item]) formBuffer(start, end int64) []Item {
	if start <= end {
		return append([]Item{}, b.buffer[start:end+1]...)
	}

	return append(append([]Item{}, b.buffer[start:]...), b.buffer[:end+1]...)
}
