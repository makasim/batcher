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
	//used       []chan struct{}
	timeout time.Duration
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
		//used:       make([]chan struct{}, size*100),
		timeout: timeout,
	}

	for i := range b.rangeStart {
		b.rangeStart[i] = make(chan int64)
		//b.used[i] = make(chan struct{}, 1)
	}

	return b
}

func (b *Batcher[Item]) Batch(item Item) []Item {
	if b.size == 1 {
		return []Item{item}
	}

	idx := atomic.AddInt64(&b.count, 1)
	bufferIdx := idx % int64(len(b.buffer))

	//b.used[bufferIdx] <- struct{}{}
	//defer func() {
	//	<-b.used[bufferIdx]
	//}()

	b.buffer[bufferIdx] = item

	start := bufferIdx
	t := acquireTimer(b.timeout)
	defer releaseTimer(t)

	for {
		select {
		case start = <-b.rangeStart[b.safeRangeStart(start)]:
			var size int64
			if start < bufferIdx {
				size = bufferIdx - start
				//log.Println("size1: ", size)
			} else {
				//log.Println(len(b.buffer), start, bufferIdx)
				size = (int64(len(b.buffer)) - start) + bufferIdx + 1
				//log.Println("size2: ", size)
			}

			if size >= (b.size - 1) {
				res := b.formBuffer(start, bufferIdx)
				//log.Println("res: ", len(res))
				return res
			}

			continue
		case b.rangeStart[bufferIdx] <- start:
			return nil
		case <-t.C:
			res := b.formBuffer(start, bufferIdx)
			//log.Println("tmt1: ", bufferIdx, start, b.safeRangeStart(start))
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
