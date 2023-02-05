package batcher_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/makasim/batcher"
	"github.com/stretchr/testify/require"
)

func TestSizeOneRateOne(t *testing.T) {
	b := batcher.New[int64](1, time.Millisecond*2)

	result := genLoad(b, 1, 100, time.Millisecond)

	require.Equal(t, int64(200), result)
}

func TestSizeOneRateTwo(t *testing.T) {
	b := batcher.New[int64](1, time.Millisecond*2)

	result := genLoad(b, 2, 100, time.Millisecond)

	require.Equal(t, int64(400), result)
}

func TestSizeOneRateTen(t *testing.T) {
	b := batcher.New[int64](1, time.Millisecond*2)

	result := genLoad(b, 10, 100, time.Millisecond)

	require.Equal(t, int64(2000), result)
}

func TestSizeTenRateOne(t *testing.T) {
	b := batcher.New[int64](10, time.Millisecond*2)

	result := genLoad(b, 1, 1000, time.Millisecond)

	require.Equal(t, int64(2000), result)
}

func TestSizeTenRateTwo(t *testing.T) {
	b := batcher.New[int64](10, time.Millisecond*2)

	result := genLoad(b, 2, 1000, time.Millisecond)

	require.Equal(t, int64(4000), result)
}

func TestSizeTenRateTen(t *testing.T) {
	b := batcher.New[int64](10, time.Millisecond*2)

	result := genLoad(b, 10, 1000, time.Millisecond)

	require.Equal(t, int64(20000), result)
}

func TestSlowSizeTenRateTwo(t *testing.T) {
	b := batcher.New[int64](10, time.Millisecond)

	result := genLoad(b, 2, 100, time.Millisecond*2)

	require.Equal(t, int64(400), result)
}

func genLoad(b *batcher.Batcher[int64], concr, reqs int, wait time.Duration) int64 {
	wg := &sync.WaitGroup{}

	var results int64

	for i := 0; i < concr; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < reqs; i++ {
				batch := b.Batch(2)
				var lCnt int64
				for _, item := range batch {
					lCnt += item
				}

				atomic.AddInt64(&results, lCnt)
				time.Sleep(wait)
			}
		}()
	}

	wg.Wait()
	return results
}

//
//func TestFive(t *testing.T) {
//	b := batcher.New[int](10, time.Millisecond)
//
//	for i := 0; i < 4; i++ {
//		go func(i int) {
//			require.Nil(t, b.Batch(i))
//		}(i)
//	}
//
//	time.Sleep(time.Millisecond * 50)
//	res := b.Batch(4)
//
//	sort.Ints(res)
//	require.Len(t, res, 5)
//	require.Equal(t, []int{0, 1, 2, 3, 4}, res)
//}
