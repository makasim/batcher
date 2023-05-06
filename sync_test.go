package batcher_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/makasim/batcher"
	"github.com/stretchr/testify/require"
)

func TestSyncBatcher_SizeOne(main *testing.T) {
	main.Run("RateOne", func(t *testing.T) {
		b := batcher.NewSync[int64](1, time.Second)

		result := genLoad(b, 1, 100, time.Millisecond)

		require.Equal(t, int64(200), result)
	})

	main.Run("RateTwo", func(t *testing.T) {
		b := batcher.NewSync[int64](1, time.Second)

		result := genLoad(b, 2, 100, time.Millisecond)

		require.Equal(t, int64(400), result)
	})

	main.Run("RateFive", func(t *testing.T) {
		b := batcher.NewSync[int64](1, time.Second)

		result := genLoad(b, 5, 100, time.Millisecond)

		require.Equal(t, int64(1000), result)
	})

	main.Run("RateTen", func(t *testing.T) {
		b := batcher.NewSync[int64](1, time.Second)

		result := genLoad(b, 10, 100, time.Millisecond)

		require.Equal(t, int64(2000), result)
	})
}

func TestSyncBatcher_SizeTwo(main *testing.T) {
	main.Run("RateTwo", func(t *testing.T) {
		b := batcher.NewSync[int64](2, time.Second)

		result := genLoad(b, 2, 100, time.Millisecond)

		require.Equal(t, int64(400), result)
	})

	main.Run("RateFour", func(t *testing.T) {
		b := batcher.NewSync[int64](2, time.Millisecond*100)

		result := genLoad(b, 4, 100, time.Millisecond)

		require.Equal(t, int64(800), result)
	})

	main.Run("RateSix", func(t *testing.T) {
		b := batcher.NewSync[int64](2, time.Second)

		result := genLoad(b, 6, 100, time.Millisecond)

		require.Equal(t, int64(1200), result)
	})

	main.Run("RateTen", func(t *testing.T) {
		b := batcher.NewSync[int64](2, time.Millisecond*100)

		result := genLoad(b, 10, 100, time.Millisecond)

		require.Equal(t, int64(2000), result)
	})
}

func TestSyncBatcher_SizeTenRateOne(t *testing.T) {
	b := batcher.NewSync[int64](10, time.Millisecond*2)

	result := genLoad(b, 1, 1000, time.Millisecond)

	require.Equal(t, int64(2000), result)
}

func TestSyncBatcher_SizeTenRateTwo(t *testing.T) {
	b := batcher.NewSync[int64](10, time.Millisecond*2)

	result := genLoad(b, 2, 1000, time.Millisecond)

	require.Equal(t, int64(4000), result)
}

func TestSyncBatcher_SizeTenRateTen(t *testing.T) {
	b := batcher.NewSync[int64](10, time.Millisecond*2)

	result := genLoad(b, 10, 1000, time.Microsecond*100)

	require.Equal(t, int64(20000), result)
}

func TestSyncBatcher_SlowSizeTenRateTwo(t *testing.T) {
	b := batcher.NewSync[int64](10, time.Millisecond)

	result := genLoad(b, 2, 100, time.Millisecond*2)

	require.Equal(t, int64(400), result)
}

func genLoad(b *batcher.SyncBatcher[int64], concr, reqs int, wait time.Duration) int64 {
	wg := &sync.WaitGroup{}

	var results int64

	for i := 0; i < concr; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < reqs; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					batch := b.Batch(2)
					var lCnt int64
					for _, item := range batch {
						lCnt += item
					}

					atomic.AddInt64(&results, lCnt)
				}()
				time.Sleep(wait)
			}
		}()
	}

	wg.Wait()
	return results
}

//
//func TestSyncBatcher_Five(t *testing.T) {
//	b := batcher.NewSync[int](10, time.Millisecond)
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
