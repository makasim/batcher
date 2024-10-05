package batcher_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/makasim/batcher"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestAsyncBatcher(main *testing.T) {
	main.Run("ZeroSize", func(t *testing.T) {
		require.PanicsWithValue(t, `size must be greater than zero`, func() {
			batcher.NewAsync[int](0, time.Second*60, func(items []int) {})
		})
	})

	main.Run("BatchOne", func(t *testing.T) {
		resultCh := make(chan []int, 10)

		b := batcher.NewAsync[int](1, time.Second*60, func(items []int) {
			resultCh <- append([]int(nil), items...)
		})
		defer func() {
			require.NoError(t, b.Shutdown(ctx100ms(t)))
		}()

		require.NoError(t, b.Batch(1))
		require.NoError(t, b.Batch(2))
		require.NoError(t, b.Batch(3))

		results := make([][]int, 0)
		results = append(results, <-resultCh)
		results = append(results, <-resultCh)
		results = append(results, <-resultCh)

		require.Contains(t, results, []int{1})
		require.Contains(t, results, []int{2})
		require.Contains(t, results, []int{3})
	})

	main.Run("BatchTwo", func(t *testing.T) {
		resultCh := make(chan []int, 10)

		b := batcher.NewAsync[int](2, time.Second*60, func(items []int) {
			resultCh <- append([]int(nil), items...)
		})
		defer func() {
			require.NoError(t, b.Shutdown(ctx100ms(t)))
		}()

		require.NoError(t, b.Batch(1))
		require.NoError(t, b.Batch(2))

		require.NoError(t, b.Batch(3))
		require.NoError(t, b.Batch(4))

		require.NoError(t, b.Batch(5))
		require.NoError(t, b.Batch(6))

		results := make([][]int, 0)
		results = append(results, <-resultCh)
		results = append(results, <-resultCh)
		results = append(results, <-resultCh)

		require.Contains(t, results, []int{1, 2})
		require.Contains(t, results, []int{3, 4})
		require.Contains(t, results, []int{5, 6})
	})

	main.Run("BatchFive", func(t *testing.T) {
		resultCh := make(chan []int, 10)

		b := batcher.NewAsync[int](5, time.Second*60, func(items []int) {
			resultCh <- append([]int(nil), items...)
		})
		defer func() {
			require.NoError(t, b.Shutdown(ctx100ms(t)))
		}()

		for i := 0; i < 15; i++ {
			require.NoError(t, b.Batch(i))
		}

		results := make([][]int, 0)
		results = append(results, <-resultCh)
		results = append(results, <-resultCh)
		results = append(results, <-resultCh)

		require.Contains(t, results, []int{0, 1, 2, 3, 4})
		require.Contains(t, results, []int{5, 6, 7, 8, 9})
		require.Contains(t, results, []int{10, 11, 12, 13, 14})
	})

	main.Run("BatchTen", func(t *testing.T) {
		resultCh := make(chan []int, 10)

		b := batcher.NewAsync[int](10, time.Second*60, func(items []int) {
			resultCh <- append([]int(nil), items...)
		})
		defer func() {
			require.NoError(t, b.Shutdown(ctx100ms(t)))
		}()

		for i := 0; i < 30; i++ {
			require.NoError(t, b.Batch(i))
		}

		results := make([][]int, 0)
		results = append(results, <-resultCh)
		results = append(results, <-resultCh)
		results = append(results, <-resultCh)

		require.Contains(t, results, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
		require.Contains(t, results, []int{10, 11, 12, 13, 14, 15, 16, 17, 18, 19})
		require.Contains(t, results, []int{20, 21, 22, 23, 24, 25, 26, 27, 28, 29})
	})

	main.Run("Rate100RPS", func(t *testing.T) {
		b := batcher.NewAsync[int](10, time.Second*60, func(items []int) {})
		defer func() {
			require.NoError(t, b.Shutdown(ctx100ms(t)))
		}()

		wg := &sync.WaitGroup{}
		closeCh := make(chan struct{})
		rl := rate.NewLimiter(100, 1)
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-closeCh:
						return
					default:
						require.NoError(t, rl.Wait(context.Background()))
						require.NoError(t, b.Batch(1))
					}
				}
			}()
		}

		time.Sleep(5 * time.Second)
		close(closeCh)
		wg.Wait()
	})

	main.Run("ErrBufferOverflow", func(t *testing.T) {
		releaseCh := make(chan struct{})

		b := batcher.NewAsync[int](1, time.Second*60, func(items []int) {
			<-releaseCh
		})

		// 100 is internal batches size
		// first 100 goes to collect and blocks in batchFunc
		// second 100 goes to batchCh and blocks there
		var err error
		for i := 0; i < 205; i++ {
			if err = b.Batch(i); err != nil {
				break
			}

		}

		require.EqualError(t, err, "buffer is full")

		close(releaseCh)

		require.NoError(t, b.Shutdown(ctx100ms(t)))
	})

	main.Run("CollectTimeout", func(t *testing.T) {
		resultCh := make(chan []int, 10)

		b := batcher.NewAsync[int](3, time.Millisecond*200, func(items []int) {
			resultCh <- append([]int(nil), items...)
		})

		results := make([][]int, 0)

		require.NoError(t, b.Batch(1))
		require.NoError(t, b.Batch(2))
		results = append(results, <-resultCh)

		require.NoError(t, b.Batch(3))
		require.NoError(t, b.Batch(4))
		results = append(results, <-resultCh)
		results = append(results, <-resultCh)

		require.NoError(t, b.Batch(5))
		require.NoError(t, b.Batch(6))
		results = append(results, <-resultCh)

		require.NoError(t, b.Batch(7))
		require.NoError(t, b.Batch(8))
		require.NoError(t, b.Batch(9))
		results = append(results, <-resultCh)

		require.Contains(t, results, []int{1, 2})
		require.Contains(t, results, []int{3})
		require.Contains(t, results, []int{4})
		require.Contains(t, results, []int{5, 6})
		require.Contains(t, results, []int{7, 8, 9})
	})

	main.Run("ShutdownGraceful", func(t *testing.T) {
		resultCh := make(chan []int, 10)

		b := batcher.NewAsync[int](3, time.Second*30, func(items []int) {
			time.Sleep(time.Millisecond * 500)
			resultCh <- append([]int(nil), items...)
		})

		results := make([][]int, 0)

		require.NoError(t, b.Batch(1))
		require.NoError(t, b.Batch(2))
		require.NoError(t, b.Batch(3))

		require.NoError(t, b.Batch(4))
		require.NoError(t, b.Batch(5))
		require.NoError(t, b.Batch(6))

		require.NoError(t, b.Batch(7))
		require.NoError(t, b.Batch(8))
		require.NoError(t, b.Batch(9))

		shutdownCtx, shutdownCtxCancel := context.WithTimeout(context.Background(), time.Second)
		defer shutdownCtxCancel()
		require.NoError(t, b.Shutdown(shutdownCtx))

		results = append(results, <-resultCh)
		results = append(results, <-resultCh)
		results = append(results, <-resultCh)

		require.Contains(t, results, []int{1, 2, 3})
		require.Contains(t, results, []int{4, 5, 6})
		require.Contains(t, results, []int{7, 8, 9})
	})

	main.Run("ShutdownGraceful2", func(t *testing.T) {
		resultCh := make(chan []int, 10)

		b := batcher.NewAsync[int](2, time.Second*30, func(items []int) {
			time.Sleep(time.Millisecond * 200)
			resultCh <- append([]int(nil), items...)
		})

		require.NoError(t, b.Batch(1))

		shutdownCtx, shutdownCtxCancel := context.WithTimeout(context.Background(), time.Second*5)
		defer shutdownCtxCancel()
		require.NoError(t, b.Shutdown(shutdownCtx))
		results := make([][]int, 0)
		results = append(results, <-resultCh)

		require.Contains(t, results, []int{1})
	})

	main.Run("ShutdownTimeout", func(t *testing.T) {
		releaseCh := make(chan struct{})

		b := batcher.NewAsync[int](1, time.Second*60, func(items []int) {
			<-releaseCh
		})

		require.NoError(t, b.Batch(1))

		require.EqualError(t, b.Shutdown(ctx100ms(t)), `context deadline exceeded`)

		close(releaseCh)
	})
}

func ctx100ms(t *testing.T) context.Context {
	ctx, ctxCancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	t.Cleanup(ctxCancel)

	return ctx
}
