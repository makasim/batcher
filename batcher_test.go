package batcher_test

import (
	"sort"
	"testing"
	"time"

	"github.com/makasim/batcher"
	"github.com/stretchr/testify/require"
)

func TestOne(t *testing.T) {
	b := batcher.New[int](10)

	require.Equal(t, []int{1}, b.Batch(1))
}

func TestFive(t *testing.T) {
	b := batcher.New[int](10)

	for i := 0; i < 4; i++ {
		go func(i int) {
			require.Nil(t, b.Batch(i))
		}(i)
	}

	time.Sleep(time.Millisecond * 50)
	res := b.Batch(4)

	sort.Ints(res)
	require.Len(t, res, 5)
	require.Equal(t, []int{0, 1, 2, 3, 4}, res)
}
