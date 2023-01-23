package batcher_test

import (
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

	go func() {
		time.Sleep(time.Millisecond * 10)

		require.Nil(t, b.Batch(2))
		require.Nil(t, b.Batch(3))
		require.Nil(t, b.Batch(4))
		require.Nil(t, b.Batch(5))
	}()

	res := b.Batch(1)

	require.Len(t, res, 5)
}
