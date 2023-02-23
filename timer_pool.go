package batcher

// Copy-paste from https://gist.github.com/FZambia/61bd96b198858d0797718b24e638be74

import (
	"sync"
	"time"
)

var timerPool sync.Pool

// acquireTimer returns time from pool if possible.
func acquireTimer(d time.Duration) *time.Timer {
	v := timerPool.Get()
	if v == nil {
		return time.NewTimer(d)
	}
	tm := v.(*time.Timer)
	if tm.Reset(d) {
		// active timer?
		return time.NewTimer(d)
	}
	return tm
}

// releaseTimer returns timer into pool.
func releaseTimer(tm *time.Timer) {
	if !tm.Stop() {
		// tm.Stop() returns false if the timer has already expired or been stopped.
		// We can't be sure that timer.C will not be filled after timer.Stop(),
		// see https://groups.google.com/forum/#!topic/golang-nuts/-8O3AknKpwk
		//
		// The tip from manual to read from timer.C possibly blocks caller if caller
		// has already done <-timer.C. Non-blocking read from timer.C with select does
		// not help either because send is done concurrently from another goroutine.
		return
	}
	timerPool.Put(tm)
}
