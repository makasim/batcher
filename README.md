# Batcher

Compose concurrent single items into batches on the fly. 

## Usage

Sync batcher either buffer an item and unblock the caller or return a batch to the caller.

```go
package main

import (
	"context"
	"time"

	"github.com/makasim/batcher"
)

func main() {
	b := batcher.NewSync[string](10, time.Second)
	defer b.Shutdown(context.Background())

	// add items to the batch concurrently
	batch := b.Batch(`foo`)
	if batch == nil {
		// the foo item goes to someone else batch
		// we are done here
		return
	}

	// process batch
}
```

Async batcher always buffer and unblock the caller and call the batch function in separate goroutine.

```go
package main

import (
    "context"
    "time"
    
    "github.com/makasim/batcher"
)

func main() {
    b := batcher.NewAsync[int](1, time.Second*60, func(items []int) {
        // process items
    })
    defer b.Shutdown(context.Background())

	b.Batch(context.Background(), 123)
}
```

## Results

[Gist](https://gist.github.com/makasim/d5c1ffd5fa07e1e95d84e750b18f137e)