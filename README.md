# Batcher

Compose concurrent single items into batches on the fly. 

## Usage

```go
package main

import "github.com/makasim/batcher"

func main() {
	b := batcher.New[string](10)

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

## Results

[Gist](https://gist.github.com/makasim/d5c1ffd5fa07e1e95d84e750b18f137e)