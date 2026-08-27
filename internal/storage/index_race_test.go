package storage

import (
	"fmt"
	"sync"
	"testing"
)

// TestIndexConcurrentRegistrationMix reproduces the high-concurrency write path:
// some goroutines hammer an identical metric+tag set, others register distinct
// label combinations under one metric. Under the broken register path this races
// the index maps (concurrent map writes) and drops or duplicates series.
func TestIndexConcurrentRegistrationMix(t *testing.T) {
	const same = 60
	const distinct = 40

	idx := NewIndex()
	var wg sync.WaitGroup

	// Hammer one fixed series from many goroutines.
	for i := 0; i < same; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx.Register("cpu.usage", map[string]string{"host": "pinned"})
		}()
	}
	// Distinct label sets, kept disjoint from "pinned".
	for i := 0; i < distinct; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			idx.Register("cpu.usage", map[string]string{"host": fmt.Sprintf("node-%02d", n)})
		}(i)
	}
	wg.Wait()

	// Same series must always collapse to exactly one identity.
	uniq := idx.Filter("cpu.usage", map[string]string{"host": "pinned"}, 0)
	if len(uniq) != 1 {
		t.Fatalf("duplicate identity for identical series: got %d", len(uniq))
	}
	// Every distinct label set plus the pinned one must survive.
	want := distinct + 1
	if got := idx.Count(); got != want {
		t.Fatalf("cardinality drift: got %d want %d", got, want)
	}
	// Querying the metric alone returns the full cardinality.
	all := idx.Filter("cpu.usage", nil, 0)
	if len(all) != want {
		t.Fatalf("query cardinality drift: got %d want %d", len(all), want)
	}
}
