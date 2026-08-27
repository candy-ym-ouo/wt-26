package storage

import (
	"fmt"
	"sync"
	"testing"
)

func TestBug08ConcurrentSeriesRegistrationIsUnique(t *testing.T) {
	const identical = 96
	const distinct = 64
	index := NewIndex()
	start := make(chan struct{})
	var wg sync.WaitGroup

	for worker := 0; worker < identical; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			index.Register("register.concurrent", map[string]string{"host": "shared"})
		}()
	}
	for worker := 0; worker < distinct; worker++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			index.Register("register.concurrent", map[string]string{"host": fmt.Sprintf("node-%03d", id)})
		}(worker)
	}
	close(start)
	wg.Wait()

	if got := index.Filter("register.concurrent", map[string]string{"host": "shared"}, 0); len(got) != 1 {
		t.Fatalf("identical series resolved to %d identities", len(got))
	}
	if got, want := index.Count(), distinct+1; got != want {
		t.Fatalf("cardinality=%d want=%d", got, want)
	}
}
