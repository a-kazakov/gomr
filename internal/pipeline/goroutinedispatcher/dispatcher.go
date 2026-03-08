// Package goroutinedispatcher provides a WaitGroup-based lifecycle manager for pipeline goroutines.
package goroutinedispatcher

import "sync"

type GoroutineDispatcher struct {
	runningGoroutinesCounter sync.WaitGroup
}

func NewGoroutineDispatcher() *GoroutineDispatcher {
	return &GoroutineDispatcher{
		runningGoroutinesCounter: sync.WaitGroup{},
	}
}

func (c *GoroutineDispatcher) StartTrackedGoroutine(f func()) {
	c.runningGoroutinesCounter.Add(1)
	go func() {
		defer c.runningGoroutinesCounter.Done()
		f()
	}()
}

// StartParallelTrackedGoroutines starts parallelism goroutines running fWork, then calls fCleanup after all finish.
// The cleanup runs in a separate goroutine that is tracked by the dispatcher.
func (c *GoroutineDispatcher) StartParallelTrackedGoroutines(parallelism int, fWork func(shardIndex int), fCleanup func()) {
	wg := sync.WaitGroup{}
	wg.Add(parallelism)
	c.runningGoroutinesCounter.Add(1)
	for i := range parallelism {
		goroutineIndex := i
		go func() {
			defer wg.Done()
			fWork(goroutineIndex)
		}()
	}
	go func() {
		defer c.runningGoroutinesCounter.Done()
		wg.Wait()
		fCleanup()
	}()
}

func (c *GoroutineDispatcher) WaitForAllGoroutinesToFinish() {
	c.runningGoroutinesCounter.Wait()
}
