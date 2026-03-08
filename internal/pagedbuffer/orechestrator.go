package pagedbuffer

import (
	"math/rand"
	"sync"

	"github.com/a-kazakov/gomr/internal/must"
)

// Orchestrator is responsible for handing out pages to the PagedBuffer
type PagedBufferOrchestrator struct {
	pagesPool      sync.Pool
	nPagesUsed     int32
	maxPages       int32
	maxPagesBase   int32
	maxPagesJitter int32
}

func NewPagedBufferOrchestrator(maxBufferSize int64, maxBufferSizeJitter float64) *PagedBufferOrchestrator {
	result := PagedBufferOrchestrator{
		pagesPool:      sync.Pool{New: func() any { return &MemPage{isInitialized: false} }},
		nPagesUsed:     0,
		maxPagesBase:   int32(maxBufferSize / PAGE_SIZE),
		maxPagesJitter: int32(float64(maxBufferSize)/PAGE_SIZE*maxBufferSizeJitter + 0.5),
	}
	result.ResetMaxPages()
	return &result
}

func (o *PagedBufferOrchestrator) ResetMaxPages() {
	if o.maxPagesJitter <= 0 {
		o.maxPages = o.maxPagesBase
		return
	}
	offset := rand.Int31n(o.maxPagesJitter*2) - o.maxPagesJitter
	o.maxPages = o.maxPagesBase + offset
}

func (o *PagedBufferOrchestrator) GetPage() *MemPage {
	page := o.pagesPool.Get().(*MemPage)
	if !page.isInitialized {
		page.data = make([]byte, PAGE_SIZE)
		page.isInitialized = true
	}
	o.nPagesUsed++
	return page
}

func (o *PagedBufferOrchestrator) IsFull() bool {
	return o.nPagesUsed >= o.maxPages
}

func (o *PagedBufferOrchestrator) ReleasePage(page *MemPage) {
	must.BeTrue(page != nil, "Attempted to release a nil page")
	o.pagesPool.Put(page)
	o.nPagesUsed--
}
