package pagedbuffer

import (
	"encoding/binary"
	"fmt"
	"io"
)

type PagedBuffer struct {
	orchestrator *PagedBufferOrchestrator
	pages        []*MemPage
	Size         int
}

func NewPagedBuffer(orchestrator *PagedBufferOrchestrator) PagedBuffer {
	return PagedBuffer{
		orchestrator: orchestrator,
		pages:        make([]*MemPage, 0, MAX_PAGES),
	}
}

func (b *PagedBuffer) addPage() *MemPage {
	page := b.orchestrator.GetPage()
	n := len(b.pages)
	b.pages = b.pages[:n+1] // Intentionally not using append to avoid allocation
	b.pages[n] = page
	return page
}

func (b *PagedBuffer) ReadAt(p []byte, off int) (n int, err error) {
	if off < 0 {
		return 0, io.EOF
	}
	pageIndex := off >> PAGE_SIZE_POW2
	pageOffset := off & (PAGE_SIZE - 1)
	remaining := len(p)
	originalLength := len(p)
	for remaining > 0 && pageIndex < len(b.pages) {
		page := b.pages[pageIndex]
		canRead := min(remaining, b.Size-(PAGE_SIZE*pageIndex+pageOffset), PAGE_SIZE-pageOffset)
		if canRead <= 0 {
			break
		}
		copy(p[:canRead], page.data[pageOffset:pageOffset+canRead])
		p = p[canRead:]
		remaining -= canRead
		pageIndex++
		pageOffset = 0
	}
	if remaining > 0 {
		err = io.EOF
	}
	return originalLength - remaining, err
}

func (b *PagedBuffer) ReadUint64AtIndexUnsafe(index int) uint64 {
	byteOffset := index << 3
	pageIndex := byteOffset >> PAGE_SIZE_POW2
	pageOffset := byteOffset & (PAGE_SIZE - 1)
	page := b.pages[pageIndex]
	return binary.BigEndian.Uint64(page.data[pageOffset : pageOffset+8])
}

func (b *PagedBuffer) WriteUint64AtIndexUnsafe(index int, value uint64) {
	byteOffset := index << 3
	pageIndex := byteOffset >> PAGE_SIZE_POW2
	pageOffset := byteOffset & (PAGE_SIZE - 1)
	page := b.pages[pageIndex]
	binary.BigEndian.PutUint64(page.data[pageOffset:pageOffset+8], value)
}

func (b *PagedBuffer) PushBackUint64Unsafe(value uint64) {
	if b.Size&(PAGE_SIZE-1) == 0 {
		b.addPage()
	}
	pageOffset := b.Size & (PAGE_SIZE - 1)
	b.Size += 8
	page := b.pages[len(b.pages)-1]
	binary.BigEndian.PutUint64(page.data[pageOffset:pageOffset+8], value)
}

func (b *PagedBuffer) Clear() {
	b.Size = 0
	for _, page := range b.pages {
		b.orchestrator.ReleasePage(page)
	}
	b.pages = b.pages[:0]
}

func (b *PagedBuffer) Write(p []byte) (n int, err error) {
	if b.Size+len(p) > MAX_BUFFER_SIZE {
		return 0, fmt.Errorf("paged buffer is full: size=%d, max size=%d", b.Size+len(p), MAX_BUFFER_SIZE)
	}
	initialSize := b.Size
	for len(p) > 0 {
		remainingOnLastPage := PAGE_SIZE*len(b.pages) - b.Size
		if remainingOnLastPage == 0 {
			b.addPage()
			remainingOnLastPage = PAGE_SIZE
		}
		canWriteToPage := min(remainingOnLastPage, len(p))
		lastPage := b.pages[len(b.pages)-1]
		lastPageOffset := b.Size % PAGE_SIZE
		copy(lastPage.data[lastPageOffset:lastPageOffset+canWriteToPage], p[:canWriteToPage])
		b.Size += canWriteToPage
		p = p[canWriteToPage:]
	}
	return b.Size - initialSize, nil
}

func (b *PagedBuffer) CanTake(size int) bool {
	return b.Size+size <= MAX_BUFFER_SIZE
}
