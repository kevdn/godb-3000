package storage

import (
	"encoding/binary"
	"fmt"
)

// Page size is typically 4KB or 8KB. We use 4KB as it's a common OS page size.
const (
	PageSize     = 4096
	PageHeader   = 4 // Header: 2 bytes type + 2 bytes
	MaxKeySize   = 1024
	MaxValueSize = 3000
)

// Page types
const (
	PageTypeNode = 1 // B-tree node page
	PageTypeMeta = 3 // Metadata page
)

// Page represents a disk page, the fundamental unit of storage.
// All disk I/O happens at page granularity.
//
// Layout:
// | Type (2 bytes) | Reserved (2 bytes) | Payload (4092 bytes) |
type Page struct {
	data [PageSize]byte
}

// NewPage creates a new page with the given type.
func NewPage(pageType uint16) *Page {
	p := &Page{}
	p.SetType(pageType)
	return p
}

// Type returns the page type.
func (p *Page) Type() uint16 {
	return binary.LittleEndian.Uint16(p.data[0:2])
}

// SetType sets the page type.
func (p *Page) SetType(pageType uint16) {
	binary.LittleEndian.PutUint16(p.data[0:2], pageType)
}

// Data returns the raw page data (entire 4KB).
func (p *Page) Data() []byte {
	return p.data[:]
}

// Payload returns the payload area (excluding header).
func (p *Page) Payload() []byte {
	return p.data[PageHeader:]
}

// Copy creates a deep copy of the page.
func (p *Page) Copy() *Page {
	newPage := &Page{}
	copy(newPage.data[:], p.data[:])
	return newPage
}

// PageID represents a unique identifier for a page (offset in file / PageSize).
type PageID uint64

const InvalidPageID PageID = 0

// IsValid checks if the page ID is valid.
func (pid PageID) IsValid() bool {
	return pid != InvalidPageID
}

// Offset returns the byte offset in the file for this page.
func (pid PageID) Offset() int64 {
	return int64(pid) * PageSize
}

// PageFromOffset converts a byte offset to a page ID.
func PageFromOffset(offset int64) PageID {
	return PageID(offset / PageSize)
}

// PageCache provides a simple in-memory cache for pages.
// This is a basic implementation; production systems use more sophisticated
// caching strategies (LRU, clock, etc.).
type PageCache struct {
	pages map[PageID]*Page
	size  int
	max   int
}

// NewPageCache creates a new page cache with the given maximum size.
func NewPageCache(maxPages int) *PageCache {
	return &PageCache{
		pages: make(map[PageID]*Page),
		max:   maxPages,
	}
}

// Get retrieves a page from cache.
func (pc *PageCache) Get(pid PageID) (*Page, bool) {
	p, ok := pc.pages[pid]
	return p, ok
}

// Put adds a page to the cache. Returns true if eviction occurred.
func (pc *PageCache) Put(pid PageID, page *Page) bool {
	if _, exists := pc.pages[pid]; exists {
		pc.pages[pid] = page
		return false
	}

	// Simple eviction: if full, clear the entire cache
	evicted := false
	if pc.size >= pc.max {
		pc.Clear()
		evicted = true
	}

	pc.pages[pid] = page
	pc.size++
	return evicted
}

// Remove removes a page from the cache.
func (pc *PageCache) Remove(pid PageID) {
	if _, exists := pc.pages[pid]; exists {
		delete(pc.pages, pid)
		pc.size--
	}
}

// Clear removes all pages from the cache.
func (pc *PageCache) Clear() {
	pc.pages = make(map[PageID]*Page)
	pc.size = 0
}

// Size returns the current number of pages in cache.
func (pc *PageCache) Size() int {
	return pc.size
}

// PageManager handles allocation and deallocation of pages.
// It maintains a free list for efficient page reuse.
type PageManager struct {
	nextPage PageID   // Next page to allocate if free list is empty
	freeList []PageID // Stack of free pages
}

// NewPageManager creates a new page manager.
// startPage is the first page ID to allocate (typically after metadata pages).
func NewPageManager(startPage PageID) *PageManager {
	return &PageManager{
		nextPage: startPage,
		freeList: make([]PageID, 0),
	}
}

// Allocate returns a page ID for a new page.
// It first tries to reuse a page from the free list, otherwise allocates a new one.
func (pm *PageManager) Allocate() PageID {
	if len(pm.freeList) > 0 {
		// Pop from free list
		pid := pm.freeList[len(pm.freeList)-1]
		pm.freeList = pm.freeList[:len(pm.freeList)-1]
		return pid
	}

	// Allocate new page
	pid := pm.nextPage
	pm.nextPage++
	return pid
}

// Free adds a page to the free list for reuse.
func (pm *PageManager) Free(pid PageID) {
	if !pid.IsValid() {
		return
	}
	pm.freeList = append(pm.freeList, pid)
}

// FreePages returns the number of pages in the free list.
func (pm *PageManager) FreePages() int {
	return len(pm.freeList)
}

// TotalPages returns the total number of pages that have been allocated.
func (pm *PageManager) TotalPages() PageID {
	return pm.nextPage
}

// Marshal serializes the page manager state (for persistence).
func (pm *PageManager) Marshal() []byte {
	// Format: nextPage (8 bytes) | freeListLen (4 bytes) | freeList (8*n bytes)
	size := 8 + 4 + len(pm.freeList)*8
	data := make([]byte, size)

	binary.LittleEndian.PutUint64(data[0:8], uint64(pm.nextPage))
	binary.LittleEndian.PutUint32(data[8:12], uint32(len(pm.freeList)))

	offset := 12
	for _, pid := range pm.freeList {
		binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(pid))
		offset += 8
	}

	return data
}

// Unmarshal deserializes the page manager state.
func (pm *PageManager) Unmarshal(data []byte) error {
	if len(data) < 12 {
		return fmt.Errorf("invalid page manager data: too short")
	}

	pm.nextPage = PageID(binary.LittleEndian.Uint64(data[0:8]))
	freeListLen := binary.LittleEndian.Uint32(data[8:12])

	expectedSize := 12 + int(freeListLen)*8
	if len(data) < expectedSize {
		return fmt.Errorf("invalid page manager data: free list truncated")
	}

	pm.freeList = make([]PageID, freeListLen)
	offset := 12
	for i := 0; i < int(freeListLen); i++ {
		pm.freeList[i] = PageID(binary.LittleEndian.Uint64(data[offset : offset+8]))
		offset += 8
	}

	return nil
}
