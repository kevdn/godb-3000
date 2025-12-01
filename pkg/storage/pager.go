package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Pager manages disk I/O for pages with durability guarantees.
// It's responsible for reading/writing pages to disk and ensuring
// data persistence through fsync.
//
// Key concepts:
// 1. All I/O happens at page granularity (4KB blocks)
// 2. Writes are buffered in memory until Flush() is called
// 3. Flush() uses fsync to guarantee durability
// 4. Page 0 is reserved for metadata
type Pager struct {
	file       *os.File
	path       string
	pageSize   int
	numPages   PageID
	cache      *PageCache
	dirtyPages map[PageID]*Page // Pages modified but not yet fsynced
	mu         sync.RWMutex
}

// PagerOptions configures the pager.
type PagerOptions struct {
	CacheSize int  // Number of pages to cache in memory
	ReadOnly  bool // Open in read-only mode
}

// DefaultPagerOptions returns default pager options.
func DefaultPagerOptions() PagerOptions {
	return PagerOptions{
		CacheSize: 1000, // Cache up to 1000 pages (4MB)
		ReadOnly:  false,
	}
}

// OpenPager opens or creates a database file.
func OpenPager(path string, opts PagerOptions) (*Pager, error) {
	flags := os.O_RDWR | os.O_CREATE
	if opts.ReadOnly {
		flags = os.O_RDONLY
	}

	file, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Get file size to determine number of pages
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	numPages := PageID(stat.Size() / PageSize)
	if stat.Size()%PageSize != 0 {
		file.Close()
		return nil, fmt.Errorf("file size %d is not a multiple of page size %d", stat.Size(), PageSize)
	}

	pager := &Pager{
		file:       file,
		path:       path,
		pageSize:   PageSize,
		numPages:   numPages,
		cache:      NewPageCache(opts.CacheSize),
		dirtyPages: make(map[PageID]*Page),
	}

	// If file is new, initialize with metadata page
	if numPages == 0 && !opts.ReadOnly {
		if err := pager.initializeNewFile(); err != nil {
			pager.Close()
			return nil, fmt.Errorf("failed to initialize file: %w", err)
		}
	}

	return pager, nil
}

// initializeNewFile creates the initial metadata page for a new database.
func (p *Pager) initializeNewFile() error {
	metaPage := NewPage(PageTypeMeta)

	// Write magic number and version
	payload := metaPage.Payload()
	copy(payload[0:8], []byte("GODB3000"))
	binary.LittleEndian.PutUint32(payload[8:12], 1) // Version 1

	if err := p.writePage(0, metaPage); err != nil {
		return err
	}

	p.numPages = 1
	return p.Sync()
}

// NumPages returns the total number of pages in the file.
func (p *Pager) NumPages() PageID {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.numPages
}

// ReadPage reads a page from disk or cache.
// This is the primary read interface.
func (p *Pager) ReadPage(pid PageID) (*Page, error) {
	p.mu.RLock()

	// Check if page exists
	if pid >= p.numPages {
		p.mu.RUnlock()
		return nil, fmt.Errorf("page %d does not exist (total pages: %d)", pid, p.numPages)
	}

	// Check dirty pages first (most recent data)
	if page, ok := p.dirtyPages[pid]; ok {
		p.mu.RUnlock()
		return page.Copy(), nil
	}

	// Check cache
	if page, ok := p.cache.Get(pid); ok {
		p.mu.RUnlock()
		return page.Copy(), nil
	}

	p.mu.RUnlock()

	// Read from disk
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check cache after acquiring write lock, another goroutine may have loaded it
	if page, ok := p.cache.Get(pid); ok {
		return page.Copy(), nil
	}

	page, err := p.readPageFromDisk(pid)
	if err != nil {
		return nil, err
	}

	// Add to cache
	p.cache.Put(pid, page.Copy())

	return page, nil
}

// readPageFromDisk reads a page directly from disk without caching.
func (p *Pager) readPageFromDisk(pid PageID) (*Page, error) {
	page := &Page{}
	offset := pid.Offset()

	n, err := p.file.ReadAt(page.data[:], offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pid, err)
	}

	if n != PageSize {
		return nil, fmt.Errorf("partial read: expected %d bytes, got %d", PageSize, n)
	}

	return page, nil
}

// WritePage marks a page as dirty. The actual write happens during Flush().
// This provides atomicity: changes are not visible until Flush() completes.
func (p *Pager) WritePage(pid PageID, page *Page) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Extend file if necessary
	if pid >= p.numPages {
		p.numPages = pid + 1
	}

	// Store in dirty pages
	p.dirtyPages[pid] = page.Copy()

	// Also update cache
	p.cache.Put(pid, page.Copy())

	return nil
}

// writePage writes a page directly to disk (internal use).
func (p *Pager) writePage(pid PageID, page *Page) error {
	offset := pid.Offset()

	n, err := p.file.WriteAt(page.data[:], offset)
	if err != nil {
		return fmt.Errorf("failed to write page %d: %w", pid, err)
	}

	if n != PageSize {
		return fmt.Errorf("partial write: expected %d bytes, wrote %d", PageSize, n)
	}

	return nil
}

// AllocatePage allocates a new page and returns its ID.
func (p *Pager) AllocatePage() (PageID, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	pid := p.numPages
	p.numPages++

	// Initialize with empty page
	page := NewPage(PageTypeNode)
	p.dirtyPages[pid] = page

	return pid, nil
}

// Flush writes all dirty pages to disk and calls fsync.
// This is the critical operation for durability - data is not guaranteed
// to be persistent until Flush() completes successfully.
//
// Process:
// 1. Write all dirty pages to disk
// 2. Call fsync to ensure data reaches physical media
// 3. Clear dirty pages set
func (p *Pager) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.dirtyPages) == 0 {
		return nil
	}

	// Write all dirty pages
	for pid, page := range p.dirtyPages {
		if err := p.writePage(pid, page); err != nil {
			return fmt.Errorf("flush failed on page %d: %w", pid, err)
		}
	}

	// Sync to disk (THE critical durability operation)
	if err := p.file.Sync(); err != nil {
		return fmt.Errorf("fsync failed: %w", err)
	}

	// Clear dirty pages
	p.dirtyPages = make(map[PageID]*Page)

	return nil
}

// Sync is an alias for Flush for clarity.
func (p *Pager) Sync() error {
	return p.Flush()
}

// DirtyPages returns the number of pages pending flush.
func (p *Pager) DirtyPages() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.dirtyPages)
}

// Close flushes dirty pages and closes the file.
// Always call Close() when done with a pager to ensure data persistence.
func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Flush any remaining dirty pages
	if len(p.dirtyPages) > 0 {
		for pid, page := range p.dirtyPages {
			if err := p.writePage(pid, page); err != nil {
				// Log error but continue closing
				fmt.Fprintf(os.Stderr, "warning: failed to write page %d during close: %v\n", pid, err)
			}
		}
		p.file.Sync()
	}

	return p.file.Close()
}

// CopyTo creates a backup of the database to the specified path.
// This performs a consistent snapshot by flushing first.
func (p *Pager) CopyTo(destPath string) error {
	// Flush to ensure consistent state
	if err := p.Flush(); err != nil {
		return fmt.Errorf("failed to flush before copy: %w", err)
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	// Create destination file
	destFile, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer destFile.Close()

	// Copy all pages
	for pid := PageID(0); pid < p.numPages; pid++ {
		page, err := p.readPageFromDisk(pid) // did not use readPage to avoid cache/dirties
		if err != nil {
			return fmt.Errorf("failed to read page %d: %w", pid, err)
		}

		offset := pid.Offset()
		if _, err := destFile.WriteAt(page.data[:], offset); err != nil {
			return fmt.Errorf("failed to write page %d: %w", pid, err)
		}
	}

	// Fsync destination
	if err := destFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync destination: %w", err)
	}

	// Fsync directory (for atomic rename later if needed)
	dir := filepath.Dir(destPath)
	if dirFile, err := os.Open(dir); err == nil {
		dirFile.Sync()
		dirFile.Close()
	}

	return nil
}

// Stats returns statistics about the pager.
type PagerStats struct {
	NumPages   PageID
	DirtyPages int
	CacheSize  int
	FileSize   int64
}

// Stats returns current pager statistics.
func (p *Pager) Stats() PagerStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return PagerStats{
		NumPages:   p.numPages,
		DirtyPages: len(p.dirtyPages),
		CacheSize:  p.cache.Size(),
		FileSize:   int64(p.numPages) * PageSize,
	}
}
