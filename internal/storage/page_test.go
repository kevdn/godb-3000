package storage

import (
	"bytes"
	"testing"
)

func TestPage(t *testing.T) {
	t.Run("NewPage", func(t *testing.T) {
		page := NewPage(PageTypeNode)
		if page == nil {
			t.Fatal("NewPage returned nil")
		}
		if page.Type() != PageTypeNode {
			t.Errorf("Expected type %d, got %d", PageTypeNode, page.Type())
		}
	})

	t.Run("SetType", func(t *testing.T) {
		page := NewPage(PageTypeNode)
		page.SetType(PageTypeMeta)
		if page.Type() != PageTypeMeta {
			t.Errorf("Expected type %d, got %d", PageTypeMeta, page.Type())
		}
	})

	t.Run("Data", func(t *testing.T) {
		page := NewPage(PageTypeNode)
		data := page.Data()
		if len(data) != PageSize {
			t.Errorf("Expected data size %d, got %d", PageSize, len(data))
		}
	})

	t.Run("Payload", func(t *testing.T) {
		page := NewPage(PageTypeNode)
		payload := page.Payload()
		expectedSize := PageSize - PageHeader
		if len(payload) != expectedSize {
			t.Errorf("Expected payload size %d, got %d", expectedSize, len(payload))
		}
	})

	t.Run("Copy", func(t *testing.T) {
		page := NewPage(PageTypeNode)
		payload := page.Payload()
		copy(payload, []byte("test data"))

		copied := page.Copy()
		if copied == page {
			t.Error("Copy returned same instance")
		}

		if !bytes.Equal(page.Data(), copied.Data()) {
			t.Error("Copied data doesn't match original")
		}

		// Modify copy and ensure original is unchanged
		copiedPayload := copied.Payload()
		copy(copiedPayload, []byte("different"))

		if bytes.Equal(page.Data(), copied.Data()) {
			t.Error("Modifying copy affected original")
		}
	})
}

func TestPageID(t *testing.T) {
	t.Run("InvalidPageID", func(t *testing.T) {
		if InvalidPageID.IsValid() {
			t.Error("InvalidPageID should not be valid")
		}
	})

	t.Run("IsValid", func(t *testing.T) {
		pid := PageID(1)
		if !pid.IsValid() {
			t.Error("PageID 1 should be valid")
		}

		pid = PageID(0)
		if pid.IsValid() {
			t.Error("PageID 0 should be invalid")
		}
	})

	t.Run("Offset", func(t *testing.T) {
		tests := []struct {
			pid      PageID
			expected int64
		}{
			{PageID(0), 0},
			{PageID(1), PageSize},
			{PageID(2), PageSize * 2},
			{PageID(100), PageSize * 100},
		}

		for _, tt := range tests {
			offset := tt.pid.Offset()
			if offset != tt.expected {
				t.Errorf("PageID(%d).Offset() = %d, want %d", tt.pid, offset, tt.expected)
			}
		}
	})

	t.Run("PageFromOffset", func(t *testing.T) {
		tests := []struct {
			offset   int64
			expected PageID
		}{
			{0, PageID(0)},
			{PageSize, PageID(1)},
			{PageSize * 2, PageID(2)},
			{PageSize * 100, PageID(100)},
		}

		for _, tt := range tests {
			pid := PageFromOffset(tt.offset)
			if pid != tt.expected {
				t.Errorf("PageFromOffset(%d) = %d, want %d", tt.offset, pid, tt.expected)
			}
		}
	})
}

func TestPageCache(t *testing.T) {
	t.Run("NewPageCache", func(t *testing.T) {
		cache := NewPageCache(10)
		if cache == nil {
			t.Fatal("NewPageCache returned nil")
		}
		if cache.Size() != 0 {
			t.Errorf("Expected size 0, got %d", cache.Size())
		}
	})

	t.Run("PutAndGet", func(t *testing.T) {
		cache := NewPageCache(10)
		page := NewPage(PageTypeNode)
		pid := PageID(1)

		cache.Put(pid, page)
		if cache.Size() != 1 {
			t.Errorf("Expected size 1, got %d", cache.Size())
		}

		retrieved, ok := cache.Get(pid)
		if !ok {
			t.Error("Failed to retrieve page from cache")
		}
		if retrieved == nil {
			t.Error("Retrieved page is nil")
		}
		if retrieved.Type() != page.Type() {
			t.Error("Retrieved page type doesn't match")
		}
	})

	t.Run("GetNonExistent", func(t *testing.T) {
		cache := NewPageCache(10)
		_, ok := cache.Get(PageID(999))
		if ok {
			t.Error("Expected false for non-existent page")
		}
	})

	t.Run("UpdateExisting", func(t *testing.T) {
		cache := NewPageCache(10)
		pid := PageID(1)

		page1 := NewPage(PageTypeNode)
		cache.Put(pid, page1)
		initialSize := cache.Size()

		page2 := NewPage(PageTypeMeta)
		evicted := cache.Put(pid, page2)
		if evicted {
			t.Error("Should not have evicted when updating existing page")
		}
		if cache.Size() != initialSize {
			t.Error("Size changed when updating existing page")
		}

		retrieved, _ := cache.Get(pid)
		if retrieved.Type() != PageTypeMeta {
			t.Error("Page was not updated")
		}
	})

	t.Run("Eviction", func(t *testing.T) {
		maxSize := 5
		cache := NewPageCache(maxSize)

		// Fill cache to capacity
		for i := 0; i < maxSize; i++ {
			evicted := cache.Put(PageID(i+1), NewPage(PageTypeNode))
			if evicted {
				t.Error("Should not evict when cache has space")
			}
		}

		if cache.Size() != maxSize {
			t.Errorf("Expected size %d, got %d", maxSize, cache.Size())
		}

		// Add one more - should trigger eviction (simple clear all in this implementation)
		evicted := cache.Put(PageID(maxSize+1), NewPage(PageTypeNode))
		if !evicted {
			t.Error("Should return true when eviction occurs")
		}

		// After eviction, cache should be rebuilt with just the new page
		if cache.Size() != 1 {
			t.Errorf("Expected size 1 after eviction, got %d", cache.Size())
		}
	})

	t.Run("Remove", func(t *testing.T) {
		cache := NewPageCache(10)
		pid := PageID(1)

		cache.Put(pid, NewPage(PageTypeNode))
		if cache.Size() != 1 {
			t.Errorf("Expected size 1, got %d", cache.Size())
		}

		cache.Remove(pid)
		if cache.Size() != 0 {
			t.Errorf("Expected size 0 after remove, got %d", cache.Size())
		}

		_, ok := cache.Get(pid)
		if ok {
			t.Error("Page still in cache after remove")
		}
	})

	t.Run("RemoveNonExistent", func(t *testing.T) {
		cache := NewPageCache(10)
		cache.Remove(PageID(999)) // Should not panic
		if cache.Size() != 0 {
			t.Error("Size changed after removing non-existent page")
		}
	})

	t.Run("Clear", func(t *testing.T) {
		cache := NewPageCache(10)
		for i := 0; i < 5; i++ {
			cache.Put(PageID(i+1), NewPage(PageTypeNode))
		}

		cache.Clear()
		if cache.Size() != 0 {
			t.Errorf("Expected size 0 after clear, got %d", cache.Size())
		}

		for i := 0; i < 5; i++ {
			_, ok := cache.Get(PageID(i + 1))
			if ok {
				t.Errorf("Page %d still in cache after clear", i+1)
			}
		}
	})
}

func TestPageManager(t *testing.T) {
	t.Run("NewPageManager", func(t *testing.T) {
		startPage := PageID(10)
		pm := NewPageManager(startPage)
		if pm == nil {
			t.Fatal("NewPageManager returned nil")
		}
		if pm.TotalPages() != startPage {
			t.Errorf("Expected total pages %d, got %d", startPage, pm.TotalPages())
		}
		if pm.FreePages() != 0 {
			t.Errorf("Expected 0 free pages, got %d", pm.FreePages())
		}
	})

	t.Run("Allocate", func(t *testing.T) {
		pm := NewPageManager(PageID(1))
		pid := pm.Allocate()
		if pid != PageID(1) {
			t.Errorf("Expected first allocation to be page 1, got %d", pid)
		}

		pid2 := pm.Allocate()
		if pid2 != PageID(2) {
			t.Errorf("Expected second allocation to be page 2, got %d", pid2)
		}

		if pm.TotalPages() != PageID(3) {
			t.Errorf("Expected total pages 3, got %d", pm.TotalPages())
		}
	})

	t.Run("FreeAndReuse", func(t *testing.T) {
		pm := NewPageManager(PageID(1))

		// Allocate some pages
		_ = pm.Allocate()
		pid2 := pm.Allocate()
		pid3 := pm.Allocate()

		// Free the middle one
		pm.Free(pid2)
		if pm.FreePages() != 1 {
			t.Errorf("Expected 1 free page, got %d", pm.FreePages())
		}

		// Next allocation should reuse freed page
		reused := pm.Allocate()
		if reused != pid2 {
			t.Errorf("Expected to reuse page %d, got %d", pid2, reused)
		}
		if pm.FreePages() != 0 {
			t.Errorf("Expected 0 free pages after reuse, got %d", pm.FreePages())
		}

		// Next allocation should be a new page
		pid4 := pm.Allocate()
		if pid4 != pid3+1 {
			t.Errorf("Expected new page %d, got %d", pid3+1, pid4)
		}
	})

	t.Run("FreeInvalidPage", func(t *testing.T) {
		pm := NewPageManager(PageID(1))
		pm.Free(InvalidPageID) // Should not panic or add to free list
		if pm.FreePages() != 0 {
			t.Error("Invalid page was added to free list")
		}
	})

	t.Run("MultipleFreePages", func(t *testing.T) {
		pm := NewPageManager(PageID(1))

		// Allocate and free multiple pages
		pages := make([]PageID, 5)
		for i := range pages {
			pages[i] = pm.Allocate()
		}

		// Free them all
		for _, pid := range pages {
			pm.Free(pid)
		}

		if pm.FreePages() != 5 {
			t.Errorf("Expected 5 free pages, got %d", pm.FreePages())
		}

		// Allocate them back in LIFO order (stack)
		for i := len(pages) - 1; i >= 0; i-- {
			reused := pm.Allocate()
			if reused != pages[i] {
				t.Errorf("Expected to reuse page %d, got %d", pages[i], reused)
			}
		}

		if pm.FreePages() != 0 {
			t.Error("Free list should be empty")
		}
	})

	t.Run("MarshalUnmarshal", func(t *testing.T) {
		pm := NewPageManager(PageID(10))

		// Set up some state
		pm.Allocate()
		pm.Allocate()
		pm.Allocate()
		pm.Free(PageID(11))
		pm.Free(PageID(12))

		// Marshal
		data := pm.Marshal()
		if len(data) == 0 {
			t.Fatal("Marshal returned empty data")
		}

		// Unmarshal into new manager
		pm2 := NewPageManager(PageID(1))
		if err := pm2.Unmarshal(data); err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}

		// Verify state matches
		if pm2.TotalPages() != pm.TotalPages() {
			t.Errorf("Total pages mismatch: got %d, want %d", pm2.TotalPages(), pm.TotalPages())
		}
		if pm2.FreePages() != pm.FreePages() {
			t.Errorf("Free pages mismatch: got %d, want %d", pm2.FreePages(), pm.FreePages())
		}

		// Verify free list order is preserved
		freed1 := pm.Allocate()
		freed2 := pm2.Allocate()
		if freed1 != freed2 {
			t.Errorf("Free list order not preserved: got %d, want %d", freed2, freed1)
		}
	})

	t.Run("UnmarshalInvalidData", func(t *testing.T) {
		pm := NewPageManager(PageID(1))

		// Too short
		err := pm.Unmarshal([]byte{1, 2, 3})
		if err == nil {
			t.Error("Expected error for too-short data")
		}

		// Invalid free list length
		data := make([]byte, 12)
		data[8] = 0xFF // Very large free list length
		data[9] = 0xFF
		data[10] = 0xFF
		data[11] = 0xFF
		err = pm.Unmarshal(data)
		if err == nil {
			t.Error("Expected error for truncated free list")
		}
	})

	t.Run("EmptyFreeList", func(t *testing.T) {
		pm := NewPageManager(PageID(5))

		// Marshal with empty free list
		data := pm.Marshal()

		// Unmarshal
		pm2 := NewPageManager(PageID(1))
		if err := pm2.Unmarshal(data); err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}

		if pm2.FreePages() != 0 {
			t.Error("Free list should be empty")
		}
		if pm2.TotalPages() != PageID(5) {
			t.Errorf("Expected total pages 5, got %d", pm2.TotalPages())
		}
	})
}
