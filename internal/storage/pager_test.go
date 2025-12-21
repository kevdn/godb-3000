package storage

import (
	"os"
	"path/filepath"
	"testing"
)

// setupTestPager creates a pager with PageManager configured for testing
func setupTestPager(t *testing.T, dbPath string) *Pager {
	pager, err := OpenPager(dbPath, DefaultPagerOptions())
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	// Set up PageManager starting from page 1 (page 0 is metadata)
	pageMgr := NewPageManager(PageID(1))
	pager.SetPageManager(pageMgr)

	return pager
}

func TestPager(t *testing.T) {
	t.Run("OpenNewFile", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		pager, err := OpenPager(dbPath, DefaultPagerOptions())
		if err != nil {
			t.Fatalf("Failed to open pager: %v", err)
		}
		defer pager.Close()

		// New file should have 1 page (metadata)
		if pager.NumPages() != 1 {
			t.Errorf("Expected 1 page, got %d", pager.NumPages())
		}

		// Verify file exists
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			t.Error("Database file was not created")
		}
	})

	t.Run("OpenExistingFile", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		// Create and close pager
		pager1 := setupTestPager(t, dbPath)
		pager1.AllocatePage()
		pager1.Close()

		// Reopen and verify pages persisted
		pager2 := setupTestPager(t, dbPath)
		defer pager2.Close()

		if pager2.NumPages() != 2 {
			t.Errorf("Expected 2 pages after reopen, got %d", pager2.NumPages())
		}
	})

	t.Run("ReadOnlyMode", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		// Create file first
		pager1, _ := OpenPager(dbPath, DefaultPagerOptions())
		pager1.Close()

		// Open in read-only mode
		opts := DefaultPagerOptions()
		opts.ReadOnly = true
		pager2, err := OpenPager(dbPath, opts)
		if err != nil {
			t.Fatalf("Failed to open in read-only mode: %v", err)
		}
		defer pager2.Close()

		if pager2.NumPages() != 1 {
			t.Errorf("Expected 1 page, got %d", pager2.NumPages())
		}
	})

	t.Run("InvalidFileSize", func(t *testing.T) {
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "invalid.db")

		// Create file with invalid size (not multiple of PageSize)
		file, _ := os.Create(dbPath)
		file.Write(make([]byte, 100)) // 100 bytes, not a multiple of 4096
		file.Close()

		_, err := OpenPager(dbPath, DefaultPagerOptions())
		if err == nil {
			t.Error("Expected error for invalid file size")
		}
	})
}

func TestPagerReadWrite(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	pager := setupTestPager(t, dbPath)
	defer pager.Close()

	t.Run("AllocateAndWrite", func(t *testing.T) {
		pid, err := pager.AllocatePage()
		if err != nil {
			t.Fatalf("AllocatePage failed: %v", err)
		}

		if !pid.IsValid() {
			t.Error("Allocated invalid page ID")
		}

		page := NewPage(PageTypeNode)
		payload := page.Payload()
		copy(payload, []byte("test data"))

		if err := pager.WritePage(pid, page); err != nil {
			t.Fatalf("WritePage failed: %v", err)
		}
	})

	t.Run("ReadWrittenPage", func(t *testing.T) {
		pid, _ := pager.AllocatePage()

		// Write data
		page := NewPage(PageTypeMeta)
		payload := page.Payload()
		testData := []byte("hello world")
		copy(payload, testData)
		pager.WritePage(pid, page)

		// Read it back
		readPage, err := pager.ReadPage(pid)
		if err != nil {
			t.Fatalf("ReadPage failed: %v", err)
		}

		readPayload := readPage.Payload()
		if string(readPayload[:len(testData)]) != string(testData) {
			t.Error("Read data doesn't match written data")
		}

		if readPage.Type() != PageTypeMeta {
			t.Errorf("Expected type %d, got %d", PageTypeMeta, readPage.Type())
		}
	})

	t.Run("ReadNonExistentPage", func(t *testing.T) {
		_, err := pager.ReadPage(PageID(9999))
		if err == nil {
			t.Error("Expected error when reading non-existent page")
		}
	})
}

func TestPagerCache(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	opts := DefaultPagerOptions()
	opts.CacheSize = 5
	pager, err := OpenPager(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}
	defer pager.Close()

	// Set up PageManager
	pageMgr := NewPageManager(PageID(1))
	pager.SetPageManager(pageMgr)

	t.Run("CacheHit", func(t *testing.T) {
		pid, _ := pager.AllocatePage()

		page := NewPage(PageTypeNode)
		payload := page.Payload()
		copy(payload, []byte("cached data"))
		pager.WritePage(pid, page)

		// First read loads into cache
		_, err := pager.ReadPage(pid)
		if err != nil {
			t.Fatalf("First read failed: %v", err)
		}

		// Second read should hit cache
		page2, err := pager.ReadPage(pid)
		if err != nil {
			t.Fatalf("Second read failed: %v", err)
		}

		if string(page2.Payload()[:11]) != "cached data" {
			t.Error("Cached data doesn't match")
		}
	})

	t.Run("DirtyPageRead", func(t *testing.T) {
		pid, _ := pager.AllocatePage()

		page := NewPage(PageTypeNode)
		payload := page.Payload()
		copy(payload, []byte("dirty data"))
		pager.WritePage(pid, page)

		// Read before flush should return dirty page
		readPage, err := pager.ReadPage(pid)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}

		if string(readPage.Payload()[:10]) != "dirty data" {
			t.Error("Should read dirty page data")
		}
	})
}

func TestPagerFlush(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	t.Run("FlushDirtyPages", func(t *testing.T) {
		pager := setupTestPager(t, dbPath)

		pid, _ := pager.AllocatePage()
		page := NewPage(PageTypeNode)
		payload := page.Payload()
		copy(payload, []byte("flush test"))
		pager.WritePage(pid, page)

		if pager.DirtyPages() != 1 {
			t.Errorf("Expected 1 dirty page, got %d", pager.DirtyPages())
		}

		if err := pager.Flush(); err != nil {
			t.Fatalf("Flush failed: %v", err)
		}

		if pager.DirtyPages() != 0 {
			t.Errorf("Expected 0 dirty pages after flush, got %d", pager.DirtyPages())
		}

		pager.Close()

		// Reopen and verify data persisted
		pager2, _ := OpenPager(dbPath, DefaultPagerOptions())
		defer pager2.Close()

		readPage, err := pager2.ReadPage(pid)
		if err != nil {
			t.Fatalf("Failed to read after reopen: %v", err)
		}

		if string(readPage.Payload()[:10]) != "flush test" {
			t.Error("Flushed data was not persisted")
		}
	})

	t.Run("FlushEmpty", func(t *testing.T) {
		pager := setupTestPager(t, dbPath)
		defer pager.Close()

		// Flushing with no dirty pages should not error
		if err := pager.Flush(); err != nil {
			t.Errorf("Flush failed: %v", err)
		}
	})
}

func TestPagerPageManager(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	t.Run("AllocateWithoutPageManager", func(t *testing.T) {
		// Create pager WITHOUT PageManager setup
		tmpPath := filepath.Join(tmpDir, "no-pm.db")
		pagerNoPM, err := OpenPager(tmpPath, DefaultPagerOptions())
		if err != nil {
			t.Fatalf("Failed to open pager: %v", err)
		}
		defer pagerNoPM.Close()

		// Should return error when PageManager not set
		_, err = pagerNoPM.AllocatePage()
		if err == nil {
			t.Error("Expected error when PageManager not set")
		}
	})

	pager := setupTestPager(t, dbPath)
	defer pager.Close()

	t.Run("AllocateWithPageManager", func(t *testing.T) {
		pm := NewPageManager(PageID(10))
		pager.SetPageManager(pm)

		pid1, _ := pager.AllocatePage()
		pid2, _ := pager.AllocatePage()

		if pid1 != PageID(10) {
			t.Errorf("Expected allocation at page 10, got %d", pid1)
		}
		if pid2 != PageID(11) {
			t.Errorf("Expected allocation at page 11, got %d", pid2)
		}

		if pm.TotalPages() != PageID(12) {
			t.Errorf("Expected PageManager total pages 12, got %d", pm.TotalPages())
		}
	})

	t.Run("FreeAndReusePages", func(t *testing.T) {
		pm := NewPageManager(PageID(20))
		pager.SetPageManager(pm)

		// Allocate some pages
		_, _ = pager.AllocatePage()
		pid2, _ := pager.AllocatePage()
		_, _ = pager.AllocatePage()

		// Free middle page
		if err := pager.FreePage(pid2); err != nil {
			t.Fatalf("FreePage failed: %v", err)
		}

		if pm.FreePages() != 1 {
			t.Errorf("Expected 1 free page, got %d", pm.FreePages())
		}

		// Next allocation should reuse freed page
		reused, _ := pager.AllocatePage()
		if reused != pid2 {
			t.Errorf("Expected to reuse page %d, got %d", pid2, reused)
		}

		if pm.FreePages() != 0 {
			t.Errorf("Expected 0 free pages after reuse, got %d", pm.FreePages())
		}
	})

	t.Run("FreeInvalidPage", func(t *testing.T) {
		err := pager.FreePage(InvalidPageID)
		if err == nil {
			t.Error("Expected error when freeing invalid page")
		}
	})

	t.Run("FreeOutOfRangePage", func(t *testing.T) {
		err := pager.FreePage(PageID(9999))
		if err == nil {
			t.Error("Expected error when freeing out-of-range page")
		}
	})

	t.Run("FreeWithoutPageManager", func(t *testing.T) {
		// Create new pager without PageManager
		dbPath2 := filepath.Join(tmpDir, "no-pm-free.db")
		pager2, err := OpenPager(dbPath2, DefaultPagerOptions())
		if err != nil {
			t.Fatalf("Failed to open pager: %v", err)
		}
		defer pager2.Close()

		// FreePage without PageManager should return error
		err = pager2.FreePage(PageID(1))
		if err == nil {
			t.Error("Expected error when PageManager not set")
		}
	})
}

func TestPagerStats(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	pager := setupTestPager(t, dbPath)
	defer pager.Close()

	// Allocate and write some pages
	for i := 0; i < 5; i++ {
		pid, _ := pager.AllocatePage()
		page := NewPage(PageTypeNode)
		pager.WritePage(pid, page)
	}

	stats := pager.Stats()

	if stats.NumPages != 6 { // 1 metadata + 5 allocated
		t.Errorf("Expected 6 pages, got %d", stats.NumPages)
	}

	if stats.DirtyPages != 5 {
		t.Errorf("Expected 5 dirty pages, got %d", stats.DirtyPages)
	}

	if stats.FileSize != int64(stats.NumPages)*PageSize {
		t.Errorf("File size mismatch: expected %d, got %d", int64(stats.NumPages)*PageSize, stats.FileSize)
	}

	// Flush and check again
	pager.Flush()
	stats2 := pager.Stats()

	if stats2.DirtyPages != 0 {
		t.Errorf("Expected 0 dirty pages after flush, got %d", stats2.DirtyPages)
	}
}

func TestPagerBackup(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")
	backupPath := filepath.Join(tmpDir, "backup.db")

	pager := setupTestPager(t, dbPath)

	// Write some data
	pid, _ := pager.AllocatePage()
	page := NewPage(PageTypeNode)
	payload := page.Payload()
	copy(payload, []byte("backup test"))
	pager.WritePage(pid, page)
	pager.Flush()

	// Create backup
	if err := pager.CopyTo(backupPath); err != nil {
		t.Fatalf("Backup failed: %v", err)
	}

	pager.Close()

	// Verify backup file exists
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		t.Fatal("Backup file was not created")
	}

	// Open backup and verify data
	backupPager := setupTestPager(t, backupPath)
	defer backupPager.Close()

	if backupPager.NumPages() != 2 {
		t.Errorf("Backup has wrong number of pages: %d", backupPager.NumPages())
	}

	readPage, err := backupPager.ReadPage(pid)
	if err != nil {
		t.Fatalf("Failed to read from backup: %v", err)
	}

	if string(readPage.Payload()[:11]) != "backup test" {
		t.Error("Backup data doesn't match original")
	}
}

func TestPagerClose(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	t.Run("CloseFlushesData", func(t *testing.T) {
		pager := setupTestPager(t, dbPath)

		pid, _ := pager.AllocatePage()
		page := NewPage(PageTypeNode)
		payload := page.Payload()
		copy(payload, []byte("close test"))
		pager.WritePage(pid, page)

		// Close should flush dirty pages
		if err := pager.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}

		// Reopen and verify data was persisted
		pager2 := setupTestPager(t, dbPath)
		defer pager2.Close()

		readPage, err := pager2.ReadPage(pid)
		if err != nil {
			t.Fatalf("Failed to read after close: %v", err)
		}

		if string(readPage.Payload()[:10]) != "close test" {
			t.Error("Data was not persisted on close")
		}
	})
}

func TestPagerConcurrency(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	pager := setupTestPager(t, dbPath)
	defer pager.Close()

	// Allocate a page to read concurrently
	pid, _ := pager.AllocatePage()
	page := NewPage(PageTypeNode)
	pager.WritePage(pid, page)
	pager.Flush()

	t.Run("ConcurrentReads", func(t *testing.T) {
		done := make(chan bool)

		for i := 0; i < 10; i++ {
			go func() {
				_, err := pager.ReadPage(pid)
				if err != nil {
					t.Errorf("Concurrent read failed: %v", err)
				}
				done <- true
			}()
		}

		for i := 0; i < 10; i++ {
			<-done
		}
	})
}
