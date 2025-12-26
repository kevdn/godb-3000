package btree

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/khoale/godb-3000/internal/storage"
)

func setupTestBTree(t *testing.T) (*BTree, *storage.Pager, string) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	pager, err := storage.OpenPager(dbPath, storage.DefaultPagerOptions())
	if err != nil {
		t.Fatalf("Failed to open pager: %v", err)
	}

	// Set up PageManager
	pageMgr := storage.NewPageManager(storage.PageID(1))
	pager.SetPageManager(pageMgr)

	btree, err := NewBTree(pager)
	if err != nil {
		pager.Close()
		t.Fatalf("Failed to create btree: %v", err)
	}

	return btree, pager, dbPath
}

func TestBTreeCreation(t *testing.T) {
	t.Run("NewBTree", func(t *testing.T) {
		btree, pager, _ := setupTestBTree(t)
		defer pager.Close()

		if btree == nil {
			t.Fatal("NewBTree returned nil")
		}

		if !btree.RootID().IsValid() {
			t.Error("Root ID is invalid")
		}

		if btree.Depth() != 1 {
			t.Errorf("Expected depth 1, got %d", btree.Depth())
		}
	})

	t.Run("LoadBTree", func(t *testing.T) {
		btree1, pager, dbPath := setupTestBTree(t)
		rootID := btree1.RootID()
		pager.Flush()
		pager.Close()

		// Reopen
		pager2, _ := storage.OpenPager(dbPath, storage.DefaultPagerOptions())
		defer pager2.Close()

		btree2, err := LoadBTree(pager2, rootID)
		if err != nil {
			t.Fatalf("Failed to load btree: %v", err)
		}

		if btree2.RootID() != rootID {
			t.Errorf("Root ID mismatch: got %d, want %d", btree2.RootID(), rootID)
		}
	})
}

func TestBTreeInsertGet(t *testing.T) {
	btree, pager, _ := setupTestBTree(t)
	defer pager.Close()

	t.Run("InsertAndGet", func(t *testing.T) {
		key := []byte("test-key")
		value := []byte("test-value")

		if err := btree.Insert(key, value); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		retrieved, found, err := btree.Get(key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if !found {
			t.Error("Key not found after insert")
		}

		if !bytes.Equal(retrieved, value) {
			t.Errorf("Value mismatch: got %s, want %s", retrieved, value)
		}
	})

	t.Run("GetNonExistent", func(t *testing.T) {
		_, found, err := btree.Get([]byte("non-existent"))
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if found {
			t.Error("Found non-existent key")
		}
	})

	t.Run("UpdateExisting", func(t *testing.T) {
		key := []byte("update-key")
		value1 := []byte("value1")
		value2 := []byte("value2")

		btree.Insert(key, value1)
		btree.Insert(key, value2)

		retrieved, found, _ := btree.Get(key)
		if !found {
			t.Error("Key not found")
		}

		if !bytes.Equal(retrieved, value2) {
			t.Errorf("Value not updated: got %s, want %s", retrieved, value2)
		}
	})

	t.Run("EmptyKey", func(t *testing.T) {
		err := btree.Insert([]byte{}, []byte("value"))
		if err == nil {
			t.Error("Expected error for empty key")
		}

		_, _, err = btree.Get([]byte{})
		if err == nil {
			t.Error("Expected error for empty key")
		}
	})

	t.Run("LargeKey", func(t *testing.T) {
		largeKey := make([]byte, storage.MaxKeySize+1)
		err := btree.Insert(largeKey, []byte("value"))
		if err == nil {
			t.Error("Expected error for oversized key")
		}
	})

	t.Run("LargeValue", func(t *testing.T) {
		largeValue := make([]byte, storage.MaxValueSize+1)
		err := btree.Insert([]byte("key"), largeValue)
		if err == nil {
			t.Error("Expected error for oversized value")
		}
	})
}

func TestBTreeMultipleInserts(t *testing.T) {
	t.Run("SequentialInserts", func(t *testing.T) {
		btree, pager, _ := setupTestBTree(t)
		defer pager.Close()

		n := 100
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("key-%04d", i))
			value := []byte(fmt.Sprintf("value-%04d", i))

			if err := btree.Insert(key, value); err != nil {
				t.Fatalf("Insert %d failed: %v", i, err)
			}
		}

		// Verify all keys
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("key-%04d", i))
			expectedValue := []byte(fmt.Sprintf("value-%04d", i))

			retrieved, found, err := btree.Get(key)
			if err != nil {
				t.Fatalf("Get key %d failed: %v", i, err)
			}

			if !found {
				t.Errorf("Key %d not found", i)
			}

			if !bytes.Equal(retrieved, expectedValue) {
				t.Errorf("Value mismatch for key %d", i)
			}
		}
	})

	t.Run("ReverseInserts", func(t *testing.T) {
		btree, pager, _ := setupTestBTree(t)
		defer pager.Close()
		n := 50
		for i := n - 1; i >= 0; i-- {
			key := []byte(fmt.Sprintf("rev-%04d", i))
			value := []byte(fmt.Sprintf("val-%04d", i))

			if err := btree.Insert(key, value); err != nil {
				t.Fatalf("Insert %d failed: %v", i, err)
			}
		}

		// Verify
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("rev-%04d", i))
			_, found, _ := btree.Get(key)
			if !found {
				t.Errorf("Key %d not found", i)
			}
		}
	})
}

func TestBTreeDelete(t *testing.T) {
	btree, pager, _ := setupTestBTree(t)
	defer pager.Close()

	t.Run("DeleteExisting", func(t *testing.T) {
		key := []byte("delete-me")
		value := []byte("value")

		btree.Insert(key, value)

		deleted, err := btree.Delete(key)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		if !deleted {
			t.Error("Delete returned false for existing key")
		}

		_, found, _ := btree.Get(key)
		if found {
			t.Error("Key still exists after deletion")
		}
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		deleted, err := btree.Delete([]byte("does-not-exist"))
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		if deleted {
			t.Error("Delete returned true for non-existent key")
		}
	})

	t.Run("DeleteEmptyKey", func(t *testing.T) {
		_, err := btree.Delete([]byte{})
		if err == nil {
			t.Error("Expected error for empty key")
		}
	})

	t.Run("DeleteAllKeysFromTree", func(t *testing.T) {
		// Insert a few keys
		keys := []string{"a", "b", "c"}
		for _, k := range keys {
			btree.Insert([]byte(k), []byte("value"))
		}

		// Delete all keys
		for _, k := range keys {
			deleted, err := btree.Delete([]byte(k))
			if err != nil {
				t.Fatalf("Delete failed: %v", err)
			}
			if !deleted {
				t.Errorf("Failed to delete key %s", k)
			}
		}

		// Tree should still be valid (empty root leaf is allowed)
		stats, err := btree.Stats()
		if err != nil {
			t.Fatalf("Stats failed: %v", err)
		}
		if stats.NumKeys != 0 {
			t.Errorf("Expected 0 keys after deleting all, got %d", stats.NumKeys)
		}
		if stats.Depth != 1 {
			t.Errorf("Expected depth 1 after deleting all, got %d", stats.Depth)
		}
	})

	t.Run("IteratorOnEmptyTree", func(t *testing.T) {
		// Create iterator on empty tree
		iter, err := btree.NewIterator(nil, nil)
		if err != nil {
			t.Fatalf("NewIterator failed: %v", err)
		}
		defer iter.Close()

		// Should return false immediately
		_, _, ok := iter.Next()
		if ok {
			t.Error("Iterator should return false on empty tree")
		}
	})
}

func TestBTreePageFreeing(t *testing.T) {
	btree, pager, _ := setupTestBTree(t)
	defer pager.Close()

	// Get the page manager from pager (we need to access it through reflection or add a getter)
	// For now, we'll test indirectly by checking stats

	t.Run("RootShrinkFreesPage", func(t *testing.T) {
		// Insert enough data to cause a split
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key-%04d", i))
			value := []byte(fmt.Sprintf("value-%04d", i))
			btree.Insert(key, value)
		}

		initialDepth := btree.Depth()
		if initialDepth <= 1 {
			t.Skip("Tree didn't split, can't test page freeing")
		}

		// Delete all keys to potentially trigger root shrinking
		for i := 0; i < 100; i++ {
			key := []byte(fmt.Sprintf("key-%04d", i))
			btree.Delete(key)
		}

		// After deleting all, depth might decrease if root becomes empty
		// This is a basic test - full rebalancing is more complex
		if btree.Depth() > initialDepth {
			t.Error("Depth increased after deleting all keys")
		}
	})
}

func TestBTreeScan(t *testing.T) {
	btree, pager, _ := setupTestBTree(t)
	defer pager.Close()

	// Insert test data
	keys := []string{"apple", "banana", "cherry", "date", "elderberry", "fig", "grape"}
	for _, k := range keys {
		btree.Insert([]byte(k), []byte("value-"+k))
	}

	t.Run("FullScan", func(t *testing.T) {
		var results []string
		err := btree.Scan(nil, nil, func(key, value []byte) bool {
			results = append(results, string(key))
			return true
		})
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		if len(results) != len(keys) {
			t.Errorf("Expected %d results, got %d", len(keys), len(results))
		}

		// Results should be in sorted order
		for i := 0; i < len(results); i++ {
			if results[i] != keys[i] {
				t.Errorf("Result %d: got %s, want %s", i, results[i], keys[i])
			}
		}
	})

	t.Run("RangeScan", func(t *testing.T) {
		var results []string
		start := []byte("banana")
		end := []byte("fig")

		err := btree.Scan(start, end, func(key, value []byte) bool {
			results = append(results, string(key))
			return true
		})
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		expected := []string{"banana", "cherry", "date", "elderberry"}
		if len(results) != len(expected) {
			t.Errorf("Expected %d results, got %d", len(expected), len(results))
		}

		for i, exp := range expected {
			if i >= len(results) || results[i] != exp {
				t.Errorf("Result %d: got %s, want %s", i, results[i], exp)
			}
		}
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		var results []string
		err := btree.Scan(nil, nil, func(key, value []byte) bool {
			results = append(results, string(key))
			return len(results) < 3 // Stop after 3 items
		})
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		if len(results) != 3 {
			t.Errorf("Expected early termination at 3 items, got %d", len(results))
		}
	})
}

func TestBTreeIterator(t *testing.T) {
	btree, pager, _ := setupTestBTree(t)
	defer pager.Close()

	// Insert test data
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key-%02d", i))
		value := []byte(fmt.Sprintf("val-%02d", i))
		btree.Insert(key, value)
	}

	t.Run("IterateAll", func(t *testing.T) {
		iter, err := btree.NewIterator(nil, nil)
		if err != nil {
			t.Fatalf("NewIterator failed: %v", err)
		}
		defer iter.Close()

		count := 0
		for {
			key, value, ok := iter.Next()
			if !ok {
				break
			}

			if key == nil || value == nil {
				t.Error("Iterator returned nil key or value")
			}

			count++
		}

		if count != 10 {
			t.Errorf("Expected 10 items, got %d", count)
		}
	})

	t.Run("IterateRange", func(t *testing.T) {
		start := []byte("key-03")
		end := []byte("key-07")

		iter, err := btree.NewIterator(start, end)
		if err != nil {
			t.Fatalf("NewIterator failed: %v", err)
		}
		defer iter.Close()

		count := 0
		for {
			_, _, ok := iter.Next()
			if !ok {
				break
			}
			count++
		}

		// Should get keys 03, 04, 05, 06 (end is exclusive)
		if count != 4 {
			t.Errorf("Expected 4 items in range, got %d", count)
		}
	})
}

func TestBTreeStats(t *testing.T) {
	btree, pager, _ := setupTestBTree(t)
	defer pager.Close()

	t.Run("EmptyTree", func(t *testing.T) {
		stats, err := btree.Stats()
		if err != nil {
			t.Fatalf("Stats failed: %v", err)
		}

		if stats.NumKeys != 0 {
			t.Errorf("Expected 0 keys, got %d", stats.NumKeys)
		}

		if stats.Depth != 1 {
			t.Errorf("Expected depth 1, got %d", stats.Depth)
		}

		if stats.NumLeaves != 1 {
			t.Errorf("Expected 1 leaf, got %d", stats.NumLeaves)
		}
	})

	t.Run("PopulatedTree", func(t *testing.T) {
		n := 50
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("key-%04d", i))
			value := []byte(fmt.Sprintf("value-%04d", i))
			btree.Insert(key, value)
		}

		stats, err := btree.Stats()
		if err != nil {
			t.Fatalf("Stats failed: %v", err)
		}

		if stats.NumKeys != n {
			t.Errorf("Expected %d keys, got %d", n, stats.NumKeys)
		}

		if stats.NumNodes < 1 {
			t.Error("Expected at least 1 node")
		}

		if stats.NumLeaves < 1 {
			t.Error("Expected at least 1 leaf")
		}

		if stats.AvgKeySize <= 0 {
			t.Error("Average key size should be positive")
		}

		if stats.AvgValueSize <= 0 {
			t.Error("Average value size should be positive")
		}
	})
}

func TestPageTypeValidation(t *testing.T) {
	t.Run("DeserializeRejectsWrongPageType", func(t *testing.T) {
		// Create a page with wrong type (PageTypeMeta instead of PageTypeNode)
		wrongPage := storage.NewPage(storage.PageTypeMeta)
		
		// Try to deserialize - should fail with validation error
		_, err := Deserialize(wrongPage)
		if err == nil {
			t.Fatal("Expected error when deserializing page with wrong type, got nil")
		}
		
		// Check error message contains expected information
		if err.Error() == "" {
			t.Error("Error message should not be empty")
		}
		expectedError := "corrupted page"
		if !bytes.Contains([]byte(err.Error()), []byte(expectedError)) {
			t.Errorf("Expected error message to contain %q, got %q", expectedError, err.Error())
		}
	})

	t.Run("DeserializeAcceptsCorrectPageType", func(t *testing.T) {
		// Create a valid node page
		node := NewLeafNode()
		page, err := node.Serialize()
		if err != nil {
			t.Fatalf("Failed to serialize node: %v", err)
		}
		
		// Deserialize should succeed
		deserialized, err := Deserialize(page)
		if err != nil {
			t.Fatalf("Deserialize should succeed for correct page type, got error: %v", err)
		}
		
		if deserialized == nil {
			t.Fatal("Deserialized node should not be nil")
		}
	})
}

func TestBTreePersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "persist.db")

	// Create and populate tree
	pager1, _ := storage.OpenPager(dbPath, storage.DefaultPagerOptions())
	pageMgr := storage.NewPageManager(storage.PageID(1))
	pager1.SetPageManager(pageMgr)

	btree1, _ := NewBTree(pager1)
	rootID := btree1.RootID()

	// Insert data
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("persist-%02d", i))
		value := []byte(fmt.Sprintf("data-%02d", i))
		btree1.Insert(key, value)
	}

	pager1.Flush()
	pager1.Close()

	// Reopen and verify
	pager2, err := storage.OpenPager(dbPath, storage.DefaultPagerOptions())
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer pager2.Close()

	btree2, err := LoadBTree(pager2, rootID)
	if err != nil {
		t.Fatalf("Failed to load btree: %v", err)
	}

	// Verify all data
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("persist-%02d", i))
		expectedValue := []byte(fmt.Sprintf("data-%02d", i))

		value, found, err := btree2.Get(key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if !found {
			t.Errorf("Key %d not found after reload", i)
		}

		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Value mismatch for key %d", i)
		}
	}
}
