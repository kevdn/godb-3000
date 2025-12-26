package kv

import (
	"bytes"
	"path/filepath"
	"testing"
)

func setupTestKV(t *testing.T) (*KV, string) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	opts := DefaultOptions()
	opts.DisableWAL = true // Disable WAL for simpler unit tests
	store, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open KV store: %v", err)
	}

	return store, dbPath
}

func TestKVBasicOperations(t *testing.T) {
	store, _ := setupTestKV(t)
	defer store.Close()

	t.Run("SetAndGet", func(t *testing.T) {
		key := []byte("test-key")
		value := []byte("test-value")

		if err := store.Set(key, value); err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		retrieved, found, err := store.Get(key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !found {
			t.Error("Key not found after Set")
		}
		if !bytes.Equal(retrieved, value) {
			t.Errorf("Value mismatch: got %s, want %s", retrieved, value)
		}
	})

	t.Run("GetNonExistent", func(t *testing.T) {
		_, found, err := store.Get([]byte("non-existent"))
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

		store.Set(key, value1)
		store.Set(key, value2)

		retrieved, found, _ := store.Get(key)
		if !found {
			t.Error("Key not found")
		}
		if !bytes.Equal(retrieved, value2) {
			t.Errorf("Value not updated: got %s, want %s", retrieved, value2)
		}
	})

	t.Run("DeleteExisting", func(t *testing.T) {
		key := []byte("delete-me")
		value := []byte("value")

		store.Set(key, value)
		deleted, err := store.Delete(key)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		if !deleted {
			t.Error("Delete returned false for existing key")
		}

		_, found, _ := store.Get(key)
		if found {
			t.Error("Key still exists after deletion")
		}
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		deleted, err := store.Delete([]byte("does-not-exist"))
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		if deleted {
			t.Error("Delete returned true for non-existent key")
		}
	})

	t.Run("EmptyKey", func(t *testing.T) {
		err := store.Set([]byte{}, []byte("value"))
		if err == nil {
			t.Error("Expected error for empty key in Set")
		}

		_, _, err = store.Get([]byte{})
		if err == nil {
			t.Error("Expected error for empty key in Get")
		}

		_, err = store.Delete([]byte{})
		if err == nil {
			t.Error("Expected error for empty key in Delete")
		}
	})
}

func TestKVScan(t *testing.T) {
	store, _ := setupTestKV(t)
	defer store.Close()

	// Insert test data
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, k := range keys {
		store.Set([]byte(k), []byte("value-"+k))
	}

	t.Run("FullScan", func(t *testing.T) {
		var results []string
		err := store.Scan(nil, nil, func(key, value []byte) bool {
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
		for i, k := range keys {
			if i >= len(results) || results[i] != k {
				t.Errorf("Result %d: got %s, want %s", i, results[i], k)
			}
		}
	})

	t.Run("RangeScan", func(t *testing.T) {
		var results []string
		start := []byte("banana")
		end := []byte("date")

		err := store.Scan(start, end, func(key, value []byte) bool {
			results = append(results, string(key))
			return true
		})
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		expected := []string{"banana", "cherry"}
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
		count := 0
		err := store.Scan(nil, nil, func(key, value []byte) bool {
			count++
			return count < 2 // Stop after 1 item (return false on 2nd call)
		})
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		if count != 2 {
			t.Errorf("Expected early termination at 2 items, got %d", count)
		}
	})
}

func TestKVTransactions(t *testing.T) {
	store, _ := setupTestKV(t)
	defer store.Close()

	t.Run("BeginCommit", func(t *testing.T) {
		if err := store.Begin(); err != nil {
			t.Fatalf("Begin failed: %v", err)
		}

		key := []byte("txn-key")
		value := []byte("txn-value")
		if err := store.Set(key, value); err != nil {
			t.Fatalf("Set in transaction failed: %v", err)
		}

		// Value should be visible before commit
		retrieved, found, _ := store.Get(key)
		if !found {
			t.Error("Key not found in transaction")
		}
		if !bytes.Equal(retrieved, value) {
			t.Error("Value mismatch in transaction")
		}

		if err := store.Commit(); err != nil {
			t.Fatalf("Commit failed: %v", err)
		}

		// Value should still be visible after commit
		retrieved, found, _ = store.Get(key)
		if !found {
			t.Error("Key not found after commit")
		}
		if !bytes.Equal(retrieved, value) {
			t.Error("Value mismatch after commit")
		}
	})

	t.Run("BeginRollback", func(t *testing.T) {
		// Set initial value
		key := []byte("rollback-key")
		initialValue := []byte("initial")
		store.Set(key, initialValue)

		// Begin transaction and update
		if err := store.Begin(); err != nil {
			t.Fatalf("Begin failed: %v", err)
		}

		newValue := []byte("updated")
		store.Set(key, newValue)

		// Value should be updated in transaction
		retrieved, found, _ := store.Get(key)
		if !found {
			t.Error("Key not found in transaction")
		}
		if !bytes.Equal(retrieved, newValue) {
			t.Error("Value not updated in transaction")
		}

		// Rollback
		if err := store.Rollback(); err != nil {
			t.Fatalf("Rollback failed: %v", err)
		}

		// Value should be reverted
		retrieved, found, _ = store.Get(key)
		if !found {
			t.Error("Key not found after rollback")
		}
		if !bytes.Equal(retrieved, initialValue) {
			t.Errorf("Value not reverted: got %s, want %s", retrieved, initialValue)
		}
	})

	t.Run("RollbackDelete", func(t *testing.T) {
		key := []byte("rollback-delete")
		value := []byte("value")
		store.Set(key, value)

		store.Begin()
		store.Delete(key)

		// Key should be deleted in transaction
		_, found, _ := store.Get(key)
		if found {
			t.Error("Key still exists in transaction after delete")
		}

		store.Rollback()

		// Key should be restored after rollback
		retrieved, found, _ := store.Get(key)
		if !found {
			t.Error("Key not restored after rollback")
		}
		if !bytes.Equal(retrieved, value) {
			t.Error("Value mismatch after rollback")
		}
	})

	t.Run("NestedBegin", func(t *testing.T) {
		store.Begin()
		err := store.Begin()
		if err == nil {
			t.Error("Expected error for nested transaction")
		}
		store.Rollback()
	})

	t.Run("CommitWithoutBegin", func(t *testing.T) {
		err := store.Commit()
		if err == nil {
			t.Error("Expected error for commit without transaction")
		}
	})

	t.Run("RollbackWithoutBegin", func(t *testing.T) {
		err := store.Rollback()
		if err == nil {
			t.Error("Expected error for rollback without transaction")
		}
	})
}

func TestKVPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "persist.db")

	// Create and populate store
	opts := DefaultOptions()
	opts.DisableWAL = true
	store1, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}

	// Insert data
	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i)}
		value := []byte{byte('0' + i)}
		store1.Set(key, value)
	}

	store1.Sync()
	store1.Close()

	// Reopen and verify
	store2, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to reopen store: %v", err)
	}
	defer store2.Close()

	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i)}
		expectedValue := []byte{byte('0' + i)}
		value, found, err := store2.Get(key)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if !found {
			t.Errorf("Key %c not found after reopen", 'a'+i)
		}
		if !bytes.Equal(value, expectedValue) {
			t.Errorf("Value mismatch for key %c", 'a'+i)
		}
	}
}

func TestKVStats(t *testing.T) {
	store, _ := setupTestKV(t)
	defer store.Close()

	// Insert some data
	for i := 0; i < 5; i++ {
		key := []byte{byte('a' + i)}
		store.Set(key, []byte("value"))
	}

	stats, err := store.Stats()
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	if stats.BTreeStats == nil {
		t.Error("BTreeStats should not be nil")
	}
	if stats.BTreeStats.NumKeys != 5 {
		t.Errorf("Expected 5 keys, got %d", stats.BTreeStats.NumKeys)
	}
}

func TestKVCount(t *testing.T) {
	store, _ := setupTestKV(t)
	defer store.Close()

	count, err := store.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 keys, got %d", count)
	}

	// Insert some data
	for i := 0; i < 10; i++ {
		store.Set([]byte{byte('a' + i)}, []byte("value"))
	}

	count, err = store.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 10 {
		t.Errorf("Expected 10 keys, got %d", count)
	}
}

func TestKVReadOnly(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "readonly.db")

	// Create database first
	opts1 := DefaultOptions()
	opts1.DisableWAL = true
	store1, err := Open(dbPath, opts1)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	store1.Set([]byte("key"), []byte("value"))
	store1.Close()

	// Open in read-only mode
	opts2 := DefaultOptions()
	opts2.ReadOnly = true
	opts2.DisableWAL = true
	store2, err := Open(dbPath, opts2)
	if err != nil {
		t.Fatalf("Failed to open in read-only mode: %v", err)
	}
	defer store2.Close()

	// Should be able to read
	value, found, err := store2.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found {
		t.Error("Key not found in read-only mode")
	}
	if !bytes.Equal(value, []byte("value")) {
		t.Error("Value mismatch in read-only mode")
	}

	// Should not be able to write
	err = store2.Set([]byte("new-key"), []byte("new-value"))
	if err == nil {
		t.Error("Expected error when writing in read-only mode")
	}
}

func TestKVNewDatabase(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "new.db")

	opts := DefaultOptions()
	opts.DisableWAL = true
	store, err := Open(dbPath, opts)
	if err != nil {
		t.Fatalf("Failed to create new database: %v", err)
	}
	defer store.Close()

	// Should be able to write
	if err := store.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
}

func TestKVNewDatabaseReadOnly(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "new-readonly.db")

	opts := DefaultOptions()
	opts.ReadOnly = true
	opts.DisableWAL = true
	_, err := Open(dbPath, opts)
	if err == nil {
		t.Error("Expected error when creating new database in read-only mode")
	}
}

func TestKVClose(t *testing.T) {
	store, _ := setupTestKV(t)

	// Set some data
	store.Set([]byte("key"), []byte("value"))

	// Close should succeed
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestKVCloseWithActiveTransaction(t *testing.T) {
	store, _ := setupTestKV(t)

	store.Begin()
	store.Set([]byte("key"), []byte("value"))

	// Close should handle active transaction gracefully
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

