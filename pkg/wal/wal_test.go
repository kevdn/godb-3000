package wal

import (
	"os"
	"path/filepath"
	"testing"
)

// bytesEqual compares two byte slices, treating nil and empty slice as equal
func bytesEqual(a, b []byte) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func setupTestWAL(t *testing.T) (*WAL, string) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := Open(walPath, DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	return wal, walPath
}

func TestRecordMarshalUnmarshal(t *testing.T) {
	tests := []struct {
		name   string
		record *Record
	}{
		{
			name: "Insert record",
			record: &Record{
				LSN:        1,
				TxnID:      100,
				RecordType: RecordTypeInsert,
				Key:        []byte("key1"),
				Value:      []byte("value1"),
				OldValue:   nil,
			},
		},
		{
			name: "Delete record with old value",
			record: &Record{
				LSN:        2,
				TxnID:      100,
				RecordType: RecordTypeDelete,
				Key:        []byte("key1"),
				Value:      nil,
				OldValue:   []byte("oldvalue"),
			},
		},
		{
			name: "Update record (insert with old value)",
			record: &Record{
				LSN:        3,
				TxnID:      200,
				RecordType: RecordTypeInsert,
				Key:        []byte("key2"),
				Value:      []byte("newvalue"),
				OldValue:   []byte("oldvalue"),
			},
		},
		{
			name: "Commit record",
			record: &Record{
				LSN:        4,
				TxnID:      100,
				RecordType: RecordTypeCommit,
				Key:        nil,
				Value:      nil,
				OldValue:   nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal
			data := tt.record.Marshal()

			// Unmarshal
			unmarshaled, size, err := UnmarshalRecord(data)
			if err != nil {
				t.Fatalf("Failed to unmarshal: %v", err)
			}

			if size != len(data) {
				t.Errorf("Size mismatch: expected %d, got %d", len(data), size)
			}

			// Compare fields
			if unmarshaled.LSN != tt.record.LSN {
				t.Errorf("LSN mismatch: expected %d, got %d", tt.record.LSN, unmarshaled.LSN)
			}
			if unmarshaled.TxnID != tt.record.TxnID {
				t.Errorf("TxnID mismatch: expected %d, got %d", tt.record.TxnID, unmarshaled.TxnID)
			}
			if unmarshaled.RecordType != tt.record.RecordType {
				t.Errorf("RecordType mismatch: expected %d, got %d", tt.record.RecordType, unmarshaled.RecordType)
			}
			// Compare slices (handle nil vs empty slice)
			if !bytesEqual(unmarshaled.Key, tt.record.Key) {
				t.Errorf("Key mismatch: expected %v, got %v", tt.record.Key, unmarshaled.Key)
			}
			if !bytesEqual(unmarshaled.Value, tt.record.Value) {
				t.Errorf("Value mismatch: expected %v, got %v", tt.record.Value, unmarshaled.Value)
			}
			if !bytesEqual(unmarshaled.OldValue, tt.record.OldValue) {
				t.Errorf("OldValue mismatch: expected %v, got %v", tt.record.OldValue, unmarshaled.OldValue)
			}
		})
	}
}

func TestRecordCRCValidation(t *testing.T) {
	record := &Record{
		LSN:        1,
		TxnID:      100,
		RecordType: RecordTypeInsert,
		Key:        []byte("key"),
		Value:      []byte("value"),
	}

	data := record.Marshal()

	// Corrupt the data
	data[10] ^= 0xFF

	_, _, err := UnmarshalRecord(data)
	if err == nil {
		t.Error("Expected CRC validation error, got nil")
	}
}

func TestWALOpen(t *testing.T) {
	t.Run("Create new WAL", func(t *testing.T) {
		wal, _ := setupTestWAL(t)
		defer wal.Close()

		stats, err := wal.Stats()
		if err != nil {
			t.Fatalf("Failed to get stats: %v", err)
		}

		if stats.NextLSN != 1 {
			t.Errorf("Expected NextLSN 1 for new WAL, got %d", stats.NextLSN)
		}
		if stats.NextTxnID != 1 {
			t.Errorf("Expected NextTxnID 1 for new WAL, got %d", stats.NextTxnID)
		}
	})

	t.Run("Reopen existing WAL", func(t *testing.T) {
		tmpDir := t.TempDir()
		walPath := filepath.Join(tmpDir, "test.wal")

		// Create and write some records
		wal1, err := Open(walPath, DefaultOptions())
		if err != nil {
			t.Fatalf("Failed to open WAL: %v", err)
		}

		txnID := wal1.BeginTxn()
		_, err = wal1.LogInsert(txnID, []byte("key1"), []byte("value1"), nil)
		if err != nil {
			t.Fatalf("Failed to log insert: %v", err)
		}
		wal1.CommitTxn(txnID)
		wal1.Close()

		// Reopen
		wal2, err := Open(walPath, DefaultOptions())
		if err != nil {
			t.Fatalf("Failed to reopen WAL: %v", err)
		}
		defer wal2.Close()

		stats, err := wal2.Stats()
		if err != nil {
			t.Fatalf("Failed to get stats: %v", err)
		}

		if stats.NextLSN <= 1 {
			t.Errorf("Expected NextLSN > 1 after reopen, got %d", stats.NextLSN)
		}
	})
}

func TestWALTransactionLifecycle(t *testing.T) {
	wal, _ := setupTestWAL(t)
	defer wal.Close()

	// Begin transaction
	txnID := wal.BeginTxn()
	if txnID == 0 {
		t.Error("Expected non-zero TxnID")
	}

	stats, _ := wal.Stats()
	if stats.ActiveTxns != 1 {
		t.Errorf("Expected 1 active transaction, got %d", stats.ActiveTxns)
	}

	// Log operations
	lsn1, err := wal.LogInsert(txnID, []byte("key1"), []byte("value1"), nil)
	if err != nil {
		t.Fatalf("Failed to log insert: %v", err)
	}
	if lsn1 == 0 {
		t.Error("Expected non-zero LSN")
	}

	lsn2, err := wal.LogDelete(txnID, []byte("key1"), []byte("value1"))
	if err != nil {
		t.Fatalf("Failed to log delete: %v", err)
	}
	if lsn2 <= lsn1 {
		t.Errorf("Expected LSN to increase: %d <= %d", lsn2, lsn1)
	}

	// Commit
	err = wal.CommitTxn(txnID)
	if err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	stats, _ = wal.Stats()
	if stats.ActiveTxns != 0 {
		t.Errorf("Expected 0 active transactions after commit, got %d", stats.ActiveTxns)
	}
}

func TestWALAbort(t *testing.T) {
	wal, _ := setupTestWAL(t)
	defer wal.Close()

	txnID := wal.BeginTxn()
	_, err := wal.LogInsert(txnID, []byte("key1"), []byte("value1"), nil)
	if err != nil {
		t.Fatalf("Failed to log insert: %v", err)
	}

	err = wal.AbortTxn(txnID)
	if err != nil {
		t.Fatalf("Failed to abort: %v", err)
	}

	stats, _ := wal.Stats()
	if stats.ActiveTxns != 0 {
		t.Errorf("Expected 0 active transactions after abort, got %d", stats.ActiveTxns)
	}
}

func TestWALRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	// Create WAL and write transactions
	wal1, err := Open(walPath, DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	// Transaction 1: committed
	txn1 := wal1.BeginTxn()
	wal1.LogInsert(txn1, []byte("key1"), []byte("value1"), nil)
	wal1.LogInsert(txn1, []byte("key2"), []byte("value2"), nil)
	wal1.CommitTxn(txn1)

	// Transaction 2: uncommitted (simulate crash)
	txn2 := wal1.BeginTxn()
	wal1.LogInsert(txn2, []byte("key3"), []byte("value3"), nil)
	wal1.LogDelete(txn2, []byte("key1"), []byte("value1"))
	// Don't commit - simulate crash

	wal1.Close()

	// Recover
	wal2, err := Open(walPath, DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	redo, undo, err := wal2.Recover()
	if err != nil {
		t.Fatalf("Failed to recover: %v", err)
	}

	// Transaction 1 was committed, so should be in redo
	if len(redo) != 2 {
		t.Errorf("Expected 2 redo records, got %d", len(redo))
	}

	// Transaction 2 was uncommitted, so should be in undo
	if len(undo) != 2 {
		t.Errorf("Expected 2 undo records, got %d", len(undo))
	}

	// Verify redo records
	foundKey1 := false
	foundKey2 := false
	for _, r := range redo {
		if string(r.Key) == "key1" && string(r.Value) == "value1" {
			foundKey1 = true
		}
		if string(r.Key) == "key2" && string(r.Value) == "value2" {
			foundKey2 = true
		}
	}
	if !foundKey1 || !foundKey2 {
		t.Error("Redo records missing expected keys")
	}

	// Verify undo records
	foundKey3 := false
	foundKey1Delete := false
	for _, r := range undo {
		if string(r.Key) == "key3" && string(r.Value) == "value3" {
			foundKey3 = true
		}
		if string(r.Key) == "key1" && r.RecordType == RecordTypeDelete {
			foundKey1Delete = true
		}
	}
	if !foundKey3 || !foundKey1Delete {
		t.Error("Undo records missing expected keys")
	}
}

func TestWALRecoverToCallback(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	// Create WAL with committed and uncommitted transactions
	wal1, _ := Open(walPath, DefaultOptions())
	txn1 := wal1.BeginTxn()
	wal1.LogInsert(txn1, []byte("key1"), []byte("value1"), nil)
	wal1.CommitTxn(txn1)

	txn2 := wal1.BeginTxn()
	wal1.LogInsert(txn2, []byte("key2"), []byte("value2"), nil)
	// Don't commit
	wal1.Close()

	// Recover with callbacks
	wal2, _ := Open(walPath, DefaultOptions())
	defer wal2.Close()

	applied := make(map[string][]byte)
	deleted := make(map[string]bool)

	err := wal2.RecoverToCallback(
		func(key, value []byte) error {
			applied[string(key)] = value
			return nil
		},
		func(key []byte) error {
			deleted[string(key)] = true
			return nil
		},
	)

	if err != nil {
		t.Fatalf("RecoverToCallback failed: %v", err)
	}

	// key1 should be applied (committed)
	if val, ok := applied["key1"]; !ok || string(val) != "value1" {
		t.Errorf("Expected key1=value1 in applied, got %v", applied)
	}

	// key2 should be deleted (uncommitted insert)
	if !deleted["key2"] {
		t.Error("Expected key2 to be deleted (undo uncommitted insert)")
	}
}

func TestWALCheckpoint(t *testing.T) {
	wal, _ := setupTestWAL(t)
	defer wal.Close()

	// Write some records
	txnID := wal.BeginTxn()
	wal.LogInsert(txnID, []byte("key1"), []byte("value1"), nil)
	wal.CommitTxn(txnID)

	// Create checkpoint
	err := wal.Checkpoint()
	if err != nil {
		t.Fatalf("Failed to create checkpoint: %v", err)
	}

	stats, _ := wal.Stats()
	if stats.LastCheckpointLSN == 0 {
		t.Error("Expected non-zero LastCheckpointLSN after checkpoint")
	}
}

func TestWALTruncate(t *testing.T) {
	wal, _ := setupTestWAL(t)

	// Write some records
	txnID := wal.BeginTxn()
	wal.LogInsert(txnID, []byte("key1"), []byte("value1"), nil)
	wal.CommitTxn(txnID)

	stats1, _ := wal.Stats()
	initialSize := stats1.FileSize

	// Truncate
	err := wal.Truncate()
	if err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}

	stats2, _ := wal.Stats()
	if stats2.FileSize >= initialSize {
		t.Errorf("Expected file size to decrease after truncate: %d >= %d", stats2.FileSize, initialSize)
	}
	if stats2.NextLSN != 1 {
		t.Errorf("Expected NextLSN to reset to 1, got %d", stats2.NextLSN)
	}

	wal.Close()
}

func TestWALSync(t *testing.T) {
	wal, _ := setupTestWAL(t)
	defer wal.Close()

	err := wal.Sync()
	if err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}

func TestWALStats(t *testing.T) {
	wal, _ := setupTestWAL(t)
	defer wal.Close()

	stats, err := wal.Stats()
	if err != nil {
		t.Fatalf("Failed to get stats: %v", err)
	}

	if stats == nil {
		t.Fatal("Stats returned nil")
	}

	// Verify file exists and has size
	if stats.FileSize < 0 {
		t.Errorf("Invalid file size: %d", stats.FileSize)
	}
}

func TestWALMultipleTransactions(t *testing.T) {
	wal, _ := setupTestWAL(t)
	defer wal.Close()

	// Multiple concurrent transactions
	txn1 := wal.BeginTxn()
	txn2 := wal.BeginTxn()
	txn3 := wal.BeginTxn()

	wal.LogInsert(txn1, []byte("key1"), []byte("value1"), nil)
	wal.LogInsert(txn2, []byte("key2"), []byte("value2"), nil)
	wal.LogInsert(txn3, []byte("key3"), []byte("value3"), nil)

	stats, _ := wal.Stats()
	if stats.ActiveTxns != 3 {
		t.Errorf("Expected 3 active transactions, got %d", stats.ActiveTxns)
	}

	wal.CommitTxn(txn1)
	wal.CommitTxn(txn2)
	wal.AbortTxn(txn3)

	stats, _ = wal.Stats()
	if stats.ActiveTxns != 0 {
		t.Errorf("Expected 0 active transactions, got %d", stats.ActiveTxns)
	}
}

func TestWALEmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "empty.wal")

	// Create empty file
	file, err := os.Create(walPath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	file.Close()

	// Open should handle empty file
	wal, err := Open(walPath, DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open empty WAL: %v", err)
	}
	defer wal.Close()

	stats, _ := wal.Stats()
	if stats.NextLSN != 1 {
		t.Errorf("Expected NextLSN 1 for empty file, got %d", stats.NextLSN)
	}
}
