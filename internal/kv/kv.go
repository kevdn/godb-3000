package kv

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/khoale/godb-3000/internal/btree"
	"github.com/khoale/godb-3000/internal/storage"
	"github.com/khoale/godb-3000/internal/wal"
)

// KV represents a key-value store built on top of a B+tree.
// It provides:
// 1. Simple CRUD operations (Get, Set, Delete)
// 2. Range queries (Scan)
// 3. Transaction support (Begin, Commit, Rollback)
// 4. Durability through WAL (Write-Ahead Logging) and fsync
// 5. Free list management for efficient space reuse
// 6. Crash recovery through WAL replay
//
// The KV store uses page 0 for metadata and page 1 for the B+tree root.
type KV struct {
	pager        *storage.Pager
	btree        *btree.BTree
	meta         *Metadata
	wal          *wal.WAL
	mu           sync.RWMutex
	txnMu        sync.Mutex // Separate lock for transaction management
	inTxn        bool
	currentTxnID wal.TxnID
	txnLog       []*txnEntry // Transaction log for rollback
}

// Metadata stores database metadata including free list and root page ID.
type Metadata struct {
	Magic      [8]byte        // Magic number "GODB3000"
	Version    uint32         // Database version
	RootPageID storage.PageID // B+tree root page ID
	PageMgr    *storage.PageManager
}

// txnEntry represents a logged operation for rollback.
type txnEntry struct {
	op    string // "set", "delete"
	key   []byte
	value []byte // Previous value (for rollback)
}

// Options configures the KV store.
type Options struct {
	PagerOptions storage.PagerOptions
	WALOptions   wal.Options
	ReadOnly     bool
	DisableWAL   bool // For testing or when WAL is not needed
}

// DefaultOptions returns default KV options.
func DefaultOptions() Options {
	return Options{
		PagerOptions: storage.DefaultPagerOptions(),
		WALOptions:   wal.DefaultOptions(),
		ReadOnly:     false,
		DisableWAL:   false,
	}
}

// Open opens or creates a KV store at the given path.
func Open(path string, opts Options) (*KV, error) {
	// Open pager
	pager, err := storage.OpenPager(path, opts.PagerOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to open pager: %w", err)
	}

	kv := &KV{
		pager:  pager,
		txnLog: make([]*txnEntry, 0),
	}

	// Load or initialize metadata
	if pager.NumPages() <= 1 {
		// New database: initialize
		if opts.ReadOnly {
			pager.Close()
			return nil, fmt.Errorf("cannot create new database in read-only mode")
		}

		if err := kv.initializeNew(); err != nil {
			pager.Close()
			return nil, fmt.Errorf("failed to initialize database: %w", err)
		}
	} else {
		// Existing database: load metadata
		if err := kv.loadMetadata(); err != nil {
			pager.Close()
			return nil, fmt.Errorf("failed to load metadata: %w", err)
		}
	}

	// Set PageManager in pager now that metadata is loaded
	pager.SetPageManager(kv.meta.PageMgr)

	// Load B+tree
	tree, err := btree.LoadBTree(pager, kv.meta.RootPageID)
	if err != nil {
		pager.Close()
		return nil, fmt.Errorf("failed to load btree: %w", err)
	}
	kv.btree = tree

	// Open WAL and perform recovery (after btree is loaded)
	if !opts.DisableWAL && !opts.ReadOnly {
		walPath := path + ".wal"
		walFile, err := wal.Open(walPath, opts.WALOptions)
		if err != nil {
			pager.Close()
			return nil, fmt.Errorf("failed to open WAL: %w", err)
		}
		kv.wal = walFile

		// Perform recovery if WAL has entries
		if err := kv.recover(); err != nil {
			walFile.Close()
			pager.Close()
			return nil, fmt.Errorf("failed to recover from WAL: %w", err)
		}
	}

	return kv, nil
}

// recover replays the WAL to restore committed transactions.
func (kv *KV) recover() error {
	if kv.wal == nil {
		return nil
	}

	// Get redo and undo records
	redo, undo, err := kv.wal.Recover()
	if err != nil {
		return fmt.Errorf("WAL recovery failed: %w", err)
	}

	// Undo uncommitted transactions (in reverse order)
	for i := len(undo) - 1; i >= 0; i-- {
		record := undo[i]
		if record.RecordType == wal.RecordTypeInsert {
			// Undo insert/update:
			// - If oldValue exists, this was an UPDATE - restore old value
			// - If oldValue is nil, this was an INSERT - delete the key
			if len(record.OldValue) > 0 {
				// UPDATE: restore old value
				if err := kv.btree.Insert(record.Key, record.OldValue); err != nil {
					return fmt.Errorf("failed to undo update: %w", err)
				}
			} else {
				// INSERT: delete the key
				if _, err := kv.btree.Delete(record.Key); err != nil {
					return fmt.Errorf("failed to undo insert: %w", err)
				}
			}
		} else if record.RecordType == wal.RecordTypeDelete {
			// Undo delete: re-insert old value
			if len(record.OldValue) > 0 {
				if err := kv.btree.Insert(record.Key, record.OldValue); err != nil {
					return fmt.Errorf("failed to undo delete: %w", err)
				}
			}
			// If no oldValue, key didn't exist before (shouldn't happen, but skip gracefully)
		}
	}

	// Redo committed transactions
	for _, record := range redo {
		if record.RecordType == wal.RecordTypeInsert {
			if err := kv.btree.Insert(record.Key, record.Value); err != nil {
				return fmt.Errorf("failed to redo insert: %w", err)
			}
		} else if record.RecordType == wal.RecordTypeDelete {
			if _, err := kv.btree.Delete(record.Key); err != nil {
				return fmt.Errorf("failed to redo delete: %w", err)
			}
		}
	}

	return nil
}

// initializeNew initializes a new database.
func (kv *KV) initializeNew() error {
	// Create metadata
	kv.meta = &Metadata{
		Magic:      [8]byte{'G', 'O', 'D', 'B', '3', '0', '0', '0'},
		Version:    1,
		RootPageID: 1,                         // Root will be page 1
		PageMgr:    storage.NewPageManager(1), // Start allocating from page 1 (page 0 is metadata)
	}

	// Set PageManager in pager BEFORE creating BTree
	// This ensures AllocatePage() always has PageManager available
	kv.pager.SetPageManager(kv.meta.PageMgr)

	// Create B+tree
	tree, err := btree.NewBTree(kv.pager)
	if err != nil {
		return err
	}
	kv.btree = tree
	kv.meta.RootPageID = tree.RootID()

	// Save metadata
	if err := kv.saveMetadata(); err != nil {
		return err
	}

	return kv.pager.Sync()
}

// loadMetadata loads metadata from page 0.
func (kv *KV) loadMetadata() error {
	page, err := kv.pager.ReadPage(0)
	if err != nil {
		return err
	}

	// Validate page type
	if page.Type() != storage.PageTypeMeta {
		return fmt.Errorf("corrupted metadata page: expected type %d (PageTypeMeta), got %d", storage.PageTypeMeta, page.Type())
	}

	payload := page.Payload()

	// Read magic and version
	magic := [8]byte{}
	copy(magic[:], payload[0:8])
	if magic != [8]byte{'G', 'O', 'D', 'B', '3', '0', '0', '0'} {
		return fmt.Errorf("invalid magic number")
	}

	version := binary.LittleEndian.Uint32(payload[8:12])
	if version != 1 {
		return fmt.Errorf("unsupported version: %d", version)
	}

	rootPageID := storage.PageID(binary.LittleEndian.Uint64(payload[12:20]))

	// Read page manager state
	pmDataLen := binary.LittleEndian.Uint32(payload[20:24])
	pmData := payload[24 : 24+pmDataLen]

	pageMgr := storage.NewPageManager(2)
	if err := pageMgr.Unmarshal(pmData); err != nil {
		return fmt.Errorf("failed to unmarshal page manager: %w", err)
	}

	kv.meta = &Metadata{
		Magic:      magic,
		Version:    version,
		RootPageID: rootPageID,
		PageMgr:    pageMgr,
	}

	return nil
}

// saveMetadata saves metadata to page 0.
func (kv *KV) saveMetadata() error {
	page := storage.NewPage(storage.PageTypeMeta)
	payload := page.Payload()

	// Write magic and version
	copy(payload[0:8], kv.meta.Magic[:])
	binary.LittleEndian.PutUint32(payload[8:12], kv.meta.Version)
	binary.LittleEndian.PutUint64(payload[12:20], uint64(kv.meta.RootPageID))

	// Write page manager state
	pmData := kv.meta.PageMgr.Marshal()
	binary.LittleEndian.PutUint32(payload[20:24], uint32(len(pmData)))
	copy(payload[24:], pmData)

	return kv.pager.WritePage(0, page)
}

// Get retrieves the value for a key.
// Returns the value and true if found, or nil and false if not found.
func (kv *KV) Get(key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, fmt.Errorf("empty key")
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	return kv.btree.Get(key)
}

// Set inserts or updates a key-value pair.
func (kv *KV) Set(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("empty key")
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// If in transaction, log the operation
	if kv.inTxn {
		oldValue, exists, _ := kv.btree.Get(key)
		entry := &txnEntry{
			op:  "set",
			key: append([]byte{}, key...),
		}
		if exists {
			entry.value = oldValue
		}
		kv.txnLog = append(kv.txnLog, entry)

		// Log to WAL if enabled (with before-image for undo recovery)
		if kv.wal != nil {
			var oldValueForWAL []byte
			if exists {
				oldValueForWAL = oldValue
			}
			if _, err := kv.wal.LogInsert(kv.currentTxnID, key, value, oldValueForWAL); err != nil {
				return fmt.Errorf("failed to log insert to WAL: %w", err)
			}
		}
	}

	return kv.btree.Insert(key, value)
}

// Delete removes a key from the store.
// Returns true if the key was found and deleted.
func (kv *KV) Delete(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, fmt.Errorf("empty key")
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// If in transaction, log the operation
	if kv.inTxn {
		oldValue, exists, _ := kv.btree.Get(key)
		if exists {
			entry := &txnEntry{
				op:    "delete",
				key:   append([]byte{}, key...),
				value: oldValue,
			}
			kv.txnLog = append(kv.txnLog, entry)

			// Log to WAL if enabled (with before-image for undo recovery)
			if kv.wal != nil {
				if _, err := kv.wal.LogDelete(kv.currentTxnID, key, oldValue); err != nil {
					return false, fmt.Errorf("failed to log delete to WAL: %w", err)
				}
			}
		}
	}

	return kv.btree.Delete(key)
}

// Scan performs a range query from startKey to endKey (exclusive).
// If startKey is nil, starts from the beginning.
// Scan performs a range scan over [startKey, endKey).
// If endKey is nil, scans to the end.
// The callback is called for each key-value pair. Return false to stop iteration.
func (kv *KV) Scan(startKey, endKey []byte, callback func(key, value []byte) bool) error {
	// Collect all results first while holding the lock
	type result struct {
		key   []byte
		value []byte
	}
	var results []result

	kv.mu.RLock()
	err := kv.btree.Scan(startKey, endKey, func(key, value []byte) bool {
		// Copy the data since we'll release the lock before calling user callback
		keyCopy := append([]byte{}, key...)
		valueCopy := append([]byte{}, value...)
		results = append(results, result{key: keyCopy, value: valueCopy})
		return true
	})
	kv.mu.RUnlock()

	if err != nil {
		return err
	}

	// Now call the user's callback without holding any locks
	for _, r := range results {
		if !callback(r.key, r.value) {
			break
		}
	}

	return nil
}

// Begin starts a new transaction.
// Only one transaction can be active at a time.
func (kv *KV) Begin() error {
	kv.txnMu.Lock()
	defer kv.txnMu.Unlock()

	if kv.inTxn {
		return fmt.Errorf("transaction already in progress")
	}

	kv.inTxn = true
	kv.txnLog = make([]*txnEntry, 0)

	// Begin WAL transaction if enabled
	if kv.wal != nil {
		kv.currentTxnID = kv.wal.BeginTxn()
	}

	return nil
}

// Commit commits the current transaction.
// All changes are made durable with WAL and fsync.
func (kv *KV) Commit() error {
	kv.txnMu.Lock()
	defer kv.txnMu.Unlock()

	if !kv.inTxn {
		return fmt.Errorf("no transaction in progress")
	}

	// Commit to WAL first (write-ahead logging principle)
	if kv.wal != nil {
		if err := kv.wal.CommitTxn(kv.currentTxnID); err != nil {
			return fmt.Errorf("failed to commit WAL: %w", err)
		}
	}

	// Save metadata and flush to disk
	if err := kv.saveMetadata(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	if err := kv.pager.Flush(); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}

	// Clear transaction state
	kv.inTxn = false
	kv.txnLog = nil

	return nil
}

// Rollback aborts the current transaction and reverts all changes.
func (kv *KV) Rollback() error {
	kv.txnMu.Lock()
	defer kv.txnMu.Unlock()

	if !kv.inTxn {
		return fmt.Errorf("no transaction in progress")
	}

	// Abort WAL transaction if enabled
	if kv.wal != nil {
		if err := kv.wal.AbortTxn(kv.currentTxnID); err != nil {
			return fmt.Errorf("failed to abort WAL: %w", err)
		}
	}

	// Apply operations in reverse order
	for i := len(kv.txnLog) - 1; i >= 0; i-- {
		entry := kv.txnLog[i]
		switch entry.op {
		case "set":
			if entry.value != nil {
				// Restore old value
				kv.btree.Insert(entry.key, entry.value)
			} else {
				// Key didn't exist before, delete it
				kv.btree.Delete(entry.key)
			}
		case "delete":
			// Restore deleted key
			if entry.value != nil {
				kv.btree.Insert(entry.key, entry.value)
			}
		}
	}

	// Clear transaction state without flushing
	kv.inTxn = false
	kv.txnLog = nil

	return nil
}

// Sync flushes all pending writes to disk.
// This is the critical durability operation.
func (kv *KV) Sync() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if err := kv.saveMetadata(); err != nil {
		return err
	}

	return kv.pager.Sync()
}

// Close closes the KV store and releases resources.
// Always call Close when done to ensure data persistence.
func (kv *KV) Close() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// If in transaction, rollback
	if kv.inTxn {
		kv.inTxn = false
		kv.txnLog = nil
	}

	if err := kv.saveMetadata(); err != nil {
		return err
	}

	// Close WAL if enabled
	if kv.wal != nil {
		if err := kv.wal.Close(); err != nil {
			return fmt.Errorf("failed to close WAL: %w", err)
		}
	}

	return kv.pager.Close()
}

// Stats returns statistics about the KV store.
type KVStats struct {
	BTreeStats    *btree.BTreeStats
	PagerStats    storage.PagerStats
	WALStats      *wal.Stats
	FreePages     int
	TotalPages    storage.PageID
	InTransaction bool
}

// Stats returns current statistics.
func (kv *KV) Stats() (*KVStats, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	btreeStats, err := kv.btree.Stats()
	if err != nil {
		return nil, err
	}

	var walStats *wal.Stats
	if kv.wal != nil {
		walStats, err = kv.wal.Stats()
		if err != nil {
			return nil, err
		}
	}

	return &KVStats{
		BTreeStats:    btreeStats,
		PagerStats:    kv.pager.Stats(),
		WALStats:      walStats,
		FreePages:     kv.meta.PageMgr.FreePages(),
		TotalPages:    kv.meta.PageMgr.TotalPages(),
		InTransaction: kv.inTxn,
	}, nil
}

// Backup creates a backup of the database to the specified path.
func (kv *KV) Backup(destPath string) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Ensure metadata is saved
	if err := kv.saveMetadata(); err != nil {
		return err
	}

	return kv.pager.CopyTo(destPath)
}

// Count returns the approximate number of keys in the store.
func (kv *KV) Count() (int, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	stats, err := kv.btree.Stats()
	if err != nil {
		return 0, err
	}

	return stats.NumKeys, nil
}

// Checkpoint creates a checkpoint in the WAL and truncates old entries.
// This should be called periodically to prevent the WAL from growing too large.
func (kv *KV) Checkpoint() error {
	if kv.wal == nil {
		return nil // WAL not enabled
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Ensure all data is flushed to disk
	if err := kv.saveMetadata(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	if err := kv.pager.Flush(); err != nil {
		return fmt.Errorf("failed to flush pager: %w", err)
	}

	// Create checkpoint in WAL
	if err := kv.wal.Checkpoint(); err != nil {
		return fmt.Errorf("failed to checkpoint WAL: %w", err)
	}

	// Optionally truncate WAL after checkpoint
	// (commented out for safety - you could enable this in production)
	// if err := kv.wal.Truncate(); err != nil {
	// 	return fmt.Errorf("failed to truncate WAL: %w", err)
	// }

	return nil
}
