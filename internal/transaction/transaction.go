package transaction

import (
	"fmt"
	"sync"
	"time"

	"github.com/khoale/godb-3000/internal/index"
	"github.com/khoale/godb-3000/internal/kv"
	"github.com/khoale/godb-3000/internal/table"
)

// IsolationLevel defines the transaction isolation level.
type IsolationLevel int

const (
	// ReadUncommitted allows dirty reads (NOT IMPLEMENTED - for reference only)
	ReadUncommitted IsolationLevel = iota
	// ReadCommitted prevents dirty reads, allows non-repeatable reads
	// Implementation: No read tracking, no validation
	ReadCommitted
	// RepeatableRead prevents non-repeatable reads using Optimistic Concurrency Control
	// Implementation: Tracks reads, validates at commit time, aborts on conflicts
	RepeatableRead
	// Serializable provides full isolation using global write lock
	// Implementation: One transaction at a time, no conflicts possible
	Serializable
)

// Transaction represents a database transaction.
// Implements ACID properties:
// - Atomicity: All operations succeed or all fail (via KV layer)
// - Consistency: Database remains in valid state (via validation)
// - Isolation: Multiple strategies based on isolation level
// - Durability: Committed changes persist (via WAL)
//
// Isolation Implementation Strategy:
// - ReadCommitted: No read tracking, allows non-repeatable reads
// - RepeatableRead: Optimistic Concurrency Control (OCC)
//   - Tracks all reads in readSet
//   - No locks during execution
//   - Validates at commit time
//   - Aborts on conflicts
//
// - Serializable: Pessimistic with global write lock
//   - Only one transaction executes at a time
//   - No validation needed
//   - Guarantees serializability but low concurrency
type Transaction struct {
	id           uint64
	store        *kv.KV
	isolationLvl IsolationLevel
	startTime    time.Time
	committed    bool
	aborted      bool
	readSet      map[string][]byte // Keys read during transaction
	writeSet     map[string][]byte // Keys written during transaction
	mu           sync.Mutex
}

// TxnManager manages concurrent transactions.
type TxnManager struct {
	store         *kv.KV
	nextTxnID     uint64
	activeTxns    map[uint64]*Transaction
	mu            sync.RWMutex
	globalWriteMu sync.Mutex // Global write lock for serializable isolation
}

// NewTxnManager creates a new transaction manager.
func NewTxnManager(store *kv.KV) *TxnManager {
	return &TxnManager{
		store:      store,
		nextTxnID:  1,
		activeTxns: make(map[uint64]*Transaction),
	}
}

// Begin starts a new transaction with the specified isolation level.
func (tm *TxnManager) Begin(level IsolationLevel) (*Transaction, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn := &Transaction{
		id:           tm.nextTxnID,
		store:        tm.store,
		isolationLvl: level,
		startTime:    time.Now(),
		readSet:      make(map[string][]byte),
		writeSet:     make(map[string][]byte),
	}

	tm.nextTxnID++
	tm.activeTxns[txn.id] = txn

	// Start KV transaction
	if err := tm.store.Begin(); err != nil {
		return nil, fmt.Errorf("failed to begin KV transaction: %w", err)
	}

	// For serializable isolation, acquire global write lock
	if level == Serializable {
		tm.globalWriteMu.Lock()
	}

	return txn, nil
}

// Commit commits the transaction and makes all changes durable.
func (tm *TxnManager) Commit(txn *Transaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.committed {
		return fmt.Errorf("transaction already committed")
	}
	if txn.aborted {
		return fmt.Errorf("transaction already aborted")
	}

	// Validate transaction (check for conflicts)
	if err := tm.validate(txn); err != nil {
		tm.abort(txn)
		return fmt.Errorf("validation failed: %w", err)
	}

	// Commit KV transaction (atomic operation)
	if err := txn.store.Commit(); err != nil {
		tm.abort(txn)
		return fmt.Errorf("failed to commit: %w", err)
	}

	txn.committed = true

	// Release locks and cleanup
	tm.mu.Lock()
	delete(tm.activeTxns, txn.id)
	tm.mu.Unlock()

	if txn.isolationLvl == Serializable {
		tm.globalWriteMu.Unlock()
	}

	return nil
}

// Rollback aborts the transaction and discards all changes.
func (tm *TxnManager) Rollback(txn *Transaction) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.committed {
		return fmt.Errorf("cannot rollback committed transaction")
	}
	if txn.aborted {
		return nil // Already aborted
	}

	return tm.abort(txn)
}

// abort performs the actual rollback (must be called with txn.mu held).
func (tm *TxnManager) abort(txn *Transaction) error {
	// Rollback KV transaction
	if err := txn.store.Rollback(); err != nil {
		return fmt.Errorf("failed to rollback: %w", err)
	}

	txn.aborted = true

	// Release locks and cleanup
	tm.mu.Lock()
	delete(tm.activeTxns, txn.id)
	tm.mu.Unlock()

	if txn.isolationLvl == Serializable {
		tm.globalWriteMu.Unlock()
	}

	return nil
}

// validate checks for conflicts with other transactions.
// Only applies to RepeatableRead (ReadCommitted doesn't track reads, Serializable uses lock).
func (tm *TxnManager) validate(txn *Transaction) error {
	// For serializable isolation, no validation needed (global lock prevents conflicts)
	if txn.isolationLvl == Serializable {
		return nil
	}

	// For read committed, no validation needed (doesn't track reads, allows non-repeatable reads)
	if txn.isolationLvl == ReadCommitted {
		return nil
	}

	tm.mu.RLock()
	defer tm.mu.RUnlock()

	// For RepeatableRead: Check if any keys in read set were modified by other transactions
	// This implements Optimistic Concurrency Control (OCC)
	for key := range txn.readSet {
		currentValue, _, err := txn.store.Get([]byte(key))
		if err != nil {
			return err
		}

		// If value changed, we have a conflict (non-repeatable read detected)
		originalValue := txn.readSet[key]
		if !bytesEqual(currentValue, originalValue) {
			return fmt.Errorf("read conflict on key: %s", key)
		}
	}

	return nil
}

// Get retrieves a value within the transaction context.
func (txn *Transaction) Get(key []byte) ([]byte, bool, error) {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.committed || txn.aborted {
		return nil, false, fmt.Errorf("transaction is not active")
	}

	keyStr := string(key)

	// Check write set first (read your own writes)
	if value, exists := txn.writeSet[keyStr]; exists {
		if value == nil {
			return nil, false, nil // Deleted in this transaction
		}
		return value, true, nil
	}

	// Read from store
	value, found, err := txn.store.Get(key)
	if err != nil {
		return nil, false, err
	}

	// Track read for validation (only for RepeatableRead and Serializable)
	// ReadCommitted doesn't track reads to allow non-repeatable reads
	if txn.isolationLvl >= RepeatableRead {
		txn.readSet[keyStr] = value
	}

	return value, found, nil
}

// Set writes a value within the transaction context.
func (txn *Transaction) Set(key, value []byte) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.committed || txn.aborted {
		return fmt.Errorf("transaction is not active")
	}

	// Write to KV store
	if err := txn.store.Set(key, value); err != nil {
		return err
	}

	// Track write
	txn.writeSet[string(key)] = append([]byte{}, value...)

	return nil
}

// Delete removes a key within the transaction context.
func (txn *Transaction) Delete(key []byte) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	if txn.committed || txn.aborted {
		return fmt.Errorf("transaction is not active")
	}

	// Delete from KV store
	if _, err := txn.store.Delete(key); err != nil {
		return err
	}

	// Track deletion
	txn.writeSet[string(key)] = nil

	return nil
}

// TableGet retrieves a row from a table within the transaction.
func (txn *Transaction) TableGet(tbl *table.Table, pkValue interface{}) (*table.Row, error) {
	if txn.committed || txn.aborted {
		return nil, fmt.Errorf("transaction is not active")
	}

	// This would normally go through the transaction's Get method
	// For simplicity, we directly use the table's Get method
	// In a full implementation, we'd intercept table operations
	return tbl.Get(pkValue)
}

// TableInsert inserts a row into a table within the transaction.
func (txn *Transaction) TableInsert(tbl *table.Table, row *table.Row) error {
	if txn.committed || txn.aborted {
		return fmt.Errorf("transaction is not active")
	}

	if err := tbl.Insert(row); err != nil {
		return err
	}

	// Update indexes
	return txn.updateIndexesForInsert(tbl, row)
}

// TableUpdate updates a row in a table within the transaction.
func (txn *Transaction) TableUpdate(tbl *table.Table, pkValue interface{}, row *table.Row) error {
	if txn.committed || txn.aborted {
		return fmt.Errorf("transaction is not active")
	}

	// Get old row before update
	oldRow, err := tbl.Get(pkValue)
	if err != nil {
		return err
	}

	if err := tbl.Update(pkValue, row); err != nil {
		return err
	}

	// Update indexes
	return txn.updateIndexesForUpdate(tbl, oldRow, row)
}

// TableDelete deletes a row from a table within the transaction.
func (txn *Transaction) TableDelete(tbl *table.Table, pkValue interface{}) error {
	if txn.committed || txn.aborted {
		return fmt.Errorf("transaction is not active")
	}

	// Get row before delete (for index updates)
	row, err := tbl.Get(pkValue)
	if err != nil {
		return err
	}

	if err := tbl.Delete(pkValue); err != nil {
		return err
	}

	// Update indexes
	return txn.updateIndexesForDelete(tbl, row)
}

// ID returns the transaction ID.
func (txn *Transaction) ID() uint64 {
	return txn.id
}

// IsActive returns true if the transaction is still active.
func (txn *Transaction) IsActive() bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return !txn.committed && !txn.aborted
}

// IsCommitted returns true if the transaction has been committed.
func (txn *Transaction) IsCommitted() bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.committed
}

// IsAborted returns true if the transaction has been aborted.
func (txn *Transaction) IsAborted() bool {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	return txn.aborted
}

// Duration returns how long the transaction has been running.
func (txn *Transaction) Duration() time.Duration {
	return time.Since(txn.startTime)
}

// Stats returns statistics about active transactions.
type TxnStats struct {
	ActiveTransactions int
	OldestTxnAge       time.Duration
	TotalCommitted     uint64
}

// Stats returns transaction manager statistics.
func (tm *TxnManager) Stats() TxnStats {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	stats := TxnStats{
		ActiveTransactions: len(tm.activeTxns),
		TotalCommitted:     tm.nextTxnID - 1,
	}

	// Find oldest transaction
	var oldestTime time.Time
	for _, txn := range tm.activeTxns {
		if oldestTime.IsZero() || txn.startTime.Before(oldestTime) {
			oldestTime = txn.startTime
		}
	}

	if !oldestTime.IsZero() {
		stats.OldestTxnAge = time.Since(oldestTime)
	}

	return stats
}

// updateIndexesForInsert updates all indexes after a row insert.
func (txn *Transaction) updateIndexesForInsert(tbl *table.Table, row *table.Row) error {
	tableName := tbl.Name()
	schema := tbl.Schema()

	// Get all indexes for this table
	indexNames, err := index.ListIndexes(tableName, txn.store)
	if err != nil {
		// No indexes or error - continue (not critical)
		return nil
	}

	// Get primary key value
	pkIdx, err := schema.GetColumnIndex(schema.PrimaryKey)
	if err != nil {
		return err
	}
	pkValue := row.Values[pkIdx]

	// Update each index
	for _, colName := range indexNames {
		idx, err := index.LoadIndex(tableName, colName, txn.store)
		if err != nil {
			// Index might have been dropped, skip it
			continue
		}

		// Get indexed column value
		colIdx, err := schema.GetColumnIndex(colName)
		if err != nil {
			continue
		}
		colValue := row.Values[colIdx]

		// Insert into index
		if err := idx.Insert(colValue, pkValue); err != nil {
			return fmt.Errorf("failed to update index %s: %w", colName, err)
		}
	}

	return nil
}

// updateIndexesForUpdate updates all indexes after a row update.
func (txn *Transaction) updateIndexesForUpdate(tbl *table.Table, oldRow, newRow *table.Row) error {
	tableName := tbl.Name()
	schema := tbl.Schema()

	// Get all indexes for this table
	indexNames, err := index.ListIndexes(tableName, txn.store)
	if err != nil {
		// No indexes or error - continue (not critical)
		return nil
	}

	// Get primary key value
	pkIdx, err := schema.GetColumnIndex(schema.PrimaryKey)
	if err != nil {
		return err
	}
	pkValue := newRow.Values[pkIdx]

	// Update each index
	for _, colName := range indexNames {
		idx, err := index.LoadIndex(tableName, colName, txn.store)
		if err != nil {
			// Index might have been dropped, skip it
			continue
		}

		// Get indexed column values
		colIdx, err := schema.GetColumnIndex(colName)
		if err != nil {
			continue
		}
		oldValue := oldRow.Values[colIdx]
		newValue := newRow.Values[colIdx]

		// If value changed, update index
		if oldValue != newValue {
			// Delete old index entry
			if oldValue != nil {
				if err := idx.Delete(oldValue, pkValue); err != nil {
					return fmt.Errorf("failed to delete old index entry for %s: %w", colName, err)
				}
			}
			// Insert new index entry
			if newValue != nil {
				if err := idx.Insert(newValue, pkValue); err != nil {
					return fmt.Errorf("failed to insert new index entry for %s: %w", colName, err)
				}
			}
		}
	}

	return nil
}

// updateIndexesForDelete updates all indexes after a row delete.
func (txn *Transaction) updateIndexesForDelete(tbl *table.Table, row *table.Row) error {
	tableName := tbl.Name()
	schema := tbl.Schema()

	// Get all indexes for this table
	indexNames, err := index.ListIndexes(tableName, txn.store)
	if err != nil {
		// No indexes or error - continue (not critical)
		return nil
	}

	// Get primary key value
	pkIdx, err := schema.GetColumnIndex(schema.PrimaryKey)
	if err != nil {
		return err
	}
	pkValue := row.Values[pkIdx]

	// Update each index
	for _, colName := range indexNames {
		idx, err := index.LoadIndex(tableName, colName, txn.store)
		if err != nil {
			// Index might have been dropped, skip it
			continue
		}

		// Get indexed column value
		colIdx, err := schema.GetColumnIndex(colName)
		if err != nil {
			continue
		}
		colValue := row.Values[colIdx]

		// Delete from index
		if colValue != nil {
			if err := idx.Delete(colValue, pkValue); err != nil {
				return fmt.Errorf("failed to delete from index %s: %w", colName, err)
			}
		}
	}

	return nil
}

// bytesEqual compares two byte slices for equality.
func bytesEqual(a, b []byte) bool {
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
