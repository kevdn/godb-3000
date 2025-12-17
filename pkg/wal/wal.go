package wal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

// WAL (Write-Ahead Log) provides durability and crash recovery.
//
// Key principles:
// 1. Write-ahead: All changes are logged before being applied to data pages
// 2. Force on commit: Log must be flushed to disk before commit returns
// 3. No-force: Data pages don't need to be flushed on commit
// 4. Redo on recovery: Replay committed transactions after a crash
// 5. Undo on recovery: Roll back uncommitted transactions
//
// WAL file format:
//
//	[Record 1][Record 2]...[Record N]
//
// Each record contains: CRC, LSN, TxnID, RecordType, Key, Value
type WAL struct {
	path      string
	file      *os.File
	mu        sync.Mutex
	nextLSN   atomic.Uint64
	nextTxnID atomic.Uint64

	// Active transactions (TxnID -> first LSN)
	activeTxns map[TxnID]LSN

	// Track last checkpoint LSN and file offset
	lastCheckpointLSN    LSN
	lastCheckpointOffset int64
}

// Options configures the WAL.
type Options struct {
	// SyncOnWrite forces fsync after every write (slower but safer)
	SyncOnWrite bool
}

// DefaultOptions returns default WAL options.
func DefaultOptions() Options {
	return Options{
		SyncOnWrite: false, // Sync only on commit for better performance
	}
}

// Open opens or creates a WAL file.
func Open(path string, opts Options) (*WAL, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	// Open WAL file (create if doesn't exist)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	wal := &WAL{
		path:              path,
		file:              file,
		activeTxns:        make(map[TxnID]LSN),
		lastCheckpointLSN: 0,
	}

	// Scan existing records to determine next LSN
	if err := wal.scanForNextLSN(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to scan WAL: %w", err)
	}

	return wal, nil
}

// scanForNextLSN scans the WAL file to determine the next LSN and TxnID.
// Also tracks the last checkpoint LSN and offset for recovery optimization.
func (w *WAL) scanForNextLSN() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to beginning
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	var maxLSN LSN = 0
	var maxTxnID TxnID = 0
	var lastCheckpointLSN LSN = 0
	var lastCheckpointOffset int64 = 0

	// Read all records
	buf := make([]byte, 64*1024) // 64KB buffer
	remainder := make([]byte, 0)
	absoluteOffset := int64(0) // Track absolute position in file where current record starts

	for {
		n, err := w.file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read WAL: %w", err)
		}

		// Prepend any remainder from previous iteration	
		data := append(remainder, buf[:n]...)
		remainder = remainder[:0]

		// Parse records from buffer
		pos := 0
		for pos < len(data) {
			// Check minimum header size - must match RecordHeaderSize (33 bytes)
			if len(data)-pos < RecordHeaderSize {
				// Save remainder for next iteration
				remainder = append(remainder, data[pos:]...)
				break
			}

			// Record the offset of this record's start
			recordOffset := absoluteOffset

			record, size, err := UnmarshalRecord(data[pos:])
			if err != nil {
				// Could be corruption or partial record
				// Save remainder for next iteration
				remainder = append(remainder, data[pos:]...)
				break
			}

			if record.LSN > maxLSN {
				maxLSN = record.LSN
			}
			if record.TxnID > maxTxnID {
				maxTxnID = record.TxnID
			}

			// Track last checkpoint
			if record.RecordType == RecordTypeCheckpoint {
				lastCheckpointLSN = record.LSN
				lastCheckpointOffset = recordOffset
			}

			pos += size
			absoluteOffset += int64(size)
		}
	}

	// Set next LSN and TxnID
	w.nextLSN.Store(uint64(maxLSN + 1))
	w.nextTxnID.Store(uint64(maxTxnID + 1))

	// Set checkpoint information
	w.lastCheckpointLSN = lastCheckpointLSN
	w.lastCheckpointOffset = lastCheckpointOffset

	// Seek to end for appending
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}

	return nil
}

// BeginTxn starts a new transaction and returns its ID.
func (w *WAL) BeginTxn() TxnID {
	w.mu.Lock()
	defer w.mu.Unlock()

	txnID := TxnID(w.nextTxnID.Add(1) - 1)
	w.activeTxns[txnID] = LSN(w.nextLSN.Load())

	return txnID
}

// LogInsert logs an insert/update operation.
// oldValue is the before-image (for undo recovery). Can be nil for pure inserts.
func (w *WAL) LogInsert(txnID TxnID, key, value, oldValue []byte) (LSN, error) {
	return w.writeRecord(&Record{
		LSN:        LSN(w.nextLSN.Add(1) - 1),
		TxnID:      txnID,
		RecordType: RecordTypeInsert,
		Key:        key,
		Value:      value,
		OldValue:   oldValue,
	})
}

// LogDelete logs a delete operation.
// oldValue is the before-image (needed for undo recovery).
func (w *WAL) LogDelete(txnID TxnID, key, oldValue []byte) (LSN, error) {
	return w.writeRecord(&Record{
		LSN:        LSN(w.nextLSN.Add(1) - 1),
		TxnID:      txnID,
		RecordType: RecordTypeDelete,
		Key:        key,
		Value:      nil,
		OldValue:   oldValue,
	})
}

// CommitTxn logs a commit record and syncs the log to disk.
// This is the critical durability point.
func (w *WAL) CommitTxn(txnID TxnID) error {
	// Write commit record
	_, err := w.writeRecord(&Record{
		LSN:        LSN(w.nextLSN.Add(1) - 1),
		TxnID:      txnID,
		RecordType: RecordTypeCommit,
		Key:        nil,
		Value:      nil,
	})
	if err != nil {
		return err
	}

	// Force sync to disk (this ensures durability)
	if err := w.Sync(); err != nil {
		return err
	}

	// Remove from active transactions
	w.mu.Lock()
	delete(w.activeTxns, txnID)
	w.mu.Unlock()

	return nil
}

// AbortTxn logs an abort record.
func (w *WAL) AbortTxn(txnID TxnID) error {
	// Write abort record
	_, err := w.writeRecord(&Record{
		LSN:        LSN(w.nextLSN.Add(1) - 1),
		TxnID:      txnID,
		RecordType: RecordTypeAbort,
		Key:        nil,
		Value:      nil,
	})
	if err != nil {
		return err
	}

	// Remove from active transactions
	w.mu.Lock()
	delete(w.activeTxns, txnID)
	w.mu.Unlock()

	return nil
}

// writeRecord writes a record to the WAL file.
func (w *WAL) writeRecord(record *Record) (LSN, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Serialize record
	data := record.Marshal()

	// Write to file
	if _, err := w.file.Write(data); err != nil {
		return 0, fmt.Errorf("failed to write WAL record: %w", err)
	}

	return record.LSN, nil
}

// Sync flushes the WAL to disk (fsync).
// Must be called before commit returns to ensure durability.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	return nil
}

// Checkpoint writes a checkpoint record and truncates old log entries.
// After a checkpoint, recovery only needs to replay from the checkpoint.
func (w *WAL) Checkpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Get current file offset before writing checkpoint
	currentOffset, err := w.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get current offset: %w", err)
	}

	// Get current LSN
	currentLSN := LSN(w.nextLSN.Load())

	// Write checkpoint record
	record := &Record{
		LSN:        currentLSN,
		TxnID:      0,
		RecordType: RecordTypeCheckpoint,
		Key:        nil,
		Value:      nil,
	}

	data := record.Marshal()
	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("failed to write checkpoint: %w", err)
	}

	// Sync to ensure checkpoint is durable
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync checkpoint: %w", err)
	}

	w.nextLSN.Add(1)
	w.lastCheckpointLSN = currentLSN
	w.lastCheckpointOffset = currentOffset

	return nil
}

// Recover replays the WAL and returns records that need to be applied.
// Returns:
//   - Committed operations that need to be redone
//   - Uncommitted operations that need to be undone
//
// Recovery starts from the last checkpoint LSN if available, otherwise from beginning.
func (w *WAL) Recover() (redo []*Record, undo []*Record, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Start from last checkpoint if available, otherwise from beginning
	startLSN := w.lastCheckpointLSN
	if startLSN == 0 || w.lastCheckpointOffset == 0 {
		// No checkpoint, start from beginning
		if _, err := w.file.Seek(0, io.SeekStart); err != nil {
			return nil, nil, err
		}
	} else {
		// Seek directly to checkpoint offset - skip reading old records!
		if _, err := w.file.Seek(w.lastCheckpointOffset, io.SeekStart); err != nil {
			return nil, nil, fmt.Errorf("failed to seek to checkpoint: %w", err)
		}
	}

	// Track committed and uncommitted transactions
	committedTxns := make(map[TxnID]bool)
	abortedTxns := make(map[TxnID]bool)
	allRecords := make([]*Record, 0)
	// If no checkpoint or we seeked to checkpoint offset, process all records from current position
	checkpointFound := startLSN == 0 || w.lastCheckpointOffset > 0

	// Read all records
	buf := make([]byte, 64*1024) // 64KB buffer
	remainder := make([]byte, 0)

	for {
		n, err := w.file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read WAL during recovery: %w", err)
		}

		// Prepend any remainder from previous iteration
		data := append(remainder, buf[:n]...)
		remainder = remainder[:0]

		// Parse records
		pos := 0
		for pos < len(data) {
			// Check minimum header size - must match RecordHeaderSize (33 bytes)
			if len(data)-pos < RecordHeaderSize {
				// Save remainder for next iteration
				remainder = append(remainder, data[pos:]...)
				break
			}

			record, size, parseErr := UnmarshalRecord(data[pos:])
			if parseErr != nil {
				// Could be corruption or partial record
				// In production, you'd want more sophisticated handling
				remainder = append(remainder, data[pos:]...)
				break
			}

			// Skip records before checkpoint (only if we're scanning from beginning)
			if !checkpointFound {
				if record.RecordType == RecordTypeCheckpoint && record.LSN >= startLSN {
					checkpointFound = true
				}
				// Skip this record (before checkpoint)
				pos += size
				continue
			}

			// Skip the checkpoint record itself if we land on it
			if record.RecordType == RecordTypeCheckpoint {
				pos += size
				continue
			}

			// Track record
			allRecords = append(allRecords, record)

			// Track transaction state
			if record.RecordType == RecordTypeCommit {
				committedTxns[record.TxnID] = true
			} else if record.RecordType == RecordTypeAbort {
				abortedTxns[record.TxnID] = true
			}

			pos += size
		}
	}

	// Separate into redo and undo lists
	redo = make([]*Record, 0)
	undo = make([]*Record, 0)

	for _, record := range allRecords {
		// Skip commit/abort/checkpoint records
		if record.RecordType == RecordTypeCommit ||
			record.RecordType == RecordTypeAbort ||
			record.RecordType == RecordTypeCheckpoint {
			continue
		}

		// If transaction was committed, add to redo list
		if committedTxns[record.TxnID] {
			redo = append(redo, record)
		} else if !abortedTxns[record.TxnID] {
			// If transaction was neither committed nor aborted, it was incomplete
			undo = append(undo, record)
		}
	}

	// Seek to end for future writes
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return nil, nil, err
	}

	return redo, undo, nil
}

// Truncate removes all WAL entries and resets the log.
// Should only be called after a successful checkpoint when all data is durable.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Close current file
	if err := w.file.Close(); err != nil {
		return err
	}

	// Truncate file
	file, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	w.file = file
	w.nextLSN.Store(1)
	w.activeTxns = make(map[TxnID]LSN)
	w.lastCheckpointLSN = 0
	w.lastCheckpointOffset = 0

	return nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Sync(); err != nil {
		return err
	}

	return w.file.Close()
}

// Stats returns WAL statistics.
type Stats struct {
	NextLSN           LSN
	NextTxnID         TxnID
	ActiveTxns        int
	LastCheckpointLSN LSN
	FileSize          int64
}

// Stats returns current WAL statistics.
func (w *WAL) Stats() (*Stats, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	info, err := w.file.Stat()
	if err != nil {
		return nil, err
	}

	return &Stats{
		NextLSN:           LSN(w.nextLSN.Load()),
		NextTxnID:         TxnID(w.nextTxnID.Load()),
		ActiveTxns:        len(w.activeTxns),
		LastCheckpointLSN: w.lastCheckpointLSN,
		FileSize:          info.Size(),
	}, nil
}
