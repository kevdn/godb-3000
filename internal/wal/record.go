package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// RecordType represents the type of WAL record.
type RecordType uint8

const (
	// RecordTypeInsert represents an insert/update operation
	RecordTypeInsert RecordType = 1
	// RecordTypeDelete represents a delete operation
	RecordTypeDelete RecordType = 2
	// RecordTypeCommit represents a transaction commit
	RecordTypeCommit RecordType = 3
	// RecordTypeAbort represents a transaction abort
	RecordTypeAbort RecordType = 4
	// RecordTypeCheckpoint represents a checkpoint
	RecordTypeCheckpoint RecordType = 5
)

// LSN (Log Sequence Number) uniquely identifies a log record.
type LSN uint64

// TxnID uniquely identifies a transaction.
type TxnID uint64

// Record represents a single WAL record.
// Layout:
//
//	[CRC32: 4 bytes],
//	[LSN: 8 bytes]
//	[TxnID: 8 bytes]
//	[RecordType: 1 byte]
//	[KeyLen: 4 bytes]
//	[ValueLen: 4 bytes]
//	[OldValueLen: 4 bytes]  // For undo recovery (before-image)
//	[Key: KeyLen bytes]
//	[Value: ValueLen bytes]  // New value (after-image) for INSERT/UPDATE, empty for DELETE
//	[OldValue: OldValueLen bytes]  // Old value (before-image) for UPDATE/DELETE undo
type Record struct {
	LSN        LSN
	TxnID      TxnID
	RecordType RecordType
	Key        []byte
	Value      []byte // New value (after-image) or empty for DELETE
	OldValue   []byte // Old value (before-image) for undo recovery
}

const (
	// RecordHeaderSize is the fixed size of a WAL record header
	// CRC + LSN + TxnID + Type + KeyLen + ValueLen + OldValueLen
	RecordHeaderSize = 4 + 8 + 8 + 1 + 4 + 4 + 4
)

// Marshal serializes a WAL record to bytes.
func (r *Record) Marshal() []byte {
	keyLen := len(r.Key)
	valueLen := len(r.Value)
	oldValueLen := len(r.OldValue)
	totalLen := RecordHeaderSize + keyLen + valueLen + oldValueLen

	buf := make([]byte, totalLen)
	offset := 0

	// Skip CRC for now (we'll compute it at the end)
	offset += 4

	// Write LSN
	binary.LittleEndian.PutUint64(buf[offset:], uint64(r.LSN))
	offset += 8

	// Write TxnID
	binary.LittleEndian.PutUint64(buf[offset:], uint64(r.TxnID))
	offset += 8

	// Write RecordType
	buf[offset] = byte(r.RecordType)
	offset += 1

	// Write KeyLen
	binary.LittleEndian.PutUint32(buf[offset:], uint32(keyLen))
	offset += 4

	// Write ValueLen
	binary.LittleEndian.PutUint32(buf[offset:], uint32(valueLen))
	offset += 4

	// Write OldValueLen
	binary.LittleEndian.PutUint32(buf[offset:], uint32(oldValueLen))
	offset += 4

	// Write Key
	copy(buf[offset:], r.Key)
	offset += keyLen

	// Write Value
	copy(buf[offset:], r.Value)
	offset += valueLen

	// Write OldValue
	copy(buf[offset:], r.OldValue)

	// Compute and write CRC32 (excluding the CRC field itself)
	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], crc)

	return buf
}

// UnmarshalRecord deserializes a WAL record from bytes.
// Returns the record and the number of bytes consumed.
func UnmarshalRecord(data []byte) (*Record, int, error) {
	if len(data) < RecordHeaderSize {
		return nil, 0, fmt.Errorf("insufficient data for record header: got %d bytes, need at least %d", len(data), RecordHeaderSize)
	}

	offset := 0

	// Read and verify CRC
	storedCRC := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Read LSN
	lsn := LSN(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8

	// Read TxnID
	txnID := TxnID(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8

	// Read RecordType
	recType := RecordType(data[offset])
	offset += 1

	// Read KeyLen
	keyLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Read ValueLen
	valueLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Read OldValueLen
	oldValueLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Calculate total record size
	totalLen := RecordHeaderSize + int(keyLen) + int(valueLen) + int(oldValueLen)

	if len(data) < totalLen {
		return nil, 0, fmt.Errorf("insufficient data for record body: got %d bytes, need %d", len(data), totalLen)
	}

	// Verify CRC (compute over everything except CRC field)
	computedCRC := crc32.ChecksumIEEE(data[4:totalLen])
	if storedCRC != computedCRC {
		return nil, 0, fmt.Errorf("CRC mismatch: stored=%x, computed=%x", storedCRC, computedCRC)
	}

	// Read Key
	key := make([]byte, keyLen)
	copy(key, data[offset:offset+int(keyLen)])
	offset += int(keyLen)

	// Read Value
	value := make([]byte, valueLen)
	copy(value, data[offset:offset+int(valueLen)])
	offset += int(valueLen)

	// Read OldValue
	var oldValue []byte
	oldValue = make([]byte, oldValueLen)
	copy(oldValue, data[offset:offset+int(oldValueLen)])

	record := &Record{
		LSN:        lsn,
		TxnID:      txnID,
		RecordType: recType,
		Key:        key,
		Value:      value,
		OldValue:   oldValue,
	}

	return record, totalLen, nil
}

// String returns a human-readable representation of the record.
func (r *Record) String() string {
	var typeName string
	switch r.RecordType {
	case RecordTypeInsert:
		typeName = "INSERT"
	case RecordTypeDelete:
		typeName = "DELETE"
	case RecordTypeCommit:
		typeName = "COMMIT"
	case RecordTypeAbort:
		typeName = "ABORT"
	case RecordTypeCheckpoint:
		typeName = "CHECKPOINT"
	default:
		typeName = fmt.Sprintf("UNKNOWN(%d)", r.RecordType)
	}

	return fmt.Sprintf("WALRecord{LSN:%d, TxnID:%d, Type:%s, KeyLen:%d, ValueLen:%d}",
		r.LSN, r.TxnID, typeName, len(r.Key), len(r.Value))
}
