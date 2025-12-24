package index

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/khoale/godb-3000/internal/kv"
	"github.com/khoale/godb-3000/internal/table"
)

// Index represents a secondary index on a table column.
// Secondary indexes map column values to primary key values,
// allowing efficient lookups without scanning the entire table.
//
// Key structure:
// - Index metadata: "__index__:<table_name>:<column_name>" -> index metadata
// - Index entries: "__idx__:<table_name>:<column_name>:<encoded_value>:<pk>" -> empty
//
// The index stores (indexed_value, primary_key) pairs, sorted by indexed_value.
// This allows efficient range queries on the indexed column.
type Index struct {
	tableName  string
	columnName string
	columnType table.DataType
	unique     bool // Whether this is a unique index
	store      *kv.KV
	prefix     string // Key prefix for this index's data
}

// IndexMetadata stores metadata about an index.
type IndexMetadata struct {
	TableName  string
	ColumnName string
	ColumnType table.DataType
	Unique     bool
}

// CreateIndex creates a new secondary index on the specified column.
func CreateIndex(tableName, columnName string, schema *table.Schema, store *kv.KV, unique bool) (*Index, error) {
	if tableName == "" || columnName == "" {
		return nil, fmt.Errorf("table name and column name cannot be empty")
	}

	// Verify column exists in schema
	col, err := schema.GetColumn(columnName)
	if err != nil {
		return nil, fmt.Errorf("column not found: %w", err)
	}

	// Don't create index on primary key (it's already indexed)
	if col.PrimaryKey {
		return nil, fmt.Errorf("cannot create index on primary key column")
	}

	idx := &Index{
		tableName:  tableName,
		columnName: columnName,
		columnType: col.Type,
		unique:     unique,
		store:      store,
		prefix:     fmt.Sprintf("__idx__:%s:%s:", tableName, columnName),
	}

	// Save index metadata
	metaKey := []byte(fmt.Sprintf("__index__:%s:%s", tableName, columnName))
	meta := &IndexMetadata{
		TableName:  tableName,
		ColumnName: columnName,
		ColumnType: col.Type,
		Unique:     unique,
	}
	metaData := meta.Marshal()
	if err := store.Set(metaKey, metaData); err != nil {
		return nil, fmt.Errorf("failed to save index metadata: %w", err)
	}

	return idx, nil
}

// LoadIndex loads an existing index from the KV store.
func LoadIndex(tableName, columnName string, store *kv.KV) (*Index, error) {
	metaKey := []byte(fmt.Sprintf("__index__:%s:%s", tableName, columnName))
	metaData, found, err := store.Get(metaKey)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("index not found")
	}

	meta, err := UnmarshalIndexMetadata(metaData)
	if err != nil {
		return nil, err
	}

	return &Index{
		tableName:  meta.TableName,
		columnName: meta.ColumnName,
		columnType: meta.ColumnType,
		unique:     meta.Unique,
		store:      store,
		prefix:     fmt.Sprintf("__idx__:%s:%s:", tableName, columnName),
	}, nil
}

// Insert adds an entry to the index for the given column value and primary key.
func (idx *Index) Insert(columnValue, primaryKey interface{}) error {
	// Handle NULL values (don't index them)
	if columnValue == nil {
		return nil
	}

	// Generate index key
	indexKey, err := idx.makeIndexKey(columnValue, primaryKey)
	if err != nil {
		return err
	}

	// Check for uniqueness constraint (only if this is a unique index)
	if idx.unique {
		exists, err := idx.Exists(columnValue)
		if err != nil {
			return err
		}
		if exists {
			// Check if it's the same primary key
			existingPK, err := idx.Lookup(columnValue)
			if err != nil {
				return err
			}
			if len(existingPK) > 0 && !equalPrimaryKeys(existingPK[0], primaryKey) {
				return fmt.Errorf("unique constraint violation: value already exists")
			}
		}
	}

	// Store empty value (we only need the key)
	return idx.store.Set(indexKey, []byte{})
}

// Delete removes an entry from the index.
func (idx *Index) Delete(columnValue, primaryKey interface{}) error {
	if columnValue == nil {
		return nil
	}

	indexKey, err := idx.makeIndexKey(columnValue, primaryKey)
	if err != nil {
		return err
	}

	_, err = idx.store.Delete(indexKey)
	return err
}

// Lookup finds all primary keys for the given column value.
func (idx *Index) Lookup(columnValue interface{}) ([]interface{}, error) {
	if columnValue == nil {
		return nil, nil
	}

	// Encode the column value
	encodedValue, err := idx.encodeValue(columnValue)
	if err != nil {
		return nil, err
	}

	// Scan for all entries with this value
	startKey := []byte(idx.prefix)
	startKey = append(startKey, encodedValue...)
	startKey = append(startKey, ':')

	endKey := append([]byte{}, startKey...)
	endKey = append(endKey, 0xFF)

	var primaryKeys []interface{}
	err = idx.store.Scan(startKey, endKey, func(key, value []byte) bool {
		// Extract primary key from the index key
		pk, err := idx.extractPrimaryKey(key)
		if err != nil {
			return true // Continue on error
		}
		primaryKeys = append(primaryKeys, pk)
		return true
	})

	return primaryKeys, err
}

// Exists checks if a value exists in the index.
func (idx *Index) Exists(columnValue interface{}) (bool, error) {
	pks, err := idx.Lookup(columnValue)
	if err != nil {
		return false, err
	}
	return len(pks) > 0, nil
}

// ScanRange performs a range scan on the index between startValue and endValue.
// Returns a list of primary keys.
func (idx *Index) ScanRange(startValue, endValue interface{}) ([]interface{}, error) {
	var startKey, endKey []byte

	if startValue != nil {
		encoded, err := idx.encodeValue(startValue)
		if err != nil {
			return nil, err
		}
		startKey = []byte(idx.prefix)
		startKey = append(startKey, encoded...)
		startKey = append(startKey, ':')
	} else {
		startKey = []byte(idx.prefix)
	}

	if endValue != nil {
		encoded, err := idx.encodeValue(endValue)
		if err != nil {
			return nil, err
		}
		endKey = []byte(idx.prefix)
		endKey = append(endKey, encoded...)
		endKey = append(endKey, ':', 0xFF)
	} else {
		endKey = []byte(idx.prefix + "\xFF")
	}

	var primaryKeys []interface{}
	err := idx.store.Scan(startKey, endKey, func(key, value []byte) bool {
		pk, err := idx.extractPrimaryKey(key)
		if err != nil {
			return true
		}
		primaryKeys = append(primaryKeys, pk)
		return true
	})

	return primaryKeys, err
}

// Drop removes the index and all its entries from the KV store.
func (idx *Index) Drop() error {
	// Delete metadata
	metaKey := []byte(fmt.Sprintf("__index__:%s:%s", idx.tableName, idx.columnName))
	if _, err := idx.store.Delete(metaKey); err != nil {
		return err
	}

	// Delete all index entries
	startKey := []byte(idx.prefix)
	endKey := []byte(idx.prefix + "\xFF")

	var keysToDelete [][]byte
	err := idx.store.Scan(startKey, endKey, func(key, value []byte) bool {
		keysToDelete = append(keysToDelete, append([]byte{}, key...))
		return true
	})
	if err != nil {
		return err
	}

	for _, key := range keysToDelete {
		if _, err := idx.store.Delete(key); err != nil {
			return err
		}
	}

	return nil
}

// makeIndexKey generates a key for an index entry.
// Format: "__idx__:<table>:<column>:<encoded_value>:<encoded_pk>"
func (idx *Index) makeIndexKey(columnValue, primaryKey interface{}) ([]byte, error) {
	encodedValue, err := idx.encodeValue(columnValue)
	if err != nil {
		return nil, err
	}

	encodedPK, err := idx.encodePrimaryKey(primaryKey)
	if err != nil {
		return nil, err
	}

	key := []byte(idx.prefix)
	key = append(key, encodedValue...)
	key = append(key, ':')
	key = append(key, encodedPK...)

	return key, nil
}

// encodeValue encodes a column value for use in an index key.
func (idx *Index) encodeValue(value interface{}) ([]byte, error) {
	switch idx.columnType {
	case table.TypeInt:
		v, ok := value.(int64)
		if !ok {
			return nil, fmt.Errorf("expected int64, got %T", value)
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf, nil

	case table.TypeString:
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", value)
		}
		return []byte(v), nil

	case table.TypeBytes:
		v, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte, got %T", value)
		}
		return v, nil

	case table.TypeBool:
		v, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("expected bool, got %T", value)
		}
		if v {
			return []byte{1}, nil
		}
		return []byte{0}, nil

	case table.TypeFloat:
		v, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("expected float64, got %T", value)
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf, nil

	default:
		return nil, fmt.Errorf("unsupported type: %d", idx.columnType)
	}
}

// encodePrimaryKey encodes a primary key value.
func (idx *Index) encodePrimaryKey(pk interface{}) ([]byte, error) {
	switch v := pk.(type) {
	case int64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf, nil
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		return nil, fmt.Errorf("unsupported primary key type: %T", pk)
	}
}

// extractPrimaryKey extracts the primary key from an index key.
func (idx *Index) extractPrimaryKey(indexKey []byte) (interface{}, error) {
	// Skip prefix
	if !bytes.HasPrefix(indexKey, []byte(idx.prefix)) {
		return nil, fmt.Errorf("invalid index key")
	}

	remaining := indexKey[len(idx.prefix):]

	// Find the last colon separator (between value and pk)
	lastColon := bytes.LastIndexByte(remaining, ':')
	if lastColon == -1 {
		return nil, fmt.Errorf("invalid index key format")
	}

	pkBytes := remaining[lastColon+1:]

	// Decode based on common primary key types
	// Try int64 first (8 bytes)
	if len(pkBytes) == 8 {
		return int64(binary.BigEndian.Uint64(pkBytes)), nil
	}

	// Otherwise treat as string
	return string(pkBytes), nil
}

// Marshal serializes index metadata.
func (m *IndexMetadata) Marshal() []byte {
	tableNameLen := len(m.TableName)
	colNameLen := len(m.ColumnName)
	size := 2 + tableNameLen + 2 + colNameLen + 2

	data := make([]byte, size)
	offset := 0

	// Table name
	binary.LittleEndian.PutUint16(data[offset:], uint16(tableNameLen))
	offset += 2
	copy(data[offset:], m.TableName)
	offset += tableNameLen

	// Column name
	binary.LittleEndian.PutUint16(data[offset:], uint16(colNameLen))
	offset += 2
	copy(data[offset:], m.ColumnName)
	offset += colNameLen

	// Column type
	data[offset] = byte(m.ColumnType)
	offset++

	// Unique flag
	if m.Unique {
		data[offset] = 1
	} else {
		data[offset] = 0
	}

	return data
}

// UnmarshalIndexMetadata deserializes index metadata.
func UnmarshalIndexMetadata(data []byte) (*IndexMetadata, error) {
	if len(data) < 6 {
		return nil, fmt.Errorf("invalid index metadata")
	}

	offset := 0

	// Table name
	tableNameLen := binary.LittleEndian.Uint16(data[offset:])
	offset += 2
	if offset+int(tableNameLen) > len(data) {
		return nil, fmt.Errorf("corrupted index metadata")
	}
	tableName := string(data[offset : offset+int(tableNameLen)])
	offset += int(tableNameLen)

	// Column name
	colNameLen := binary.LittleEndian.Uint16(data[offset:])
	offset += 2
	if offset+int(colNameLen) > len(data) {
		return nil, fmt.Errorf("corrupted index metadata")
	}
	colName := string(data[offset : offset+int(colNameLen)])
	offset += int(colNameLen)

	// Column type
	if offset+2 > len(data) {
		return nil, fmt.Errorf("corrupted index metadata")
	}
	colType := table.DataType(data[offset])
	offset++

	// Unique flag
	unique := data[offset] != 0

	return &IndexMetadata{
		TableName:  tableName,
		ColumnName: colName,
		ColumnType: colType,
		Unique:     unique,
	}, nil
}

// ListIndexes returns all indexes for a given table.
func ListIndexes(tableName string, store *kv.KV) ([]string, error) {
	prefix := []byte(fmt.Sprintf("__index__:%s:", tableName))
	endKey := []byte(fmt.Sprintf("__index__:%s:\xFF", tableName))

	var indexes []string
	err := store.Scan(prefix, endKey, func(key, value []byte) bool {
		// Extract column name from key
		parts := bytes.Split(key, []byte(":"))
		if len(parts) >= 3 {
			indexes = append(indexes, string(parts[2]))
		}
		return true
	})

	return indexes, err
}

// equalPrimaryKeys checks if two primary keys are equal.
func equalPrimaryKeys(pk1, pk2 interface{}) bool {
	switch v1 := pk1.(type) {
	case int64:
		v2, ok := pk2.(int64)
		return ok && v1 == v2
	case string:
		v2, ok := pk2.(string)
		return ok && v1 == v2
	case []byte:
		v2, ok := pk2.([]byte)
		if !ok {
			return false
		}
		return bytes.Equal(v1, v2)
	default:
		return false
	}
}
