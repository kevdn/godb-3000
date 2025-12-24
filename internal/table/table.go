package table

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/khoale/godb-3000/internal/kv"
)

// Table represents a relational table built on top of a KV store.
// Each table is stored with a prefix to isolate its data in the KV namespace.
//
// Key structure:
// - Schema: "__schema__:<table_name>" -> serialized schema
// - Rows: "<table_name>:<primary_key>" -> serialized row
// - Auto-increment counter: "__autoinc__:<table_name>" -> int64
//
// This design allows multiple tables to coexist in a single KV store.
type Table struct {
	name      string
	schema    *Schema
	store     *kv.KV
	prefix    string     // Key prefix for this table's data
	counterMu sync.Mutex // Protects auto-increment counter operations
}

// NewTable creates a new table with the given schema.
func NewTable(name string, schema *Schema, store *kv.KV) (*Table, error) {
	if name == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}
	if schema.PrimaryKey == "" {
		return nil, fmt.Errorf("table must have a primary key")
	}

	t := &Table{
		name:   name,
		schema: schema.Clone(),
		store:  store,
		prefix: name + ":",
	}

	// Save schema to KV store
	schemaKey := []byte("__schema__:" + name)
	schemaData := schema.Marshal()
	if err := store.Set(schemaKey, schemaData); err != nil {
		return nil, fmt.Errorf("failed to save schema: %w", err)
	}

	return t, nil
}

// LoadTable loads an existing table from the KV store.
func LoadTable(name string, store *kv.KV) (*Table, error) {
	if name == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	// Load schema
	schemaKey := []byte("__schema__:" + name)
	schemaData, found, err := store.Get(schemaKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load schema: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("table %s does not exist", name)
	}

	schema, err := UnmarshalSchema(schemaData)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	return &Table{
		name:   name,
		schema: schema,
		store:  store,
		prefix: name + ":",
	}, nil
}

// Name returns the table name.
func (t *Table) Name() string {
	return t.name
}

// Schema returns the table schema.
func (t *Table) Schema() *Schema {
	return t.schema.Clone()
}

// Insert inserts a new row into the table.
// If the primary key is auto-increment and the value is nil, it will be generated automatically.
func (t *Table) Insert(row *Row) error {
	// Get primary key column
	pkIndex, err := t.schema.GetColumnIndex(t.schema.PrimaryKey)
	if err != nil {
		return err
	}

	pkCol, err := t.schema.GetColumn(t.schema.PrimaryKey)
	if err != nil {
		return err
	}

	pkValue := row.Values[pkIndex]

	// Auto-generate ID if primary key is nil and column is auto-increment
	if pkValue == nil && pkCol.AutoIncrement {
		nextID, err := t.getNextAutoIncrementID()
		if err != nil {
			return fmt.Errorf("failed to generate auto-increment ID: %w", err)
		}
		row.Values[pkIndex] = nextID
		pkValue = nextID
	} else if pkValue == nil {
		return fmt.Errorf("primary key cannot be NULL")
	}

	// If user provided manual ID and column is auto-increment, update counter if needed
	if pkCol.AutoIncrement {
		if manualID, ok := pkValue.(int64); ok {
			if err := t.updateAutoIncrementCounter(manualID); err != nil {
				return fmt.Errorf("failed to update auto-increment counter: %w", err)
			}
		}
	}

	// Validate row against schema
	if err := t.schema.Validate(row); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Generate row key
	rowKey, err := t.makeRowKey(pkValue)
	if err != nil {
		return err
	}

	// Check if key already exists
	_, exists, err := t.store.Get(rowKey)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("primary key already exists: %v", pkValue)
	}

	// Serialize and store row
	rowData, err := row.Marshal(t.schema)
	if err != nil {
		return fmt.Errorf("failed to marshal row: %w", err)
	}

	return t.store.Set(rowKey, rowData)
}

// Update updates an existing row identified by the primary key.
func (t *Table) Update(pkValue interface{}, row *Row) error {
	// Validate row against schema
	if err := t.schema.Validate(row); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Get primary key value from row
	pkIndex, err := t.schema.GetColumnIndex(t.schema.PrimaryKey)
	if err != nil {
		return err
	}

	rowPKValue := row.Values[pkIndex]
	if rowPKValue == nil {
		return fmt.Errorf("primary key cannot be NULL")
	}

	// Verify that the row's primary key matches the pkValue parameter
	if !valuesEqual(pkValue, rowPKValue) {
		return fmt.Errorf("primary key mismatch: row contains %v but update key is %v", rowPKValue, pkValue)
	}

	// Generate row key
	rowKey, err := t.makeRowKey(pkValue)
	if err != nil {
		return err
	}

	// Check if row exists
	_, exists, err := t.store.Get(rowKey)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("row not found: %v", pkValue)
	}

	// Serialize and store updated row
	rowData, err := row.Marshal(t.schema)
	if err != nil {
		return fmt.Errorf("failed to marshal row: %w", err)
	}

	return t.store.Set(rowKey, rowData)
}

// Delete deletes a row by primary key.
func (t *Table) Delete(pkValue interface{}) error {
	rowKey, err := t.makeRowKey(pkValue)
	if err != nil {
		return err
	}

	deleted, err := t.store.Delete(rowKey)
	if err != nil {
		return err
	}
	if !deleted {
		return fmt.Errorf("row not found: %v", pkValue)
	}

	return nil
}

// Get retrieves a row by primary key.
func (t *Table) Get(pkValue interface{}) (*Row, error) {
	rowKey, err := t.makeRowKey(pkValue)
	if err != nil {
		return nil, err
	}

	rowData, found, err := t.store.Get(rowKey)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("row not found: %v", pkValue)
	}

	row, err := UnmarshalRow(rowData, t.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal row: %w", err)
	}

	return row, nil
}

// Scan performs a range scan over all rows in the table.
// The callback is called for each row. Return false to stop iteration.
func (t *Table) Scan(callback func(row *Row) bool) error {
	// Start and end keys for this table's data
	startKey := []byte(t.prefix)
	endKey := []byte(t.prefix + "\xff") // All keys with this prefix

	return t.store.Scan(startKey, endKey, func(key, value []byte) bool {
		row, err := UnmarshalRow(value, t.schema)
		if err != nil {
			// Skip corrupted rows
			return true
		}
		return callback(row)
	})
}

// ScanRange performs a range scan between two primary key values.
func (t *Table) ScanRange(startPK, endPK interface{}, callback func(row *Row) bool) error {
	var startKey, endKey []byte
	var err error

	if startPK != nil {
		startKey, err = t.makeRowKey(startPK)
		if err != nil {
			return err
		}
	} else {
		startKey = []byte(t.prefix)
	}

	if endPK != nil {
		endKey, err = t.makeRowKey(endPK)
		if err != nil {
			return err
		}
	} else {
		endKey = []byte(t.prefix + "\xff")
	}

	return t.store.Scan(startKey, endKey, func(key, value []byte) bool {
		row, err := UnmarshalRow(value, t.schema)
		if err != nil {
			return true
		}
		return callback(row)
	})
}

// Count returns the number of rows in the table.
func (t *Table) Count() (int, error) {
	count := 0
	err := t.Scan(func(row *Row) bool {
		count++
		return true
	})
	return count, err
}

// valuesEqual compares two primary key values for equality.
// Supports int64, string, and []byte types.
func valuesEqual(v1, v2 interface{}) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}

	switch val1 := v1.(type) {
	case int64:
		val2, ok := v2.(int64)
		return ok && val1 == val2
	case string:
		val2, ok := v2.(string)
		return ok && val1 == val2
	case []byte:
		val2, ok := v2.([]byte)
		if !ok {
			return false
		}
		if len(val1) != len(val2) {
			return false
		}
		for i := range val1 {
			if val1[i] != val2[i] {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// makeRowKey generates a key for storing a row based on its primary key value.
// Key format: "<table_name>:<encoded_pk_value>"
func (t *Table) makeRowKey(pkValue interface{}) ([]byte, error) {
	// Find primary key column
	pkCol, err := t.schema.GetColumn(t.schema.PrimaryKey)
	if err != nil {
		return nil, err
	}

	// Encode primary key value based on type
	var encodedPK []byte

	switch pkCol.Type {
	case TypeInt:
		v, ok := pkValue.(int64)
		if !ok {
			return nil, fmt.Errorf("primary key must be int64, got %T", pkValue)
		}
		encodedPK = make([]byte, 8)
		binary.BigEndian.PutUint64(encodedPK, uint64(v))

	case TypeString:
		v, ok := pkValue.(string)
		if !ok {
			return nil, fmt.Errorf("primary key must be string, got %T", pkValue)
		}
		encodedPK = []byte(v)

	case TypeBytes:
		v, ok := pkValue.([]byte)
		if !ok {
			return nil, fmt.Errorf("primary key must be []byte, got %T", pkValue)
		}
		encodedPK = v

	default:
		return nil, fmt.Errorf("unsupported primary key type: %s", pkCol.Type)
	}

	// Combine prefix and encoded PK
	key := make([]byte, len(t.prefix)+len(encodedPK))
	copy(key, t.prefix)
	copy(key[len(t.prefix):], encodedPK)

	return key, nil
}

// Drop removes the table and all its data from the KV store.
func (t *Table) Drop() error {
	// Delete schema
	schemaKey := []byte("__schema__:" + t.name)
	if _, err := t.store.Delete(schemaKey); err != nil {
		return err
	}

	// Delete auto-increment counter
	counterKey := []byte("__autoinc__:" + t.name)
	t.store.Delete(counterKey) // Ignore error if doesn't exist

	// Delete all rows
	startKey := []byte(t.prefix)
	endKey := []byte(t.prefix + "\xff")

	// Collect all keys to delete
	var keysToDelete [][]byte
	err := t.store.Scan(startKey, endKey, func(key, value []byte) bool {
		keysToDelete = append(keysToDelete, append([]byte{}, key...))
		return true
	})
	if err != nil {
		return err
	}

	// Delete all collected keys
	for _, key := range keysToDelete {
		if _, err := t.store.Delete(key); err != nil {
			return err
		}
	}

	return nil
}

// Truncate removes all rows from the table but keeps the schema.
func (t *Table) Truncate() error {
	startKey := []byte(t.prefix)
	endKey := []byte(t.prefix + "\xff")

	// Collect all keys to delete
	var keysToDelete [][]byte
	err := t.store.Scan(startKey, endKey, func(key, value []byte) bool {
		keysToDelete = append(keysToDelete, append([]byte{}, key...))
		return true
	})
	if err != nil {
		return err
	}

	// Delete all collected keys
	for _, key := range keysToDelete {
		if _, err := t.store.Delete(key); err != nil {
			return err
		}
	}

	// Reset auto-increment counter if table has auto-increment column
	if err := t.resetAutoIncrementCounter(); err != nil {
		return fmt.Errorf("failed to reset auto-increment counter: %w", err)
	}

	return nil
}

// ListTables returns the names of all tables in the KV store.
func ListTables(store *kv.KV) ([]string, error) {
	prefix := []byte("__schema__:")
	endKey := []byte("__schema__:\xff")

	var tables []string
	err := store.Scan(prefix, endKey, func(key, value []byte) bool {
		// Extract table name from key
		tableName := string(key[len(prefix):])
		tables = append(tables, tableName)
		return true
	})

	return tables, err
}

// TableExists checks if a table exists in the KV store.
func TableExists(name string, store *kv.KV) (bool, error) {
	schemaKey := []byte("__schema__:" + name)
	_, found, err := store.Get(schemaKey)
	return found, err
}

// getNextAutoIncrementID returns the next auto-increment ID for the table.
func (t *Table) getNextAutoIncrementID() (int64, error) {
	t.counterMu.Lock()
	defer t.counterMu.Unlock()

	counterKey := []byte("__autoinc__:" + t.name)

	// Get current counter value
	data, found, err := t.store.Get(counterKey)
	if err != nil {
		return 0, fmt.Errorf("failed to read counter: %w", err)
	}

	var nextID int64 = 1
	if found {
		currentID := int64(binary.LittleEndian.Uint64(data))
		nextID = currentID + 1
	}

	// Update counter
	newData := make([]byte, 8)
	binary.LittleEndian.PutUint64(newData, uint64(nextID))
	if err := t.store.Set(counterKey, newData); err != nil {
		return 0, fmt.Errorf("failed to update counter: %w", err)
	}

	return nextID, nil
}

// updateAutoIncrementCounter updates the counter if the provided ID is greater than current counter.
// This handles the case where users manually provide IDs.
func (t *Table) updateAutoIncrementCounter(providedID int64) error {
	t.counterMu.Lock()
	defer t.counterMu.Unlock()

	counterKey := []byte("__autoinc__:" + t.name)

	// Get current counter value
	data, found, err := t.store.Get(counterKey)
	if err != nil {
		return fmt.Errorf("failed to read counter: %w", err)
	}

	var currentID int64 = 0
	if found {
		currentID = int64(binary.LittleEndian.Uint64(data))
	}

	// Update counter only if provided ID is greater
	if providedID > currentID {
		newData := make([]byte, 8)
		binary.LittleEndian.PutUint64(newData, uint64(providedID))
		if err := t.store.Set(counterKey, newData); err != nil {
			return fmt.Errorf("failed to update counter: %w", err)
		}
	}

	return nil
}

// resetAutoIncrementCounter resets the auto-increment counter to 0.
// Used when truncating a table.
func (t *Table) resetAutoIncrementCounter() error {
	// Check if table has auto-increment column
	hasAutoInc := false
	for _, col := range t.schema.Columns {
		if col.AutoIncrement {
			hasAutoInc = true
			break
		}
	}

	if !hasAutoInc {
		return nil // No auto-increment column, nothing to reset
	}

	counterKey := []byte("__autoinc__:" + t.name)
	_, err := t.store.Delete(counterKey)
	return err
}
