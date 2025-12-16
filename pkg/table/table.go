package table

import (
	"encoding/binary"
	"fmt"

	"github.com/khoale/godb-3000/pkg/kv"
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
	name   string
	schema *Schema
	store  *kv.KV
	prefix string // Key prefix for this table's data
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
// The row must include a value for the primary key.
func (t *Table) Insert(row *Row) error {
	// Validate row against schema
	if err := t.schema.Validate(row); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	// Get primary key value
	pkIndex, err := t.schema.GetColumnIndex(t.schema.PrimaryKey)
	if err != nil {
		return err
	}

	pkValue := row.Values[pkIndex]
	if pkValue == nil {
		return fmt.Errorf("primary key cannot be NULL")
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
