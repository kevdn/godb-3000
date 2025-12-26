package index

import (
	"path/filepath"
	"testing"

	"github.com/khoale/godb-3000/internal/kv"
	"github.com/khoale/godb-3000/internal/table"
)

// setupTestIndex creates a test schema and KV store for index testing
func setupTestIndex(t *testing.T) (*table.Schema, *kv.KV, string) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	store, err := kv.Open(dbPath, kv.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open KV store: %v", err)
	}

	// Create schema
	schema := table.NewSchema("users")
	schema.AddColumn(&table.Column{
		Name:       "id",
		Type:       table.TypeInt,
		PrimaryKey: true,
	})
	schema.AddColumn(&table.Column{
		Name: "name",
		Type: table.TypeString,
	})
	schema.AddColumn(&table.Column{
		Name: "age",
		Type: table.TypeInt,
	})
	schema.AddColumn(&table.Column{
		Name: "email",
		Type: table.TypeString,
	})
	schema.AddColumn(&table.Column{
		Name: "active",
		Type: table.TypeBool,
	})
	schema.AddColumn(&table.Column{
		Name: "score",
		Type: table.TypeFloat,
	})
	schema.AddColumn(&table.Column{
		Name: "data",
		Type: table.TypeBytes,
	})

	return schema, store, dbPath
}

// TestIndexCreateIndex tests CreateIndex function
func TestIndexCreateIndex(t *testing.T) {
	schema, store, _ := setupTestIndex(t)
	defer store.Close()

	// Valid index creation
	idx, err := CreateIndex("users", "name", schema, store, false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}
	if idx == nil {
		t.Fatal("CreateIndex returned nil index")
	}
	if idx.tableName != "users" {
		t.Errorf("Expected tableName=users, got %s", idx.tableName)
	}
	if idx.columnName != "name" {
		t.Errorf("Expected columnName=name, got %s", idx.columnName)
	}
	if idx.unique {
		t.Error("Expected unique=false")
	}

	// Unique index
	idx2, err := CreateIndex("users", "email", schema, store, true)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}
	if !idx2.unique {
		t.Error("Expected unique=true")
	}
}

// TestIndexCreateIndexErrors tests CreateIndex error cases
func TestIndexCreateIndexErrors(t *testing.T) {
	schema, store, _ := setupTestIndex(t)
	defer store.Close()

	t.Run("EmptyTableName", func(t *testing.T) {
		_, err := CreateIndex("", "name", schema, store, false)
		if err == nil {
			t.Error("Expected error for empty table name")
		}
	})

	t.Run("EmptyColumnName", func(t *testing.T) {
		_, err := CreateIndex("users", "", schema, store, false)
		if err == nil {
			t.Error("Expected error for empty column name")
		}
	})

	t.Run("ColumnNotFound", func(t *testing.T) {
		_, err := CreateIndex("users", "nonexistent", schema, store, false)
		if err == nil {
			t.Error("Expected error for non-existent column")
		}
	})

	t.Run("PrimaryKeyColumn", func(t *testing.T) {
		_, err := CreateIndex("users", "id", schema, store, false)
		if err == nil {
			t.Error("Expected error for primary key column")
		}
	})
}

// TestIndexLoadIndex tests LoadIndex function
func TestIndexLoadIndex(t *testing.T) {
	schema, store, _ := setupTestIndex(t)
	defer store.Close()

	// Create index first
	_, err := CreateIndex("users", "name", schema, store, false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Load existing index
	idx, err := LoadIndex("users", "name", store)
	if err != nil {
		t.Fatalf("LoadIndex failed: %v", err)
	}
	if idx == nil {
		t.Fatal("LoadIndex returned nil")
	}
	if idx.tableName != "users" || idx.columnName != "name" {
		t.Errorf("Loaded index metadata incorrect: %+v", idx)
	}

	// Load non-existent index
	_, err = LoadIndex("users", "nonexistent", store)
	if err == nil {
		t.Error("Expected error for non-existent index")
	}
}

// TestIndexInsertDelete tests Insert and Delete operations
func TestIndexInsertDelete(t *testing.T) {
	schema, store, _ := setupTestIndex(t)
	defer store.Close()

	idx, err := CreateIndex("users", "name", schema, store, false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Insert
	err = idx.Insert("alice", int64(1))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Verify exists
	exists, err := idx.Exists("alice")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("Value should exist after insert")
	}

	// Lookup
	pks, err := idx.Lookup("alice")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}
	if len(pks) != 1 || pks[0].(int64) != 1 {
		t.Errorf("Lookup returned wrong PKs: %v", pks)
	}

	// Delete
	err = idx.Delete("alice", int64(1))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	exists, err = idx.Exists("alice")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("Value should not exist after delete")
	}
}

// TestIndexNullHandling tests NULL value handling
func TestIndexNullHandling(t *testing.T) {
	schema, store, _ := setupTestIndex(t)
	defer store.Close()

	idx, err := CreateIndex("users", "name", schema, store, false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Insert with NULL should be no-op
	err = idx.Insert(nil, int64(1))
	if err != nil {
		t.Fatalf("Insert with NULL should succeed (no-op): %v", err)
	}

	// Lookup NULL should return empty
	pks, err := idx.Lookup(nil)
	if err != nil {
		t.Fatalf("Lookup NULL failed: %v", err)
	}
	if len(pks) != 0 {
		t.Errorf("Lookup NULL should return empty, got %v", pks)
	}

	// Delete with NULL should be no-op
	err = idx.Delete(nil, int64(1))
	if err != nil {
		t.Fatalf("Delete with NULL should succeed (no-op): %v", err)
	}
}

// TestIndexDataTypes tests different data types
func TestIndexDataTypes(t *testing.T) {
	schema, store, _ := setupTestIndex(t)
	defer store.Close()

	t.Run("String", func(t *testing.T) {
		idx, err := CreateIndex("users", "name", schema, store, false)
		if err != nil {
			t.Fatalf("CreateIndex failed: %v", err)
		}

		err = idx.Insert("alice", int64(1))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		pks, err := idx.Lookup("alice")
		if err != nil {
			t.Fatalf("Lookup failed: %v", err)
		}
		if len(pks) != 1 {
			t.Errorf("Expected 1 PK, got %d", len(pks))
		}
	})

	t.Run("Int", func(t *testing.T) {
		idx, err := CreateIndex("users", "age", schema, store, false)
		if err != nil {
			t.Fatalf("CreateIndex failed: %v", err)
		}

		err = idx.Insert(int64(25), int64(1))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		pks, err := idx.Lookup(int64(25))
		if err != nil {
			t.Fatalf("Lookup failed: %v", err)
		}
		if len(pks) != 1 {
			t.Errorf("Expected 1 PK, got %d", len(pks))
		}
	})

	t.Run("Bool", func(t *testing.T) {
		idx, err := CreateIndex("users", "active", schema, store, false)
		if err != nil {
			t.Fatalf("CreateIndex failed: %v", err)
		}

		err = idx.Insert(true, int64(1))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		pks, err := idx.Lookup(true)
		if err != nil {
			t.Fatalf("Lookup failed: %v", err)
		}
		if len(pks) != 1 {
			t.Errorf("Expected 1 PK, got %d", len(pks))
		}
	})

	t.Run("Float", func(t *testing.T) {
		idx, err := CreateIndex("users", "score", schema, store, false)
		if err != nil {
			t.Fatalf("CreateIndex failed: %v", err)
		}

		err = idx.Insert(95.5, int64(1))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		pks, err := idx.Lookup(95.5)
		if err != nil {
			t.Fatalf("Lookup failed: %v", err)
		}
		if len(pks) != 1 {
			t.Errorf("Expected 1 PK, got %d", len(pks))
		}
	})

	t.Run("Bytes", func(t *testing.T) {
		idx, err := CreateIndex("users", "data", schema, store, false)
		if err != nil {
			t.Fatalf("CreateIndex failed: %v", err)
		}

		data := []byte{0x01, 0x02, 0x03}
		err = idx.Insert(data, int64(1))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		pks, err := idx.Lookup(data)
		if err != nil {
			t.Fatalf("Lookup failed: %v", err)
		}
		if len(pks) != 1 {
			t.Errorf("Expected 1 PK, got %d", len(pks))
		}
	})
}

// TestIndexScanRange tests ScanRange operation
func TestIndexScanRange(t *testing.T) {
	schema, store, _ := setupTestIndex(t)
	defer store.Close()

	idx, err := CreateIndex("users", "age", schema, store, false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Insert multiple values
	ages := []int64{20, 25, 30, 35, 40}
	for i, age := range ages {
		err = idx.Insert(age, int64(i+1))
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Range scan
	pks, err := idx.ScanRange(int64(25), int64(35))
	if err != nil {
		t.Fatalf("ScanRange failed: %v", err)
	}
	if len(pks) != 3 {
		t.Errorf("Expected 3 PKs in range [25,35], got %d", len(pks))
	}

	// Full scan (nil bounds)
	pks, err = idx.ScanRange(nil, nil)
	if err != nil {
		t.Fatalf("ScanRange failed: %v", err)
	}
	if len(pks) != 5 {
		t.Errorf("Expected 5 PKs in full scan, got %d", len(pks))
	}

	// Start bound only
	pks, err = idx.ScanRange(int64(30), nil)
	if err != nil {
		t.Fatalf("ScanRange failed: %v", err)
	}
	if len(pks) != 3 {
		t.Errorf("Expected 3 PKs from 30 onwards, got %d", len(pks))
	}

	// End bound only
	pks, err = idx.ScanRange(nil, int64(30))
	if err != nil {
		t.Fatalf("ScanRange failed: %v", err)
	}
	if len(pks) != 3 {
		t.Errorf("Expected 3 PKs up to 30, got %d", len(pks))
	}
}

// TestIndexDrop tests Drop operation
func TestIndexDrop(t *testing.T) {
	schema, store, _ := setupTestIndex(t)
	defer store.Close()

	idx, err := CreateIndex("users", "name", schema, store, false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// Insert some data
	err = idx.Insert("alice", int64(1))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Drop index
	err = idx.Drop()
	if err != nil {
		t.Fatalf("Drop failed: %v", err)
	}

	// Verify index is gone
	_, err = LoadIndex("users", "name", store)
	if err == nil {
		t.Error("Index should not exist after drop")
	}

	// Verify data is gone
	exists, err := idx.Exists("alice")
	if err == nil && exists {
		t.Error("Index data should be deleted")
	}
}

// TestIndexListIndexes tests ListIndexes function
func TestIndexListIndexes(t *testing.T) {
	schema, store, _ := setupTestIndex(t)
	defer store.Close()

	// Create multiple indexes
	_, err := CreateIndex("users", "name", schema, store, false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}
	_, err = CreateIndex("users", "email", schema, store, true)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}
	_, err = CreateIndex("users", "age", schema, store, false)
	if err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	// List indexes
	indexes, err := ListIndexes("users", store)
	if err != nil {
		t.Fatalf("ListIndexes failed: %v", err)
	}
	if len(indexes) != 3 {
		t.Errorf("Expected 3 indexes, got %d: %v", len(indexes), indexes)
	}

	// Verify all indexes are present
	expected := map[string]bool{"name": true, "email": true, "age": true}
	for _, idxName := range indexes {
		if !expected[idxName] {
			t.Errorf("Unexpected index: %s", idxName)
		}
		delete(expected, idxName)
	}
	if len(expected) > 0 {
		t.Errorf("Missing indexes: %v", expected)
	}
}

// TestIndexNonUniqueIndexAllowsDuplicates verifies that non-unique indexes
// should allow multiple rows with the same column value
func TestIndexNonUniqueIndexAllowsDuplicates(t *testing.T) {
	schema, store, _ := setupTestIndex(t)
	defer store.Close()

	// Create NON-UNIQUE index on age column
	ageIndex, err := CreateIndex("users", "age", schema, store, false)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Insert first user with age=25
	err = ageIndex.Insert(int64(25), int64(1))
	if err != nil {
		t.Fatalf("First insert failed: %v", err)
	}

	// Insert second user with SAME age=25 but different PK
	err = ageIndex.Insert(int64(25), int64(2))
	if err != nil {
		t.Errorf("Second insert should succeed for non-unique index, got error: %v", err)
	}

	// Insert third user with age=25
	err = ageIndex.Insert(int64(25), int64(3))
	if err != nil {
		t.Errorf("Third insert should succeed for non-unique index, got error: %v", err)
	}

	// Verify all three users are in the index
	pks, err := ageIndex.Lookup(int64(25))
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}

	expectedCount := 3
	if len(pks) != expectedCount {
		t.Errorf("Expected %d PKs for age=25, got %d: %v", expectedCount, len(pks), pks)
	}

	// Verify the PKs are correct
	expectedPKs := map[int64]bool{1: true, 2: true, 3: true}
	for _, pk := range pks {
		pkInt, ok := pk.(int64)
		if !ok {
			t.Errorf("Expected int64 PK, got %T", pk)
			continue
		}
		if !expectedPKs[pkInt] {
			t.Errorf("Unexpected PK: %d", pkInt)
		}
		delete(expectedPKs, pkInt)
	}

	if len(expectedPKs) > 0 {
		t.Errorf("Missing PKs: %v", expectedPKs)
	}
}

// TestIndexUniqueIndexRejectsDuplicates verifies that unique indexes
// should reject multiple rows with the same column value
func TestIndexUniqueIndexRejectsDuplicates(t *testing.T) {
	schema, store, _ := setupTestIndex(t)
	defer store.Close()

	// Create UNIQUE index on email column
	emailIndex, err := CreateIndex("users", "email", schema, store, true)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Insert first user
	err = emailIndex.Insert("alice@example.com", int64(1))
	if err != nil {
		t.Fatalf("First insert failed: %v", err)
	}

	// Try to insert second user with SAME email
	err = emailIndex.Insert("alice@example.com", int64(2))
	if err == nil {
		t.Errorf("Second insert should fail for unique index")
	}

	// Verify error message
	if err != nil && err.Error() != "unique constraint violation: value already exists" {
		t.Errorf("Expected unique constraint error, got: %v", err)
	}

	// Verify only one user is in the index
	pks, err := emailIndex.Lookup("alice@example.com")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}

	if len(pks) != 1 {
		t.Errorf("Expected 1 PK for email, got %d", len(pks))
	}

	if len(pks) > 0 {
		if pk, ok := pks[0].(int64); !ok || pk != 1 {
			t.Errorf("Expected PK=1, got %v", pks[0])
		}
	}
}

// TestIndexUniqueIndexAllowsSamePKReinsert verifies that reinserting
// the same (value, PK) pair should be allowed (idempotent)
func TestIndexUniqueIndexAllowsSamePKReinsert(t *testing.T) {
	schema, store, _ := setupTestIndex(t)
	defer store.Close()

	emailIndex, err := CreateIndex("users", "email", schema, store, true)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Insert first time
	err = emailIndex.Insert("alice@example.com", int64(1))
	if err != nil {
		t.Fatalf("First insert failed: %v", err)
	}

	// Re-insert with SAME PK (idempotent operation)
	err = emailIndex.Insert("alice@example.com", int64(1))
	if err != nil {
		t.Errorf("Idempotent re-insert should succeed, got error: %v", err)
	}

	// Verify still only one entry
	pks, err := emailIndex.Lookup("alice@example.com")
	if err != nil {
		t.Fatalf("Lookup failed: %v", err)
	}

	if len(pks) != 1 {
		t.Errorf("Expected 1 PK after re-insert, got %d", len(pks))
	}
}

// TestIndexMetadataSerialization tests metadata marshal/unmarshal
func TestIndexMetadataSerialization(t *testing.T) {
	meta := &IndexMetadata{
		TableName:  "users",
		ColumnName: "email",
		ColumnType: table.TypeString,
		Unique:     true,
	}

	// Marshal
	data := meta.Marshal()
	if len(data) == 0 {
		t.Fatal("Marshal returned empty data")
	}

	// Unmarshal
	unmarshaled, err := UnmarshalIndexMetadata(data)
	if err != nil {
		t.Fatalf("UnmarshalIndexMetadata failed: %v", err)
	}

	if unmarshaled.TableName != meta.TableName {
		t.Errorf("TableName mismatch: expected %s, got %s", meta.TableName, unmarshaled.TableName)
	}
	if unmarshaled.ColumnName != meta.ColumnName {
		t.Errorf("ColumnName mismatch: expected %s, got %s", meta.ColumnName, unmarshaled.ColumnName)
	}
	if unmarshaled.ColumnType != meta.ColumnType {
		t.Errorf("ColumnType mismatch: expected %d, got %d", meta.ColumnType, unmarshaled.ColumnType)
	}
	if unmarshaled.Unique != meta.Unique {
		t.Errorf("Unique mismatch: expected %v, got %v", meta.Unique, unmarshaled.Unique)
	}
}

// TestIndexMetadataErrors tests error cases for metadata unmarshaling
func TestIndexMetadataErrors(t *testing.T) {
	// Too short
	_, err := UnmarshalIndexMetadata([]byte{1, 2, 3})
	if err == nil {
		t.Error("Expected error for too short data")
	}

	// Corrupted data (invalid length fields)
	corrupted := make([]byte, 100)
	corrupted[0] = 0xFF // Invalid table name length
	corrupted[1] = 0xFF
	_, err = UnmarshalIndexMetadata(corrupted)
	if err == nil {
		t.Error("Expected error for corrupted data")
	}
}
