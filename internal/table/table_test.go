package table

import (
	"path/filepath"
	"testing"

	"github.com/khoale/godb-3000/internal/kv"
)

// setupTestTable creates a test table with auto-increment
func setupTestTable(t *testing.T, autoInc bool) (*Table, *kv.KV, string) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	store, err := kv.Open(dbPath, kv.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open KV store: %v", err)
	}

	// Create schema
	schema := NewSchema("test_table")
	schema.AddColumn(&Column{
		Name:          "id",
		Type:          TypeInt,
		PrimaryKey:    true,
		AutoIncrement: autoInc,
	})
	schema.AddColumn(&Column{
		Name:    "name",
		Type:    TypeString,
		NotNull: true,
	})
	schema.AddColumn(&Column{
		Name: "email",
		Type: TypeString,
	})

	tbl, err := NewTable("test_table", schema, store)
	if err != nil {
		store.Close()
		t.Fatalf("Failed to create table: %v", err)
	}

	return tbl, store, dbPath
}

// TestTableBasicOperations tests basic CRUD operations
func TestTableBasicOperations(t *testing.T) {
	tbl, store, _ := setupTestTable(t, false) // No auto-increment
	defer store.Close()

	t.Run("InsertAndGet", func(t *testing.T) {
		row := NewRow(int64(1), "Alice", "alice@example.com")
		if err := tbl.Insert(row); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		retrieved, err := tbl.Get(int64(1))
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if retrieved.Values[0] != int64(1) {
			t.Errorf("Expected id=1, got %v", retrieved.Values[0])
		}
		if retrieved.Values[1] != "Alice" {
			t.Errorf("Expected name=Alice, got %v", retrieved.Values[1])
		}
	})

	t.Run("Update", func(t *testing.T) {
		row := NewRow(int64(2), "Bob", "bob@example.com")
		tbl.Insert(row)

		updatedRow := NewRow(int64(2), "Robert", "robert@example.com")
		if err := tbl.Update(int64(2), updatedRow); err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		retrieved, _ := tbl.Get(int64(2))
		if retrieved.Values[1] != "Robert" {
			t.Errorf("Expected name=Robert, got %v", retrieved.Values[1])
		}
	})

	t.Run("Delete", func(t *testing.T) {
		row := NewRow(int64(3), "Charlie", "charlie@example.com")
		tbl.Insert(row)

		if err := tbl.Delete(int64(3)); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		_, err := tbl.Get(int64(3))
		if err == nil {
			t.Error("Expected error when getting deleted row")
		}
	})

	t.Run("DuplicatePrimaryKey", func(t *testing.T) {
		row1 := NewRow(int64(10), "User1", "user1@example.com")
		tbl.Insert(row1)

		row2 := NewRow(int64(10), "User2", "user2@example.com")
		err := tbl.Update(int64(10), row2)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		// Insert with same PK should fail
		row3 := NewRow(int64(10), "User3", "user3@example.com")
		err = tbl.Insert(row3)
		if err == nil {
			t.Error("Expected error for duplicate primary key")
		}
	})

	t.Run("GetNonExistent", func(t *testing.T) {
		_, err := tbl.Get(int64(999))
		if err == nil {
			t.Error("Expected error when getting non-existent row")
		}
	})

	t.Run("UpdateNonExistent", func(t *testing.T) {
		row := NewRow(int64(999), "NotFound", "notfound@example.com")
		err := tbl.Update(int64(999), row)
		if err == nil {
			t.Error("Expected error when updating non-existent row")
		}
	})

	t.Run("DeleteNonExistent", func(t *testing.T) {
		err := tbl.Delete(int64(999))
		if err == nil {
			t.Error("Expected error when deleting non-existent row")
		}
	})
}

// TestTableAutoIncrement tests auto-increment functionality
func TestTableAutoIncrement(t *testing.T) {
	tbl, store, _ := setupTestTable(t, true)
	defer store.Close()

	t.Run("Basic", func(t *testing.T) {
		// Nil ID should be auto-generated
		row := NewRow(nil, "Alice", "alice@example.com")
		if err := tbl.Insert(row); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		if row.Values[0] != int64(1) {
			t.Errorf("Expected ID=1, got %v", row.Values[0])
		}

		// Next insert should be ID 2
		row2 := NewRow(nil, "Bob", "bob@example.com")
		if err := tbl.Insert(row2); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		if row2.Values[0] != int64(2) {
			t.Errorf("Expected ID=2, got %v", row2.Values[0])
		}
	})

	t.Run("ManualID", func(t *testing.T) {
		// Manual ID should be accepted
		row := NewRow(int64(100), "Manual", "manual@example.com")
		if err := tbl.Insert(row); err != nil {
			t.Fatalf("Insert with manual ID failed: %v", err)
		}

		// Next auto ID should be 101
		row2 := NewRow(nil, "Auto", "auto@example.com")
		if err := tbl.Insert(row2); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		if row2.Values[0] != int64(101) {
			t.Errorf("Expected ID=101, got %v", row2.Values[0])
		}
	})

	t.Run("NilOnNonAutoIncrement", func(t *testing.T) {
		tbl2, store2, _ := setupTestTable(t, false)
		defer store2.Close()

		row := NewRow(nil, "Alice", "alice@example.com")
		err := tbl2.Insert(row)
		if err == nil {
			t.Error("Expected error for nil primary key on non-auto-increment column")
		}
	})

	t.Run("TruncateResetsCounter", func(t *testing.T) {
		// Insert some rows
		for i := 0; i < 3; i++ {
			row := NewRow(nil, "User", "user@example.com")
			tbl.Insert(row)
		}

		// Truncate
		if err := tbl.Truncate(); err != nil {
			t.Fatalf("Truncate failed: %v", err)
		}

		// Next insert should start from 1
		row := NewRow(nil, "AfterTruncate", "truncate@example.com")
		if err := tbl.Insert(row); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		if row.Values[0] != int64(1) {
			t.Errorf("Expected ID=1 after truncate, got %v", row.Values[0])
		}
	})
}

// TestTablePrimaryKeyTypes tests different primary key types
func TestTablePrimaryKeyTypes(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	store, err := kv.Open(dbPath, kv.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open KV store: %v", err)
	}
	defer store.Close()

	t.Run("StringPrimaryKey", func(t *testing.T) {
		schema := NewSchema("users")
		schema.AddColumn(&Column{
			Name:       "username",
			Type:       TypeString,
			PrimaryKey: true,
		})
		schema.AddColumn(&Column{
			Name: "email",
			Type: TypeString,
		})

		tbl, err := NewTable("users", schema, store)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		row := NewRow("alice", "alice@example.com")
		if err := tbl.Insert(row); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		retrieved, err := tbl.Get("alice")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if retrieved.Values[0] != "alice" {
			t.Errorf("Expected username=alice, got %v", retrieved.Values[0])
		}
	})

	t.Run("BytesPrimaryKey", func(t *testing.T) {
		schema := NewSchema("data")
		schema.AddColumn(&Column{
			Name:       "key",
			Type:       TypeBytes,
			PrimaryKey: true,
		})
		schema.AddColumn(&Column{
			Name: "value",
			Type: TypeString,
		})

		tbl, err := NewTable("data", schema, store)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		pk := []byte{0x01, 0x02, 0x03}
		row := NewRow(pk, "value1")
		if err := tbl.Insert(row); err != nil {
			t.Fatalf("Insert failed: %v", err)
		}

		retrieved, err := tbl.Get(pk)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		retrievedPK := retrieved.Values[0].([]byte)
		if len(retrievedPK) != len(pk) {
			t.Errorf("PK length mismatch")
		}
	})
}

// TestTableScan tests scan operations
func TestTableScan(t *testing.T) {
	tbl, store, _ := setupTestTable(t, false)
	defer store.Close()

	// Insert test data
	for i := 0; i < 5; i++ {
		row := NewRow(int64(i+1), "User", "user@example.com")
		tbl.Insert(row)
	}

	t.Run("FullScan", func(t *testing.T) {
		count := 0
		err := tbl.Scan(func(row *Row) bool {
			count++
			return true
		})
		if err != nil {
			t.Fatalf("Scan failed: %v", err)
		}

		if count != 5 {
			t.Errorf("Expected 5 rows, got %d", count)
		}
	})

	t.Run("ScanRange", func(t *testing.T) {
		count := 0
		err := tbl.ScanRange(int64(2), int64(4), func(row *Row) bool {
			count++
			return true
		})
		if err != nil {
			t.Fatalf("ScanRange failed: %v", err)
		}

		if count != 2 { // IDs 2 and 3
			t.Errorf("Expected 2 rows, got %d", count)
		}
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		count := 0
		err := tbl.Scan(func(row *Row) bool {
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

// TestTableCount tests count operation
func TestTableCount(t *testing.T) {
	tbl, store, _ := setupTestTable(t, false)
	defer store.Close()

	count, err := tbl.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected count=0, got %d", count)
	}

	// Insert some rows
	for i := 0; i < 10; i++ {
		row := NewRow(int64(i+1), "User", "user@example.com")
		tbl.Insert(row)
	}

	count, err = tbl.Count()
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 10 {
		t.Errorf("Expected count=10, got %d", count)
	}
}

// TestTableDrop tests drop operation
func TestTableDrop(t *testing.T) {
	tbl, store, _ := setupTestTable(t, false)
	defer store.Close()

	// Insert some data
	for i := 0; i < 5; i++ {
		row := NewRow(int64(i+1), "User", "user@example.com")
		tbl.Insert(row)
	}

	// Drop table
	if err := tbl.Drop(); err != nil {
		t.Fatalf("Drop failed: %v", err)
	}

	// Verify table doesn't exist
	exists, err := TableExists("test_table", store)
	if err != nil {
		t.Fatalf("TableExists failed: %v", err)
	}
	if exists {
		t.Error("Table should not exist after drop")
	}
}

// TestTableTruncate tests truncate operation
func TestTableTruncate(t *testing.T) {
	tbl, store, _ := setupTestTable(t, false)
	defer store.Close()

	// Insert some data
	for i := 0; i < 5; i++ {
		row := NewRow(int64(i+1), "User", "user@example.com")
		tbl.Insert(row)
	}

	// Truncate
	if err := tbl.Truncate(); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	// Verify table is empty but still exists
	count, _ := tbl.Count()
	if count != 0 {
		t.Errorf("Expected count=0 after truncate, got %d", count)
	}

	exists, _ := TableExists("test_table", store)
	if !exists {
		t.Error("Table should still exist after truncate")
	}
}

// TestTableListTables tests ListTables function
func TestTableListTables(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	store, err := kv.Open(dbPath, kv.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open KV store: %v", err)
	}
	defer store.Close()

	// Create multiple tables
	schema1 := NewSchema("table1")
	schema1.AddColumn(&Column{Name: "id", Type: TypeInt, PrimaryKey: true})
	_, _ = NewTable("table1", schema1, store)

	schema2 := NewSchema("table2")
	schema2.AddColumn(&Column{Name: "id", Type: TypeInt, PrimaryKey: true})
	_, _ = NewTable("table2", schema2, store)

	// List tables
	tables, err := ListTables(store)
	if err != nil {
		t.Fatalf("ListTables failed: %v", err)
	}

	if len(tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(tables))
	}
}

// TestTableTableExists tests TableExists function
func TestTableTableExists(t *testing.T) {
	_, store, _ := setupTestTable(t, false)
	defer store.Close()

	exists, err := TableExists("test_table", store)
	if err != nil {
		t.Fatalf("TableExists failed: %v", err)
	}
	if !exists {
		t.Error("Table should exist")
	}

	exists, err = TableExists("non_existent", store)
	if err != nil {
		t.Fatalf("TableExists failed: %v", err)
	}
	if exists {
		t.Error("Non-existent table should not exist")
	}
}

// TestTableLoadTable tests loading existing table
func TestTableLoadTable(t *testing.T) {
	tbl1, store1, dbPath := setupTestTable(t, false)

	// Insert data
	row := NewRow(int64(1), "Alice", "alice@example.com")
	tbl1.Insert(row)

	store1.Sync()
	store1.Close()

	// Reopen and load table
	store2, err := kv.Open(dbPath, kv.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer store2.Close()

	tbl2, err := LoadTable("test_table", store2)
	if err != nil {
		t.Fatalf("LoadTable failed: %v", err)
	}

	// Verify data
	retrieved, err := tbl2.Get(int64(1))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.Values[1] != "Alice" {
		t.Errorf("Expected name=Alice, got %v", retrieved.Values[1])
	}
}

// TestTableValidation tests schema validation
func TestTableValidation(t *testing.T) {
	t.Run("NotNullConstraint", func(t *testing.T) {
		tbl, store, _ := setupTestTable(t, false)
		defer store.Close()

		// name is NOT NULL, but we provide nil
		row := NewRow(int64(1), nil, "email@example.com")
		err := tbl.Insert(row)
		if err == nil {
			t.Error("Expected error for NULL value in NOT NULL column")
		}
	})

	t.Run("TypeMismatch", func(t *testing.T) {
		tbl, store, _ := setupTestTable(t, false)
		defer store.Close()

		// name should be string, but we provide int
		row := NewRow(int64(1), 123, "email@example.com")
		err := tbl.Insert(row)
		if err == nil {
			t.Error("Expected error for type mismatch")
		}
	})

	t.Run("PrimaryKeyMismatch", func(t *testing.T) {
		tbl, store, _ := setupTestTable(t, false)
		defer store.Close()

		row := NewRow(int64(1), "Alice", "alice@example.com")
		tbl.Insert(row)

		// Try to update with different PK in row
		updatedRow := NewRow(int64(2), "Bob", "bob@example.com")
		err := tbl.Update(int64(1), updatedRow)
		if err == nil {
			t.Error("Expected error for primary key mismatch")
		}
	})
}

// TestTableAutoIncrementValidation tests auto-increment validation
func TestTableAutoIncrementValidation(t *testing.T) {
	t.Run("AutoIncrementOnNonINT", func(t *testing.T) {
		schema := NewSchema("test")
		err := schema.AddColumn(&Column{
			Name:          "id",
			Type:          TypeString,
			PrimaryKey:    true,
			AutoIncrement: true,
		})
		if err == nil {
			t.Error("Expected error for AUTO_INCREMENT on non-INT column")
		}
	})

	t.Run("AutoIncrementOnNonPrimaryKey", func(t *testing.T) {
		schema := NewSchema("test")
		schema.AddColumn(&Column{
			Name:       "id",
			Type:       TypeInt,
			PrimaryKey: true,
		})
		err := schema.AddColumn(&Column{
			Name:          "count",
			Type:          TypeInt,
			AutoIncrement: true,
		})
		if err == nil {
			t.Error("Expected error for AUTO_INCREMENT on non-primary key column")
		}
	})
}

// TestTableNewTableErrors tests error cases for NewTable
func TestTableNewTableErrors(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	store, err := kv.Open(dbPath, kv.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open KV store: %v", err)
	}
	defer store.Close()

	t.Run("EmptyTableName", func(t *testing.T) {
		schema := NewSchema("test")
		schema.AddColumn(&Column{Name: "id", Type: TypeInt, PrimaryKey: true})
		_, err := NewTable("", schema, store)
		if err == nil {
			t.Error("Expected error for empty table name")
		}
	})

	t.Run("NilSchema", func(t *testing.T) {
		_, err := NewTable("test", nil, store)
		if err == nil {
			t.Error("Expected error for nil schema")
		}
	})

	t.Run("NoPrimaryKey", func(t *testing.T) {
		schema := NewSchema("test")
		schema.AddColumn(&Column{Name: "id", Type: TypeInt})
		_, err := NewTable("test", schema, store)
		if err == nil {
			t.Error("Expected error for table without primary key")
		}
	})
}

// TestTableLoadTableErrors tests error cases for LoadTable
func TestTableLoadTableErrors(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	store, err := kv.Open(dbPath, kv.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open KV store: %v", err)
	}
	defer store.Close()

	t.Run("EmptyTableName", func(t *testing.T) {
		_, err := LoadTable("", store)
		if err == nil {
			t.Error("Expected error for empty table name")
		}
	})

	t.Run("NonExistentTable", func(t *testing.T) {
		_, err := LoadTable("non_existent", store)
		if err == nil {
			t.Error("Expected error for non-existent table")
		}
	})
}
