package sql

import (
	"fmt"

	"github.com/khoale/godb-3000/internal/index"
	"github.com/khoale/godb-3000/internal/kv"
	"github.com/khoale/godb-3000/internal/table"
	"github.com/khoale/godb-3000/internal/transaction"
)

// Executor executes parsed SQL statements against the database.
type Executor struct {
	store  *kv.KV
	txnMgr *transaction.TxnManager
	curTxn *transaction.Transaction
}

// NewExecutor creates a new SQL executor.
func NewExecutor(store *kv.KV) *Executor {
	return &Executor{
		store:  store,
		txnMgr: transaction.NewTxnManager(store),
	}
}

// Execute executes a SQL statement and returns the result.
func (e *Executor) Execute(stmt Statement) (*Result, error) {
	switch s := stmt.(type) {
	case *CreateTableStmt:
		return e.executeCreateTable(s)
	case *DropTableStmt:
		return e.executeDropTable(s)
	case *InsertStmt:
		return e.executeInsert(s)
	case *SelectStmt:
		return e.executeSelect(s)
	case *UpdateStmt:
		return e.executeUpdate(s)
	case *DeleteStmt:
		return e.executeDelete(s)
	case *CreateIndexStmt:
		return e.executeCreateIndex(s)
	case *DropIndexStmt:
		return e.executeDropIndex(s)
	case *BeginStmt:
		return e.executeBegin(s)
	case *CommitStmt:
		return e.executeCommit(s)
	case *RollbackStmt:
		return e.executeRollback(s)
	default:
		return nil, fmt.Errorf("unsupported statement type")
	}
}

// Result represents the result of executing a SQL statement.
type Result struct {
	RowsAffected int64
	Rows         []*table.Row
	Columns      []string
	Message      string
}

// executeCreateTable executes a CREATE TABLE statement.
func (e *Executor) executeCreateTable(stmt *CreateTableStmt) (*Result, error) {
	// Check if table already exists
	exists, err := table.TableExists(stmt.TableName, e.store)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("table %s already exists", stmt.TableName)
	}

	// Build schema
	schema := table.NewSchema(stmt.TableName)

	for _, colDef := range stmt.Columns {
		col := &table.Column{
			Name:       colDef.Name,
			Type:       colDef.Type,
			PrimaryKey: colDef.PrimaryKey,
			NotNull:    colDef.NotNull,
			Unique:     colDef.Unique,
		}

		if err := schema.AddColumn(col); err != nil {
			return nil, fmt.Errorf("failed to add column %s: %w", colDef.Name, err)
		}
	}

	// Validate schema has a primary key
	if schema.PrimaryKey == "" {
		return nil, fmt.Errorf("table must have a primary key")
	}

	// Create table
	_, err = table.NewTable(stmt.TableName, schema, e.store)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// Sync to disk
	if err := e.store.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("Table %s created successfully", stmt.TableName),
	}, nil
}

// executeDropTable executes a DROP TABLE statement.
func (e *Executor) executeDropTable(stmt *DropTableStmt) (*Result, error) {
	// Load table
	tbl, err := table.LoadTable(stmt.TableName, e.store)
	if err != nil {
		return nil, fmt.Errorf("table not found: %w", err)
	}

	// Drop table
	if err := tbl.Drop(); err != nil {
		return nil, fmt.Errorf("failed to drop table: %w", err)
	}

	// Sync to disk
	if err := e.store.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("Table %s dropped successfully", stmt.TableName),
	}, nil
}

// updateIndexesForInsert updates all indexes after a row insert.
func (e *Executor) updateIndexesForInsert(tableName string, schema *table.Schema, row *table.Row) error {
	// Get all indexes for this table
	indexNames, err := index.ListIndexes(tableName, e.store)
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
		idx, err := index.LoadIndex(tableName, colName, e.store)
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

// executeInsert executes an INSERT statement.
func (e *Executor) executeInsert(stmt *InsertStmt) (*Result, error) {
	// Load table
	tbl, err := table.LoadTable(stmt.TableName, e.store)
	if err != nil {
		return nil, fmt.Errorf("table not found: %w", err)
	}

	schema := tbl.Schema()

	// Build row
	var row *table.Row
	if len(stmt.Columns) > 0 {
		// Specific columns provided
		if len(stmt.Columns) != len(stmt.Values) {
			return nil, fmt.Errorf("column count mismatch: %d columns, %d values", len(stmt.Columns), len(stmt.Values))
		}

		// Create row with all columns (fill with NULL by default)
		values := make([]interface{}, schema.NumColumns())
		for i := range values {
			values[i] = nil
		}

		// Fill in provided values
		for i, colName := range stmt.Columns {
			colIdx, err := schema.GetColumnIndex(colName)
			if err != nil {
				return nil, fmt.Errorf("column not found: %w", err)
			}
			values[colIdx] = stmt.Values[i]
		}

		row = table.NewRow(values...)
	} else {
		// All columns in order
		if len(stmt.Values) != schema.NumColumns() {
			return nil, fmt.Errorf("value count mismatch: expected %d, got %d", schema.NumColumns(), len(stmt.Values))
		}
		row = table.NewRow(stmt.Values...)
	}

	// Insert row
	if e.curTxn != nil {
		if err := e.curTxn.TableInsert(tbl, row); err != nil {
			return nil, fmt.Errorf("failed to insert row: %w", err)
		}
	} else {
		if err := tbl.Insert(row); err != nil {
			return nil, fmt.Errorf("failed to insert row: %w", err)
		}
		// Update indexes
		if err := e.updateIndexesForInsert(stmt.TableName, schema, row); err != nil {
			return nil, fmt.Errorf("failed to update indexes: %w", err)
		}
		// Auto-commit
		if err := e.store.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync: %w", err)
		}
	}

	return &Result{
		RowsAffected: 1,
		Message:      "1 row inserted",
	}, nil
}

// executeSelect executes a SELECT statement.
func (e *Executor) executeSelect(stmt *SelectStmt) (*Result, error) {
	// Load table
	tbl, err := table.LoadTable(stmt.TableName, e.store)
	if err != nil {
		return nil, fmt.Errorf("table not found: %w", err)
	}

	schema := tbl.Schema()

	// Determine columns to return
	var columns []string
	var colIndices []int

	if len(stmt.Columns) == 0 || (len(stmt.Columns) == 1 && stmt.Columns[0] == "*") {
		// All columns
		for _, col := range schema.Columns {
			columns = append(columns, col.Name)
			colIndices = append(colIndices, len(colIndices))
		}
	} else {
		// Specific columns
		for _, colName := range stmt.Columns {
			idx, err := schema.GetColumnIndex(colName)
			if err != nil {
				return nil, fmt.Errorf("column not found: %w", err)
			}
			columns = append(columns, colName)
			colIndices = append(colIndices, idx)
		}
	}

	var rows []*table.Row

	// Try to use index if WHERE clause is on indexed column
	if stmt.Where != nil {
		// Check if the WHERE column has an index
		idx, err := index.LoadIndex(stmt.TableName, stmt.Where.Column, e.store)
		if err == nil {
			// Index exists! Use it for fast lookup
			if stmt.Where.Operator == "=" {
				// Exact match - use index lookup
				pks, err := idx.Lookup(stmt.Where.Value)
				if err != nil {
					return nil, fmt.Errorf("index lookup failed: %w", err)
				}

				// Fetch rows by primary key
				for _, pk := range pks {
					row, err := tbl.Get(pk)
					if err != nil {
						// Row might have been deleted, skip
						continue
					}

					// Project columns
					projectedRow := &table.Row{
						Values: make([]interface{}, len(colIndices)),
					}
					for i, idx := range colIndices {
						projectedRow.Values[i] = row.Values[idx]
					}
					rows = append(rows, projectedRow)

					// Check LIMIT
					if stmt.Limit > 0 && len(rows) >= stmt.Limit {
						break
					}
				}

				return &Result{
					Rows:    rows,
					Columns: columns,
					Message: fmt.Sprintf("%d rows selected", len(rows)),
				}, nil
			} else if stmt.Where.Operator == ">" || stmt.Where.Operator == ">=" || stmt.Where.Operator == "<" || stmt.Where.Operator == "<=" {
				// Range query - use index range scan
				var startValue, endValue interface{}
				if stmt.Where.Operator == ">" || stmt.Where.Operator == ">=" {
					startValue = stmt.Where.Value
					endValue = nil
				} else {
					startValue = nil
					endValue = stmt.Where.Value
				}

				pks, err := idx.ScanRange(startValue, endValue)
				if err != nil {
					return nil, fmt.Errorf("index range scan failed: %w", err)
				}

				// Fetch rows by primary key
				for _, pk := range pks {
					row, err := tbl.Get(pk)
					if err != nil {
						continue
					}

					// Apply WHERE clause filter (for exact boundary matching)
					if !e.evaluateWhere(row, schema, stmt.Where) {
						continue
					}

					// Project columns
					projectedRow := &table.Row{
						Values: make([]interface{}, len(colIndices)),
					}
					for i, idx := range colIndices {
						projectedRow.Values[i] = row.Values[idx]
					}
					rows = append(rows, projectedRow)

					// Check LIMIT
					if stmt.Limit > 0 && len(rows) >= stmt.Limit {
						break
					}
				}

				return &Result{
					Rows:    rows,
					Columns: columns,
					Message: fmt.Sprintf("%d rows selected", len(rows)),
				}, nil
			}
			// For other operators (!=, etc.), fall through to full scan
		}
	}

	// Fall back to full table scan
	count := 0
	err = tbl.Scan(func(row *table.Row) bool {
		// Apply WHERE clause if present
		if stmt.Where != nil {
			if !e.evaluateWhere(row, schema, stmt.Where) {
				return true // Continue scanning
			}
		}

		// Check LIMIT
		if stmt.Limit > 0 && count >= stmt.Limit {
			return false // Stop scanning
		}

		// Project columns
		projectedRow := &table.Row{
			Values: make([]interface{}, len(colIndices)),
		}
		for i, idx := range colIndices {
			projectedRow.Values[i] = row.Values[idx]
		}

		rows = append(rows, projectedRow)
		count++
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	return &Result{
		Rows:    rows,
		Columns: columns,
		Message: fmt.Sprintf("%d rows selected", len(rows)),
	}, nil
}

// updateIndexesForUpdate updates all indexes after a row update.
func (e *Executor) updateIndexesForUpdate(tableName string, schema *table.Schema, oldRow, newRow *table.Row) error {
	// Get all indexes for this table
	indexNames, err := index.ListIndexes(tableName, e.store)
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
		idx, err := index.LoadIndex(tableName, colName, e.store)
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

// executeUpdate executes an UPDATE statement.
func (e *Executor) executeUpdate(stmt *UpdateStmt) (*Result, error) {
	// Load table
	tbl, err := table.LoadTable(stmt.TableName, e.store)
	if err != nil {
		return nil, fmt.Errorf("table not found: %w", err)
	}

	schema := tbl.Schema()

	// Find rows to update
	var rowsToUpdate []*table.Row
	var oldRows []*table.Row
	var primaryKeys []interface{}

	pkIdx, err := schema.GetColumnIndex(schema.PrimaryKey)
	if err != nil {
		return nil, err
	}

	err = tbl.Scan(func(row *table.Row) bool {
		// Apply WHERE clause if present
		if stmt.Where != nil {
			if !e.evaluateWhere(row, schema, stmt.Where) {
				return true
			}
		}

		oldRows = append(oldRows, row.Clone())
		rowsToUpdate = append(rowsToUpdate, row.Clone())
		primaryKeys = append(primaryKeys, row.Values[pkIdx])
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	// Update rows
	for i, row := range rowsToUpdate {
		// Apply updates
		for colName, newValue := range stmt.Sets {
			if err := row.SetByName(schema, colName, newValue); err != nil {
				return nil, fmt.Errorf("failed to set column %s: %w", colName, err)
			}
		}

		// Update in table
		if e.curTxn != nil {
			if err := e.curTxn.TableUpdate(tbl, primaryKeys[i], row); err != nil {
				return nil, fmt.Errorf("failed to update row: %w", err)
			}
		} else {
			if err := tbl.Update(primaryKeys[i], row); err != nil {
				return nil, fmt.Errorf("failed to update row: %w", err)
			}
			// Update indexes
			if err := e.updateIndexesForUpdate(stmt.TableName, schema, oldRows[i], row); err != nil {
				return nil, fmt.Errorf("failed to update indexes: %w", err)
			}
		}
	}

	// Auto-commit if not in transaction
	if e.curTxn == nil {
		if err := e.store.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync: %w", err)
		}
	}

	return &Result{
		RowsAffected: int64(len(rowsToUpdate)),
		Message:      fmt.Sprintf("%d rows updated", len(rowsToUpdate)),
	}, nil
}

// updateIndexesForDelete updates all indexes after a row delete.
func (e *Executor) updateIndexesForDelete(tableName string, schema *table.Schema, row *table.Row) error {
	// Get all indexes for this table
	indexNames, err := index.ListIndexes(tableName, e.store)
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
		idx, err := index.LoadIndex(tableName, colName, e.store)
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

// executeDelete executes a DELETE statement.
func (e *Executor) executeDelete(stmt *DeleteStmt) (*Result, error) {
	// Load table
	tbl, err := table.LoadTable(stmt.TableName, e.store)
	if err != nil {
		return nil, fmt.Errorf("table not found: %w", err)
	}

	schema := tbl.Schema()

	// Find rows to delete
	var primaryKeys []interface{}
	var rowsToDelete []*table.Row

	pkIdx, err := schema.GetColumnIndex(schema.PrimaryKey)
	if err != nil {
		return nil, err
	}

	err = tbl.Scan(func(row *table.Row) bool {
		// Apply WHERE clause if present
		if stmt.Where != nil {
			if !e.evaluateWhere(row, schema, stmt.Where) {
				return true
			}
		}

		rowsToDelete = append(rowsToDelete, row.Clone())
		primaryKeys = append(primaryKeys, row.Values[pkIdx])
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	// Delete rows
	for i, pk := range primaryKeys {
		if e.curTxn != nil {
			if err := e.curTxn.TableDelete(tbl, pk); err != nil {
				return nil, fmt.Errorf("failed to delete row: %w", err)
			}
		} else {
			if err := tbl.Delete(pk); err != nil {
				return nil, fmt.Errorf("failed to delete row: %w", err)
			}
			// Update indexes
			if err := e.updateIndexesForDelete(stmt.TableName, schema, rowsToDelete[i]); err != nil {
				return nil, fmt.Errorf("failed to update indexes: %w", err)
			}
		}
	}

	// Auto-commit if not in transaction
	if e.curTxn == nil {
		if err := e.store.Sync(); err != nil {
			return nil, fmt.Errorf("failed to sync: %w", err)
		}
	}

	return &Result{
		RowsAffected: int64(len(primaryKeys)),
		Message:      fmt.Sprintf("%d rows deleted", len(primaryKeys)),
	}, nil
}

// executeCreateIndex executes a CREATE INDEX statement.
func (e *Executor) executeCreateIndex(stmt *CreateIndexStmt) (*Result, error) {
	// Load table to get schema
	tbl, err := table.LoadTable(stmt.TableName, e.store)
	if err != nil {
		return nil, fmt.Errorf("table not found: %w", err)
	}

	schema := tbl.Schema()

	// Create index
	idx, err := index.CreateIndex(stmt.TableName, stmt.ColumnName, schema, e.store, stmt.Unique)
	if err != nil {
		return nil, fmt.Errorf("failed to create index: %w", err)
	}

	// Populate index with existing data
	colIdx, err := schema.GetColumnIndex(stmt.ColumnName)
	if err != nil {
		return nil, err
	}

	pkIdx, err := schema.GetColumnIndex(schema.PrimaryKey)
	if err != nil {
		return nil, err
	}

	err = tbl.Scan(func(row *table.Row) bool {
		colValue := row.Values[colIdx]
		pkValue := row.Values[pkIdx]
		if err := idx.Insert(colValue, pkValue); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return nil, fmt.Errorf("failed to populate index: %w", err)
	}

	// Sync to disk
	if err := e.store.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("Index %s created successfully", stmt.IndexName),
	}, nil
}

// executeDropIndex executes a DROP INDEX statement.
func (e *Executor) executeDropIndex(stmt *DropIndexStmt) (*Result, error) {
	// Load index
	idx, err := index.LoadIndex(stmt.TableName, stmt.ColumnName, e.store)
	if err != nil {
		return nil, fmt.Errorf("index not found: %w", err)
	}

	// Drop index
	if err := idx.Drop(); err != nil {
		return nil, fmt.Errorf("failed to drop index: %w", err)
	}

	// Sync to disk
	if err := e.store.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync: %w", err)
	}

	return &Result{
		Message: fmt.Sprintf("Index %s dropped successfully", stmt.IndexName),
	}, nil
}

// executeBegin executes a BEGIN TRANSACTION statement.
func (e *Executor) executeBegin(stmt *BeginStmt) (*Result, error) {
	if e.curTxn != nil {
		return nil, fmt.Errorf("transaction already in progress")
	}

	txn, err := e.txnMgr.Begin(transaction.Serializable)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	e.curTxn = txn

	return &Result{
		Message: "Transaction started",
	}, nil
}

// executeCommit executes a COMMIT statement.
func (e *Executor) executeCommit(stmt *CommitStmt) (*Result, error) {
	if e.curTxn == nil {
		return nil, fmt.Errorf("no transaction in progress")
	}

	if err := e.txnMgr.Commit(e.curTxn); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	e.curTxn = nil

	return &Result{
		Message: "Transaction committed",
	}, nil
}

// executeRollback executes a ROLLBACK statement.
func (e *Executor) executeRollback(stmt *RollbackStmt) (*Result, error) {
	if e.curTxn == nil {
		return nil, fmt.Errorf("no transaction in progress")
	}

	if err := e.txnMgr.Rollback(e.curTxn); err != nil {
		return nil, fmt.Errorf("failed to rollback transaction: %w", err)
	}

	e.curTxn = nil

	return &Result{
		Message: "Transaction rolled back",
	}, nil
}

// evaluateWhere evaluates a WHERE clause against a row.
func (e *Executor) evaluateWhere(row *table.Row, schema *table.Schema, where *WhereClause) bool {
	colIdx, err := schema.GetColumnIndex(where.Column)
	if err != nil {
		return false
	}

	rowValue := row.Values[colIdx]
	whereValue := where.Value

	// Handle NULL comparisons
	if rowValue == nil || whereValue == nil {
		return rowValue == whereValue
	}

	// Compare based on operator
	switch where.Operator {
	case "=":
		return compareValues(rowValue, whereValue) == 0
	case "!=", "<>":
		return compareValues(rowValue, whereValue) != 0
	case ">":
		return compareValues(rowValue, whereValue) > 0
	case "<":
		return compareValues(rowValue, whereValue) < 0
	case ">=":
		return compareValues(rowValue, whereValue) >= 0
	case "<=":
		return compareValues(rowValue, whereValue) <= 0
	default:
		return false
	}
}

// compareValues compares two values and returns -1, 0, or 1.
func compareValues(a, b interface{}) int {
	switch v1 := a.(type) {
	case int64:
		v2, ok := b.(int64)
		if !ok {
			return -1
		}
		if v1 < v2 {
			return -1
		} else if v1 > v2 {
			return 1
		}
		return 0
	case string:
		v2, ok := b.(string)
		if !ok {
			return -1
		}
		if v1 < v2 {
			return -1
		} else if v1 > v2 {
			return 1
		}
		return 0
	case float64:
		v2, ok := b.(float64)
		if !ok {
			return -1
		}
		if v1 < v2 {
			return -1
		} else if v1 > v2 {
			return 1
		}
		return 0
	case bool:
		v2, ok := b.(bool)
		if !ok {
			return -1
		}
		if v1 == v2 {
			return 0
		}
		if !v1 && v2 {
			return -1
		}
		return 1
	default:
		return 0
	}
}
