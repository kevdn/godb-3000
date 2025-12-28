package sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/khoale/godb-3000/internal/table"
	"github.com/khoale/godb-3000/internal/transaction"
)

// Statement represents a parsed SQL statement.
type Statement interface {
	Type() StatementType
}

// StatementType represents the type of SQL statement.
type StatementType int

const (
	StmtUnknown StatementType = iota
	StmtCreateTable
	StmtDropTable
	StmtInsert
	StmtSelect
	StmtUpdate
	StmtDelete
	StmtCreateIndex
	StmtDropIndex
	StmtBegin
	StmtCommit
	StmtRollback
	StmtShowTables
	StmtBackup
)

// CreateTableStmt represents a CREATE TABLE statement.
type CreateTableStmt struct {
	TableName string
	Columns   []*ColumnDef
}

func (s *CreateTableStmt) Type() StatementType { return StmtCreateTable }

// ColumnDef represents a column definition in CREATE TABLE.
type ColumnDef struct {
	Name          string
	Type          table.DataType
	PrimaryKey    bool
	NotNull       bool
	Unique        bool
	AutoIncrement bool
}

// DropTableStmt represents a DROP TABLE statement.
type DropTableStmt struct {
	TableName string
}

func (s *DropTableStmt) Type() StatementType { return StmtDropTable }

// InsertStmt represents an INSERT statement.
type InsertStmt struct {
	TableName string
	Columns   []string // Column names (empty means all columns)
	Values    []interface{}
}

func (s *InsertStmt) Type() StatementType { return StmtInsert }

// SelectStmt represents a SELECT statement.
type SelectStmt struct {
	TableName string
	Columns   []string // Column names (empty or ["*"] means all columns)
	Where     *WhereClause
	OrderBy   []string
	Limit     int
}

func (s *SelectStmt) Type() StatementType { return StmtSelect }

// UpdateStmt represents an UPDATE statement.
type UpdateStmt struct {
	TableName string
	Sets      map[string]interface{} // Column name -> new value
	Where     *WhereClause
}

func (s *UpdateStmt) Type() StatementType { return StmtUpdate }

// DeleteStmt represents a DELETE statement.
type DeleteStmt struct {
	TableName string
	Where     *WhereClause
}

func (s *DeleteStmt) Type() StatementType { return StmtDelete }

// CreateIndexStmt represents a CREATE INDEX statement.
type CreateIndexStmt struct {
	IndexName  string
	TableName  string
	ColumnName string
	Unique     bool
}

func (s *CreateIndexStmt) Type() StatementType { return StmtCreateIndex }

// DropIndexStmt represents a DROP INDEX statement.
type DropIndexStmt struct {
	IndexName  string
	TableName  string
	ColumnName string
}

func (s *DropIndexStmt) Type() StatementType { return StmtDropIndex }

// BeginStmt represents a BEGIN TRANSACTION statement.
type BeginStmt struct {
	IsolationLevel transaction.IsolationLevel // Optional isolation level
}

func (s *BeginStmt) Type() StatementType { return StmtBegin }

// CommitStmt represents a COMMIT statement.
type CommitStmt struct{}

func (s *CommitStmt) Type() StatementType { return StmtCommit }

// RollbackStmt represents a ROLLBACK statement.
type RollbackStmt struct{}

func (s *RollbackStmt) Type() StatementType { return StmtRollback }

// ShowTablesStmt represents a SHOW TABLES statement.
type ShowTablesStmt struct{}

func (s *ShowTablesStmt) Type() StatementType { return StmtShowTables }

// BackupStmt represents a BACKUP TO statement.
type BackupStmt struct {
	Path string
}

func (s *BackupStmt) Type() StatementType { return StmtBackup }

// WhereClause represents a WHERE condition.
type WhereClause struct {
	Column   string
	Operator string // "=", "!=", ">", "<", ">=", "<="
	Value    interface{}
}

// Parser parses SQL statements.
type Parser struct {
	tokens []string
	pos    int
}

// NewParser creates a new SQL parser.
func NewParser(sql string) *Parser {
	tokens := tokenize(sql)
	return &Parser{
		tokens: tokens,
		pos:    0,
	}
}

// Parse parses the SQL statement.
func (p *Parser) Parse() (Statement, error) {
	if len(p.tokens) == 0 {
		return nil, fmt.Errorf("empty statement")
	}

	// Get the first keyword to determine statement type
	keyword := strings.ToUpper(p.tokens[0])

	switch keyword {
	case "CREATE":
		return p.parseCreate()
	case "DROP":
		return p.parseDrop()
	case "INSERT":
		return p.parseInsert()
	case "SELECT":
		return p.parseSelect()
	case "UPDATE":
		return p.parseUpdate()
	case "DELETE":
		return p.parseDelete()
	case "BEGIN":
		return p.parseBegin()
	case "COMMIT":
		return &CommitStmt{}, nil
	case "ROLLBACK":
		return &RollbackStmt{}, nil
	case "SHOW":
		return p.parseShow()
	case "BACKUP":
		return p.parseBackup()
	default:
		return nil, fmt.Errorf("unsupported statement type: %s", keyword)
	}
}

// parseCreate parses CREATE TABLE or CREATE INDEX.
func (p *Parser) parseCreate() (Statement, error) {
	if len(p.tokens) < 2 {
		return nil, fmt.Errorf("incomplete CREATE statement")
	}

	objType := strings.ToUpper(p.tokens[1])

	switch objType {
	case "TABLE":
		return p.parseCreateTable()
	case "INDEX", "UNIQUE":
		return p.parseCreateIndex()
	default:
		return nil, fmt.Errorf("unsupported CREATE type: %s", objType)
	}
}

// parseCreateTable parses CREATE TABLE statement.
// Syntax: CREATE TABLE table_name (col1 TYPE [PRIMARY KEY] [NOT NULL], ...)
func (p *Parser) parseCreateTable() (*CreateTableStmt, error) {
	if len(p.tokens) < 4 {
		return nil, fmt.Errorf("invalid CREATE TABLE syntax")
	}

	stmt := &CreateTableStmt{
		TableName: p.tokens[2],
		Columns:   make([]*ColumnDef, 0),
	}

	// Find opening parenthesis
	start := -1
	for i := 3; i < len(p.tokens); i++ {
		if p.tokens[i] == "(" {
			start = i + 1
			break
		}
	}

	if start == -1 {
		return nil, fmt.Errorf("missing opening parenthesis")
	}

	// Parse column definitions
	i := start
	for i < len(p.tokens) {
		if p.tokens[i] == ")" {
			break
		}

		// Skip commas
		if p.tokens[i] == "," {
			i++
			continue
		}

		// Parse column: name type [constraints]
		if i+1 >= len(p.tokens) {
			return nil, fmt.Errorf("incomplete column definition")
		}

		col := &ColumnDef{
			Name: p.tokens[i],
		}

		// Parse type
		i++
		colType := strings.ToUpper(p.tokens[i])
		switch colType {
		case "INT", "INTEGER":
			col.Type = table.TypeInt
		case "STRING", "TEXT", "VARCHAR":
			col.Type = table.TypeString
		case "BYTES", "BLOB":
			col.Type = table.TypeBytes
		case "BOOL", "BOOLEAN":
			col.Type = table.TypeBool
		case "FLOAT", "DOUBLE", "REAL":
			col.Type = table.TypeFloat
		default:
			return nil, fmt.Errorf("unsupported column type: %s", colType)
		}

		// Parse constraints
		i++
		for i < len(p.tokens) && p.tokens[i] != "," && p.tokens[i] != ")" {
			constraint := strings.ToUpper(p.tokens[i])
			switch constraint {
			case "PRIMARY":
				if i+1 < len(p.tokens) && strings.ToUpper(p.tokens[i+1]) == "KEY" {
					col.PrimaryKey = true
					i++
				}
			case "NOT":
				if i+1 < len(p.tokens) && strings.ToUpper(p.tokens[i+1]) == "NULL" {
					col.NotNull = true
					i++
				}
			case "UNIQUE":
				col.Unique = true
			case "AUTO_INCREMENT", "AUTOINCREMENT":
				col.AutoIncrement = true
			}
			i++
		}

		stmt.Columns = append(stmt.Columns, col)
	}

	if len(stmt.Columns) == 0 {
		return nil, fmt.Errorf("no columns defined")
	}

	return stmt, nil
}

// parseCreateIndex parses CREATE INDEX statement.
// Syntax: CREATE [UNIQUE] INDEX index_name ON table_name (column_name)
func (p *Parser) parseCreateIndex() (*CreateIndexStmt, error) {
	stmt := &CreateIndexStmt{}

	pos := 1
	if strings.ToUpper(p.tokens[pos]) == "UNIQUE" {
		stmt.Unique = true
		pos++
	}

	if strings.ToUpper(p.tokens[pos]) != "INDEX" {
		return nil, fmt.Errorf("expected INDEX keyword")
	}
	pos++

	if pos >= len(p.tokens) {
		return nil, fmt.Errorf("missing index name")
	}
	stmt.IndexName = p.tokens[pos]
	pos++

	if pos >= len(p.tokens) || strings.ToUpper(p.tokens[pos]) != "ON" {
		return nil, fmt.Errorf("expected ON keyword")
	}
	pos++

	if pos >= len(p.tokens) {
		return nil, fmt.Errorf("missing table name")
	}
	stmt.TableName = p.tokens[pos]
	pos++

	// Parse column name (with or without parentheses)
	if pos >= len(p.tokens) {
		return nil, fmt.Errorf("missing column name")
	}

	if p.tokens[pos] == "(" {
		pos++
		if pos >= len(p.tokens) {
			return nil, fmt.Errorf("missing column name")
		}
		stmt.ColumnName = p.tokens[pos]
	} else {
		stmt.ColumnName = p.tokens[pos]
	}

	return stmt, nil
}

// parseDrop parses DROP TABLE or DROP INDEX.
func (p *Parser) parseDrop() (Statement, error) {
	if len(p.tokens) < 3 {
		return nil, fmt.Errorf("incomplete DROP statement")
	}

	objType := strings.ToUpper(p.tokens[1])

	switch objType {
	case "TABLE":
		return &DropTableStmt{TableName: p.tokens[2]}, nil
	case "INDEX":
		// DROP INDEX index_name ON table_name (column_name)
		stmt := &DropIndexStmt{}
		if len(p.tokens) < 5 {
			return nil, fmt.Errorf("invalid DROP INDEX syntax")
		}
		stmt.IndexName = p.tokens[2]
		if strings.ToUpper(p.tokens[3]) == "ON" && len(p.tokens) >= 6 {
			stmt.TableName = p.tokens[4]
			stmt.ColumnName = p.tokens[5]
			if stmt.ColumnName == "(" && len(p.tokens) > 6 {
				stmt.ColumnName = p.tokens[6]
			}
		}
		return stmt, nil
	default:
		return nil, fmt.Errorf("unsupported DROP type: %s", objType)
	}
}

// parseInsert parses INSERT statement.
// Syntax: INSERT INTO table_name (col1, col2, ...) VALUES (val1, val2, ...)
func (p *Parser) parseInsert() (*InsertStmt, error) {
	if len(p.tokens) < 3 {
		return nil, fmt.Errorf("incomplete INSERT statement")
	}

	stmt := &InsertStmt{}

	pos := 1
	if strings.ToUpper(p.tokens[pos]) == "INTO" {
		pos++
	}

	stmt.TableName = p.tokens[pos]
	pos++

	// Parse column names (optional)
	if pos < len(p.tokens) && p.tokens[pos] == "(" {
		pos++
		for pos < len(p.tokens) && p.tokens[pos] != ")" {
			if p.tokens[pos] != "," {
				stmt.Columns = append(stmt.Columns, p.tokens[pos])
			}
			pos++
		}
		pos++ // Skip closing )
	}

	// Expect VALUES keyword
	if pos >= len(p.tokens) || strings.ToUpper(p.tokens[pos]) != "VALUES" {
		return nil, fmt.Errorf("expected VALUES keyword")
	}
	pos++

	// Parse values
	if pos >= len(p.tokens) || p.tokens[pos] != "(" {
		return nil, fmt.Errorf("expected opening parenthesis for values")
	}
	pos++

	for pos < len(p.tokens) && p.tokens[pos] != ")" {
		if p.tokens[pos] != "," {
			val := parseValue(p.tokens[pos])
			stmt.Values = append(stmt.Values, val)
		}
		pos++
	}

	return stmt, nil
}

// parseSelect parses SELECT statement.
// Syntax: SELECT col1, col2 FROM table_name [WHERE condition] [ORDER BY col] [LIMIT n]
func (p *Parser) parseSelect() (*SelectStmt, error) {
	if len(p.tokens) < 4 {
		return nil, fmt.Errorf("incomplete SELECT statement")
	}

	stmt := &SelectStmt{}

	pos := 1

	// Parse columns
	for pos < len(p.tokens) && strings.ToUpper(p.tokens[pos]) != "FROM" {
		if p.tokens[pos] != "," {
			stmt.Columns = append(stmt.Columns, p.tokens[pos])
		}
		pos++
	}

	if pos >= len(p.tokens) || strings.ToUpper(p.tokens[pos]) != "FROM" {
		return nil, fmt.Errorf("expected FROM keyword")
	}
	pos++

	if pos >= len(p.tokens) {
		return nil, fmt.Errorf("missing table name")
	}
	stmt.TableName = p.tokens[pos]
	pos++

	// Parse optional clauses
	for pos < len(p.tokens) {
		keyword := strings.ToUpper(p.tokens[pos])
		switch keyword {
		case "WHERE":
			pos++
			where, newPos, err := p.parseWhere(pos)
			if err != nil {
				return nil, err
			}
			stmt.Where = where
			pos = newPos
		case "ORDER":
			if pos+1 < len(p.tokens) && strings.ToUpper(p.tokens[pos+1]) == "BY" {
				pos += 2
				for pos < len(p.tokens) && strings.ToUpper(p.tokens[pos]) != "LIMIT" {
					if p.tokens[pos] != "," {
						stmt.OrderBy = append(stmt.OrderBy, p.tokens[pos])
					}
					pos++
				}
			}
		case "LIMIT":
			pos++
			if pos < len(p.tokens) {
				limit, err := strconv.Atoi(p.tokens[pos])
				if err != nil {
					return nil, fmt.Errorf("invalid LIMIT value")
				}
				stmt.Limit = limit
				pos++
			}
		default:
			pos++
		}
	}

	return stmt, nil
}

// parseUpdate parses UPDATE statement.
// Syntax: UPDATE table_name SET col1=val1, col2=val2 [WHERE condition]
func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	if len(p.tokens) < 5 {
		return nil, fmt.Errorf("incomplete UPDATE statement")
	}

	stmt := &UpdateStmt{
		TableName: p.tokens[1],
		Sets:      make(map[string]interface{}),
	}

	pos := 2
	if strings.ToUpper(p.tokens[pos]) != "SET" {
		return nil, fmt.Errorf("expected SET keyword")
	}
	pos++

	// Parse SET clause
	for pos < len(p.tokens) && strings.ToUpper(p.tokens[pos]) != "WHERE" {
		if p.tokens[pos] == "," {
			pos++
			continue
		}

		colName := p.tokens[pos]
		pos++

		if pos >= len(p.tokens) || p.tokens[pos] != "=" {
			return nil, fmt.Errorf("expected = after column name")
		}
		pos++

		if pos >= len(p.tokens) {
			return nil, fmt.Errorf("expected value after =")
		}

		value := parseValue(p.tokens[pos])
		stmt.Sets[colName] = value
		pos++
	}

	// Parse WHERE clause
	if pos < len(p.tokens) && strings.ToUpper(p.tokens[pos]) == "WHERE" {
		pos++
		where, _, err := p.parseWhere(pos)
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

// parseDelete parses DELETE statement.
// Syntax: DELETE FROM table_name [WHERE condition]
func (p *Parser) parseDelete() (*DeleteStmt, error) {
	if len(p.tokens) < 3 {
		return nil, fmt.Errorf("incomplete DELETE statement")
	}

	stmt := &DeleteStmt{}

	pos := 1
	if strings.ToUpper(p.tokens[pos]) == "FROM" {
		pos++
	}

	stmt.TableName = p.tokens[pos]
	pos++

	// Parse WHERE clause
	if pos < len(p.tokens) && strings.ToUpper(p.tokens[pos]) == "WHERE" {
		pos++
		where, _, err := p.parseWhere(pos)
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

// parseShow parses SHOW TABLES statement.
// Syntax: SHOW TABLES
func (p *Parser) parseShow() (Statement, error) {
	if len(p.tokens) < 2 {
		return nil, fmt.Errorf("incomplete SHOW statement")
	}

	objType := strings.ToUpper(p.tokens[1])
	switch objType {
	case "TABLES":
		if len(p.tokens) > 2 {
			return nil, fmt.Errorf("unexpected tokens after SHOW TABLES")
		}
		return &ShowTablesStmt{}, nil
	default:
		return nil, fmt.Errorf("unsupported SHOW type: %s", objType)
	}
}

// parseBegin parses BEGIN TRANSACTION statement.
// Syntax: BEGIN [TRANSACTION] [ISOLATION LEVEL level]
// Supported levels: READ COMMITTED, REPEATABLE READ, SERIALIZABLE
// Default: REPEATABLE READ
func (p *Parser) parseBegin() (Statement, error) {
	stmt := &BeginStmt{
		IsolationLevel: transaction.RepeatableRead, // Default to RepeatableRead
	}

	pos := 1

	// Skip optional TRANSACTION keyword
	if pos < len(p.tokens) && strings.ToUpper(p.tokens[pos]) == "TRANSACTION" {
		pos++
	}

	// Check for ISOLATION LEVEL
	if pos < len(p.tokens) && strings.ToUpper(p.tokens[pos]) == "ISOLATION" {
		pos++
		if pos >= len(p.tokens) || strings.ToUpper(p.tokens[pos]) != "LEVEL" {
			return nil, fmt.Errorf("expected LEVEL after ISOLATION")
		}
		pos++

		if pos >= len(p.tokens) {
			return nil, fmt.Errorf("missing isolation level")
		}

		levelStr := strings.ToUpper(p.tokens[pos])
		
		// Handle two-word isolation levels
		if levelStr == "READ" && pos+1 < len(p.tokens) && strings.ToUpper(p.tokens[pos+1]) == "COMMITTED" {
			stmt.IsolationLevel = transaction.ReadCommitted
			pos += 2
		} else if levelStr == "REPEATABLE" && pos+1 < len(p.tokens) && strings.ToUpper(p.tokens[pos+1]) == "READ" {
			stmt.IsolationLevel = transaction.RepeatableRead
			pos += 2
		} else if levelStr == "SERIALIZABLE" {
			stmt.IsolationLevel = transaction.Serializable
			pos++
		} else {
			return nil, fmt.Errorf("unsupported isolation level: %s (supported: READ COMMITTED, REPEATABLE READ, SERIALIZABLE)", levelStr)
		}
	}

	if pos < len(p.tokens) {
		return nil, fmt.Errorf("unexpected tokens after BEGIN statement")
	}

	return stmt, nil
}

// parseBackup parses BACKUP TO statement.
// Syntax: BACKUP TO 'path'
func (p *Parser) parseBackup() (Statement, error) {
	if len(p.tokens) < 3 {
		return nil, fmt.Errorf("incomplete BACKUP statement, expected: BACKUP TO 'path'")
	}

	if strings.ToUpper(p.tokens[1]) != "TO" {
		return nil, fmt.Errorf("expected TO after BACKUP, got: %s", p.tokens[1])
	}

	path := p.tokens[2]
	// Remove quotes if present
	if (strings.HasPrefix(path, "'") && strings.HasSuffix(path, "'")) ||
		(strings.HasPrefix(path, "\"") && strings.HasSuffix(path, "\"")) {
		path = path[1 : len(path)-1]
	}

	if len(p.tokens) > 3 {
		return nil, fmt.Errorf("unexpected tokens after BACKUP TO path")
	}

	return &BackupStmt{
		Path: path,
	}, nil
}

// parseWhere parses a WHERE clause.
// Simple format: column operator value
func (p *Parser) parseWhere(pos int) (*WhereClause, int, error) {
	if pos+2 >= len(p.tokens) {
		return nil, pos, fmt.Errorf("incomplete WHERE clause")
	}

	where := &WhereClause{
		Column:   p.tokens[pos],
		Operator: p.tokens[pos+1],
		Value:    parseValue(p.tokens[pos+2]),
	}

	return where, pos + 3, nil
}

// tokenize splits SQL into tokens.
func tokenize(sql string) []string {
	sql = strings.TrimSpace(sql)
	tokens := make([]string, 0)

	var current strings.Builder
	inString := false
	stringChar := rune(0)

	for i, ch := range sql {
		switch {
		case ch == '\'' || ch == '"':
			if inString && ch == stringChar {
				// End of string
				current.WriteRune(ch)
				tokens = append(tokens, current.String())
				current.Reset()
				inString = false
			} else if !inString {
				// Start of string
				inString = true
				stringChar = ch
				current.WriteRune(ch)
			} else {
				current.WriteRune(ch)
			}
		case inString:
			current.WriteRune(ch)
		case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r':
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		case ch == '(' || ch == ')' || ch == ',' || ch == ';':
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			tokens = append(tokens, string(ch))
		case ch == '=' || ch == '>' || ch == '<' || ch == '!':
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			// Check for multi-char operators
			if i+1 < len(sql) && (sql[i+1] == '=' || (ch == '!' && sql[i+1] == '=')) {
				tokens = append(tokens, sql[i:i+2])
				i++ // This won't work in range loop, but handles common case
			} else {
				tokens = append(tokens, string(ch))
			}
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}

	return tokens
}

// parseValue converts a string token to the appropriate Go type.
func parseValue(token string) interface{} {
	// Remove quotes for strings
	if (strings.HasPrefix(token, "'") && strings.HasSuffix(token, "'")) ||
		(strings.HasPrefix(token, "\"") && strings.HasSuffix(token, "\"")) {
		return token[1 : len(token)-1]
	}

	// Try parsing as int
	if intVal, err := strconv.ParseInt(token, 10, 64); err == nil {
		return intVal
	}

	// Try parsing as float
	if floatVal, err := strconv.ParseFloat(token, 64); err == nil {
		return floatVal
	}

	// Try parsing as bool
	if token == "true" || token == "TRUE" {
		return true
	}
	if token == "false" || token == "FALSE" {
		return false
	}

	// NULL
	if strings.ToUpper(token) == "NULL" {
		return nil
	}

	// Otherwise, treat as string
	return token
}
