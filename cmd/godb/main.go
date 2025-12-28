package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/khoale/godb-3000/internal/kv"
	"github.com/khoale/godb-3000/internal/sql"
	"github.com/khoale/godb-3000/internal/table"
)

const banner = `
 ██████╗  ██████╗ ██████╗ ██████╗       ██████╗  ██████╗  ██████╗  ██████╗
██╔════╝ ██╔═══██╗██╔══██╗██╔══██╗      ╚════██╗██╔═████╗██╔═████╗██╔═████╗
██║  ███╗██║   ██║██║  ██║██████╔╝█████╗ █████╔╝██║██╔██║██║██╔██║██║██╔██║
██║   ██║██║   ██║██║  ██║██╔══██╗╚════╝ ╚═══██╗████╔╝██║████╔╝██║████╔╝██║
╚██████╔╝╚██████╔╝██████╔╝██████╔╝      ██████╔╝╚██████╔╝╚██████╔╝╚██████╔╝
 ╚═════╝  ╚═════╝ ╚═════╝ ╚═════╝       ╚═════╝  ╚═════╝  ╚═════╝  ╚═════╝

Educational Database System - v1.0
Type 'help' for commands, 'quit' to exit
`

func main() {
	fmt.Println(banner)

	// Check command line arguments
	dbPath := "godb.db"
	if len(os.Args) > 1 {
		dbPath = os.Args[1]
	}

	fmt.Printf("Opening database: %s\n", dbPath)

	// Open database
	store, err := kv.Open(dbPath, kv.DefaultOptions())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	// Create executor
	executor := sql.NewExecutor(store)

	// Interactive REPL
	reader := bufio.NewReader(os.Stdin)
	var multilineBuffer strings.Builder

	for {
		// Determine prompt
		prompt := "godb> "
		if multilineBuffer.Len() > 0 {
			prompt = "   -> "
		}

		fmt.Print(prompt)
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)

		// Handle empty lines
		if line == "" {
			continue
		}

		// Check for special commands
		if multilineBuffer.Len() == 0 {
			switch strings.ToLower(line) {
			case "quit", "exit", "\\q":
				fmt.Println("Goodbye!")
				return
			case "help", "\\h", "\\?":
				printHelp()
				continue
			case "\\l", "\\tables":
				listTables(store)
				continue
			case "\\d":
				fmt.Println("Usage: \\d <table_name>")
				continue
			}

			// Check if it's a describe command
			if strings.HasPrefix(strings.ToLower(line), "\\d ") {
				tableName := strings.TrimSpace(line[3:])
				describeTable(store, tableName)
				continue
			}

			if strings.ToLower(line) == "\\stats" {
				showStats(store)
				continue
			}
		}

		// Build multiline statement
		multilineBuffer.WriteString(line)
		multilineBuffer.WriteString(" ")

		// Check if statement is complete (ends with semicolon)
		if !strings.HasSuffix(line, ";") {
			continue
		}

		// Execute complete statement
		statement := strings.TrimSpace(multilineBuffer.String())
		statement = strings.TrimSuffix(statement, ";")
		multilineBuffer.Reset()

		if statement == "" {
			continue
		}

		// Parse and execute
		parser := sql.NewParser(statement)
		stmt, err := parser.Parse()
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			continue
		}

		result, err := executor.Execute(stmt)
		if err != nil {
			fmt.Printf("Execution error: %v\n", err)
			continue
		}

		// Display results
		displayResult(result)
	}
}

func printHelp() {
	help := `
Available Commands:

  SQL Statements:
    CREATE TABLE <name> (<columns>)  - Create a new table
    DROP TABLE <name>                - Drop a table
    INSERT INTO <table> VALUES (...)  - Insert a row
    SELECT <cols> FROM <table> [WHERE ...] - Query data
    UPDATE <table> SET ... [WHERE ...]     - Update rows
    DELETE FROM <table> [WHERE ...]        - Delete rows
    BEGIN                            - Start transaction
    COMMIT                           - Commit transaction
    ROLLBACK                         - Rollback transaction

  Meta Commands:
    \l, \tables      - List all tables
    \d <table>       - Describe table schema
    \stats           - Show database statistics
    \h, help, \?     - Show this help
    \q, quit, exit   - Exit the program

  Examples:
    CREATE TABLE users (id INT PRIMARY KEY, name STRING NOT NULL, age INT);
    INSERT INTO users VALUES (1, 'Alice', 30);
    SELECT * FROM users WHERE age > 25;
    UPDATE users SET age = 31 WHERE id = 1;
    DELETE FROM users WHERE id = 1;

  Note: All SQL statements must end with a semicolon (;)
`
	fmt.Println(help)
}

func listTables(store *kv.KV) {
	tables, err := table.ListTables(store)
	if err != nil {
		fmt.Printf("Error listing tables: %v\n", err)
		return
	}

	if len(tables) == 0 {
		fmt.Println("No tables found.")
		return
	}

	fmt.Println("\nTables:")
	for _, tableName := range tables {
		tbl, err := table.LoadTable(tableName, store)
		if err != nil {
			fmt.Printf("  %s (error loading)\n", tableName)
			continue
		}
		count, _ := tbl.Count()
		fmt.Printf("  %s (%d rows)\n", tableName, count)
	}
	fmt.Println()
}

func describeTable(store *kv.KV, tableName string) {
	tbl, err := table.LoadTable(tableName, store)
	if err != nil {
		fmt.Printf("Table '%s' not found: %v\n", tableName, err)
		return
	}

	schema := tbl.Schema()
	fmt.Printf("\nTable: %s\n", schema.Name)
	fmt.Println("Columns:")
	fmt.Println("  Name                Type       Constraints")
	fmt.Println("  ----------------    --------   ------------------")

	for _, col := range schema.Columns {
		constraints := []string{}
		if col.PrimaryKey {
			constraints = append(constraints, "PRIMARY KEY")
		}
		if col.NotNull {
			constraints = append(constraints, "NOT NULL")
		}
		if col.Unique {
			constraints = append(constraints, "UNIQUE")
		}
		constraintStr := strings.Join(constraints, ", ")
		if constraintStr == "" {
			constraintStr = "-"
		}

		fmt.Printf("  %-20s %-10s %s\n", col.Name, col.Type, constraintStr)
	}
	fmt.Println()
}

func showStats(store *kv.KV) {
	stats, err := store.Stats()
	if err != nil {
		fmt.Printf("Error getting stats: %v\n", err)
		return
	}

	fmt.Println("\nDatabase Statistics:")
	fmt.Printf("  Total Pages:     %d\n", stats.PagerStats.NumPages)
	fmt.Printf("  Dirty Pages:     %d\n", stats.PagerStats.DirtyPages)
	fmt.Printf("  Cached Pages:    %d\n", stats.PagerStats.CacheSize)
	fmt.Printf("  Free Pages:      %d\n", stats.FreePages)
	fmt.Printf("  File Size:       %d bytes (%.2f MB)\n",
		stats.PagerStats.FileSize,
		float64(stats.PagerStats.FileSize)/(1024*1024))

	if stats.BTreeStats != nil {
		fmt.Printf("  B+Tree Depth:    %d\n", stats.BTreeStats.Depth)
		fmt.Printf("  Total Keys:      %d\n", stats.BTreeStats.NumKeys)
		fmt.Printf("  Leaf Nodes:      %d\n", stats.BTreeStats.NumLeaves)
		fmt.Printf("  Internal Nodes:  %d\n", stats.BTreeStats.NumNodes-stats.BTreeStats.NumLeaves)
		if stats.BTreeStats.NumKeys > 0 {
			fmt.Printf("  Avg Key Size:    %.1f bytes\n", stats.BTreeStats.AvgKeySize)
			fmt.Printf("  Avg Value Size:  %.1f bytes\n", stats.BTreeStats.AvgValueSize)
		}
	}

	fmt.Printf("  In Transaction:  %v\n", stats.InTransaction)
	fmt.Println()
}

func displayResult(result *sql.Result) {
	if result.Message != "" {
		fmt.Println(result.Message)
		return
	}

	if len(result.Rows) == 0 {
		if result.RowsAffected > 0 {
			fmt.Printf("%d row(s) affected\n", result.RowsAffected)
		} else {
			fmt.Println("No rows returned")
		}
		return
	}

	// Display as table
	if len(result.Columns) == 0 {
		fmt.Printf("%d row(s) returned\n", len(result.Rows))
		return
	}

	// Calculate column widths
	widths := make([]int, len(result.Columns))
	for i, col := range result.Columns {
		widths[i] = len(col)
	}

	for _, row := range result.Rows {
		for i, val := range row.Values {
			if i < len(widths) {
				valStr := formatValue(val)
				if len(valStr) > widths[i] {
					widths[i] = len(valStr)
				}
			}
		}
	}

	// Print header
	fmt.Println()
	for i, col := range result.Columns {
		fmt.Printf("%-*s", widths[i]+2, col)
	}
	fmt.Println()

	// Print separator
	for i := range result.Columns {
		fmt.Print(strings.Repeat("-", widths[i]+2))
	}
	fmt.Println()

	// Print rows
	for _, row := range result.Rows {
		for i, val := range row.Values {
			if i < len(widths) {
				fmt.Printf("%-*s", widths[i]+2, formatValue(val))
			}
		}
		fmt.Println()
	}

	fmt.Printf("\n(%d row%s)\n", len(result.Rows), pluralize(len(result.Rows)))
}

func formatValue(val interface{}) string {
	if val == nil {
		return "NULL"
	}
	return fmt.Sprintf("%v", val)
}

func pluralize(count int) string {
	if count == 1 {
		return ""
	}
	return "s"
}
