package table

import (
	"encoding/binary"
	"fmt"
)

// DataType represents the type of a column.
type DataType uint8

const (
	TypeInt    DataType = 1 // 64-bit integer
	TypeString DataType = 2 // Variable-length string
	TypeBytes  DataType = 3 // Variable-length byte array
	TypeBool   DataType = 4 // Boolean
	TypeFloat  DataType = 5 // 64-bit float
)

// String returns the string representation of the data type.
func (dt DataType) String() string {
	switch dt {
	case TypeInt:
		return "INT"
	case TypeString:
		return "STRING"
	case TypeBytes:
		return "BYTES"
	case TypeBool:
		return "BOOL"
	case TypeFloat:
		return "FLOAT"
	default:
		return "UNKNOWN"
	}
}

// Column represents a table column definition.
type Column struct {
	Name       string
	Type       DataType
	PrimaryKey bool
	NotNull    bool
	Unique     bool
	Index      bool // Whether this column has an index
}

// Schema represents a table schema.
type Schema struct {
	Name       string
	Columns    []*Column
	PrimaryKey string // Name of primary key column
}

// NewSchema creates a new table schema.
func NewSchema(name string) *Schema {
	return &Schema{
		Name:    name,
		Columns: make([]*Column, 0),
	}
}

// AddColumn adds a column to the schema.
func (s *Schema) AddColumn(col *Column) error {
	// Check for duplicate column names
	for _, existing := range s.Columns {
		if existing.Name == col.Name {
			return fmt.Errorf("column %s already exists", col.Name)
		}
	}

	// If this is a primary key, set it
	if col.PrimaryKey {
		if s.PrimaryKey != "" {
			return fmt.Errorf("table already has primary key: %s", s.PrimaryKey)
		}
		s.PrimaryKey = col.Name
		col.NotNull = true // Primary keys are always NOT NULL
		col.Unique = true  // Primary keys are always UNIQUE
	}

	s.Columns = append(s.Columns, col)
	return nil
}

// GetColumn returns a column by name.
func (s *Schema) GetColumn(name string) (*Column, error) {
	for _, col := range s.Columns {
		if col.Name == name {
			return col, nil
		}
	}
	return nil, fmt.Errorf("column %s not found", name)
}

// GetColumnIndex returns the index of a column by name.
func (s *Schema) GetColumnIndex(name string) (int, error) {
	for i, col := range s.Columns {
		if col.Name == name {
			return i, nil
		}
	}
	return -1, fmt.Errorf("column %s not found", name)
}

// NumColumns returns the number of columns.
func (s *Schema) NumColumns() int {
	return len(s.Columns)
}

// Validate validates that a row conforms to the schema.
func (s *Schema) Validate(row *Row) error {
	if len(row.Values) != len(s.Columns) {
		return fmt.Errorf("row has %d values, expected %d", len(row.Values), len(s.Columns))
	}

	for i, col := range s.Columns {
		value := row.Values[i]

		// Check NOT NULL constraint
		if col.NotNull && value == nil {
			return fmt.Errorf("column %s cannot be NULL", col.Name)
		}

		// Check type
		if value != nil {
			if err := s.validateType(col.Type, value); err != nil {
				return fmt.Errorf("column %s: %w", col.Name, err)
			}
		}
	}

	return nil
}

// validateType checks if a value matches the expected type.
func (s *Schema) validateType(expected DataType, value interface{}) error {
	switch expected {
	case TypeInt:
		if _, ok := value.(int64); !ok {
			return fmt.Errorf("expected INT, got %T", value)
		}
	case TypeString:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected STRING, got %T", value)
		}
	case TypeBytes:
		if _, ok := value.([]byte); !ok {
			return fmt.Errorf("expected BYTES, got %T", value)
		}
	case TypeBool:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected BOOL, got %T", value)
		}
	case TypeFloat:
		if _, ok := value.(float64); !ok {
			return fmt.Errorf("expected FLOAT, got %T", value)
		}
	default:
		return fmt.Errorf("unknown type: %d", expected)
	}
	return nil
}

// Marshal serializes the schema to bytes.
// Format:
// | NameLen (2) | Name (var) | NumColumns (2) | [Columns] |
// Column format:
// | NameLen (2) | Name (var) | Type (1) | Flags (1) |
func (s *Schema) Marshal() []byte {
	// Calculate size
	size := 2 + len(s.Name) + 2 // Name + NumColumns
	for _, col := range s.Columns {
		size += 2 + len(col.Name) + 2 // NameLen + Name + Type + Flags
	}

	data := make([]byte, size)
	offset := 0

	// Write table name
	binary.LittleEndian.PutUint16(data[offset:offset+2], uint16(len(s.Name)))
	offset += 2
	copy(data[offset:], s.Name)
	offset += len(s.Name)

	// Write number of columns
	binary.LittleEndian.PutUint16(data[offset:offset+2], uint16(len(s.Columns)))
	offset += 2

	// Write columns
	for _, col := range s.Columns {
		// Column name
		binary.LittleEndian.PutUint16(data[offset:offset+2], uint16(len(col.Name)))
		offset += 2
		copy(data[offset:], col.Name)
		offset += len(col.Name)

		// Column type
		data[offset] = byte(col.Type)
		offset++

		// Column flags
		var flags uint8
		if col.PrimaryKey {
			flags |= 1 << 0
		}
		if col.NotNull {
			flags |= 1 << 1
		}
		if col.Unique {
			flags |= 1 << 2
		}
		if col.Index {
			flags |= 1 << 3
		}
		data[offset] = flags
		offset++
	}

	return data
}

// UnmarshalSchema deserializes a schema from bytes.
func UnmarshalSchema(data []byte) (*Schema, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("schema data too short")
	}

	offset := 0

	// Read table name
	nameLen := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2
	if offset+int(nameLen) > len(data) {
		return nil, fmt.Errorf("invalid schema data: name truncated")
	}
	name := string(data[offset : offset+int(nameLen)])
	offset += int(nameLen)

	schema := NewSchema(name)

	// Read number of columns
	if offset+2 > len(data) {
		return nil, fmt.Errorf("invalid schema data: num columns missing")
	}
	numCols := binary.LittleEndian.Uint16(data[offset : offset+2])
	offset += 2

	// Read columns
	for i := 0; i < int(numCols); i++ {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("invalid schema data: column name length missing")
		}

		// Column name
		colNameLen := binary.LittleEndian.Uint16(data[offset : offset+2])
		offset += 2
		if offset+int(colNameLen) > len(data) {
			return nil, fmt.Errorf("invalid schema data: column name truncated")
		}
		colName := string(data[offset : offset+int(colNameLen)])
		offset += int(colNameLen)

		// Column type
		if offset+2 > len(data) {
			return nil, fmt.Errorf("invalid schema data: column type missing")
		}
		colType := DataType(data[offset])
		offset++

		// Column flags
		flags := data[offset]
		offset++

		col := &Column{
			Name:       colName,
			Type:       colType,
			PrimaryKey: (flags & (1 << 0)) != 0,
			NotNull:    (flags & (1 << 1)) != 0,
			Unique:     (flags & (1 << 2)) != 0,
			Index:      (flags & (1 << 3)) != 0,
		}

		if err := schema.AddColumn(col); err != nil {
			return nil, err
		}
	}

	return schema, nil
}

// Clone creates a deep copy of the schema.
func (s *Schema) Clone() *Schema {
	clone := NewSchema(s.Name)
	clone.PrimaryKey = s.PrimaryKey

	for _, col := range s.Columns {
		clone.Columns = append(clone.Columns, &Column{
			Name:       col.Name,
			Type:       col.Type,
			PrimaryKey: col.PrimaryKey,
			NotNull:    col.NotNull,
			Unique:     col.Unique,
			Index:      col.Index,
		})
	}

	return clone
}
