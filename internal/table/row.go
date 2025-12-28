package table

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Row represents a single row in a table.
// It stores values in the same order as the schema columns.
type Row struct {
	Values []interface{} // Values corresponding to schema columns
}

// NewRow creates a new row with the given values.
func NewRow(values ...interface{}) *Row {
	return &Row{
		Values: values,
	}
}

// Get returns the value at the given column index.
func (r *Row) Get(index int) (interface{}, error) {
	if index < 0 || index >= len(r.Values) {
		return nil, fmt.Errorf("column index %d out of bounds", index)
	}
	return r.Values[index], nil
}

// Set sets the value at the given column index.
func (r *Row) Set(index int, value interface{}) error {
	if index < 0 || index >= len(r.Values) {
		return fmt.Errorf("column index %d out of bounds", index)
	}
	r.Values[index] = value
	return nil
}

// GetByName returns the value for the named column (requires schema).
func (r *Row) GetByName(schema *Schema, name string) (interface{}, error) {
	idx, err := schema.GetColumnIndex(name)
	if err != nil {
		return nil, err
	}
	return r.Get(idx)
}

// SetByName sets the value for the named column (requires schema).
func (r *Row) SetByName(schema *Schema, name string, value interface{}) error {
	idx, err := schema.GetColumnIndex(name)
	if err != nil {
		return err
	}
	return r.Set(idx, value)
}

// Clone creates a deep copy of the row.
func (r *Row) Clone() *Row {
	clone := &Row{
		Values: make([]interface{}, len(r.Values)),
	}
	for i, v := range r.Values {
		switch val := v.(type) {
		case []byte:
			clone.Values[i] = append([]byte{}, val...)
		default:
			clone.Values[i] = val
		}
	}
	return clone
}

// Marshal serializes a row to bytes according to the schema.
// Format:
// | NullBitmap (ceil(numCols/8) bytes) | [Values] |
//
// Each value is encoded according to its type:
// - INT: 8 bytes (little-endian int64)
// - STRING: 2 bytes length + data
// - BYTES: 2 bytes length + data
// - BOOL: 1 byte (0 or 1)
// - FLOAT: 8 bytes (little-endian float64)
//
// The null bitmap marks which values are NULL (bit=1 means NULL).
func (r *Row) Marshal(schema *Schema) ([]byte, error) {
	if len(r.Values) != schema.NumColumns() {
		return nil, fmt.Errorf("row has %d values, schema has %d columns", len(r.Values), schema.NumColumns())
	}

	// Calculate null bitmap size
	bitmapSize := (schema.NumColumns() + 7) / 8 // ceiling division ceil(a / b) = (a + b - 1) / b

	// Estimate size (will be exact after we know variable-length sizes)
	estimatedSize := bitmapSize
	for i, col := range schema.Columns {
		value := r.Values[i]
		if value == nil {
			continue
		}

		switch col.Type {
		case TypeInt, TypeFloat:
			estimatedSize += 8
		case TypeBool:
			estimatedSize += 1
		case TypeString:
			s := value.(string)
			estimatedSize += 2 + len(s)
		case TypeBytes:
			b := value.([]byte)
			estimatedSize += 2 + len(b)
		}
	}

	data := make([]byte, estimatedSize)
	offset := 0

	// Write null bitmap
	nullBitmap := make([]byte, bitmapSize)
	for i, value := range r.Values {
		if value == nil {
			byteIdx := i / 8
			bitIdx := uint(i % 8)
			nullBitmap[byteIdx] |= (1 << bitIdx)
		}
	}
	copy(data[offset:], nullBitmap)
	offset += bitmapSize

	// Write values
	for i, col := range schema.Columns {
		value := r.Values[i]
		if value == nil {
			continue // Skip NULL values
		}

		switch col.Type {
		case TypeInt:
			v := value.(int64)
			binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(v))
			offset += 8

		case TypeString:
			s := value.(string)
			binary.LittleEndian.PutUint16(data[offset:offset+2], uint16(len(s)))
			offset += 2
			copy(data[offset:], s)
			offset += len(s)

		case TypeBytes:
			b := value.([]byte)
			binary.LittleEndian.PutUint16(data[offset:offset+2], uint16(len(b)))
			offset += 2
			copy(data[offset:], b)
			offset += len(b)

		case TypeBool:
			if value.(bool) {
				data[offset] = 1
			} else {
				data[offset] = 0
			}
			offset++

		case TypeFloat:
			v := value.(float64)
			binary.LittleEndian.PutUint64(data[offset:offset+8], math.Float64bits(v))
			offset += 8

		default:
			return nil, fmt.Errorf("unsupported type: %d", col.Type)
		}
	}

	return data[:offset], nil
}

// UnmarshalRow deserializes a row from bytes according to the schema.
func UnmarshalRow(data []byte, schema *Schema) (*Row, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty row data")
	}

	numCols := schema.NumColumns()
	bitmapSize := (numCols + 7) / 8 // ceiling division ceil(a / b) = (a + b - 1) / b

	if len(data) < bitmapSize {
		return nil, fmt.Errorf("row data too short for null bitmap")
	}

	// Read null bitmap
	nullBitmap := data[0:bitmapSize]
	offset := bitmapSize

	row := &Row{
		Values: make([]interface{}, numCols),
	}

	// Read values
	for i, col := range schema.Columns {
		// Check if value is NULL
		byteIdx := i / 8
		bitIdx := uint(i % 8)
		isNull := (nullBitmap[byteIdx] & (1 << bitIdx)) != 0

		if isNull {
			row.Values[i] = nil
			continue
		}

		// Read value based on type
		switch col.Type {
		case TypeInt:
			if offset+8 > len(data) {
				return nil, fmt.Errorf("row data truncated: expected INT at column %d", i)
			}
			row.Values[i] = int64(binary.LittleEndian.Uint64(data[offset : offset+8]))
			offset += 8

		case TypeString:
			if offset+2 > len(data) {
				return nil, fmt.Errorf("row data truncated: expected STRING length at column %d", i)
			}
			length := binary.LittleEndian.Uint16(data[offset : offset+2])
			offset += 2
			if offset+int(length) > len(data) {
				return nil, fmt.Errorf("row data truncated: expected STRING data at column %d", i)
			}
			row.Values[i] = string(data[offset : offset+int(length)])
			offset += int(length)

		case TypeBytes:
			if offset+2 > len(data) {
				return nil, fmt.Errorf("row data truncated: expected BYTES length at column %d", i)
			}
			length := binary.LittleEndian.Uint16(data[offset : offset+2])
			offset += 2
			if offset+int(length) > len(data) {
				return nil, fmt.Errorf("row data truncated: expected BYTES data at column %d", i)
			}
			row.Values[i] = append([]byte{}, data[offset:offset+int(length)]...)
			offset += int(length)

		case TypeBool:
			if offset+1 > len(data) {
				return nil, fmt.Errorf("row data truncated: expected BOOL at column %d", i)
			}
			row.Values[i] = data[offset] != 0
			offset++

		case TypeFloat:
			if offset+8 > len(data) {
				return nil, fmt.Errorf("row data truncated: expected FLOAT at column %d", i)
			}
			bits := binary.LittleEndian.Uint64(data[offset : offset+8])
			row.Values[i] = math.Float64frombits(bits)
			offset += 8

		default:
			return nil, fmt.Errorf("unsupported type: %d", col.Type)
		}
	}

	return row, nil
}

// String returns a string representation of the row.
func (r *Row) String() string {
	return fmt.Sprintf("Row%v", r.Values)
}

// Equals checks if two rows are equal.
func (r *Row) Equals(other *Row) bool {
	if len(r.Values) != len(other.Values) {
		return false
	}

	for i := range r.Values {
		v1 := r.Values[i]
		v2 := other.Values[i]

		// Handle NULL values
		if v1 == nil && v2 == nil {
			continue
		}
		if v1 == nil || v2 == nil {
			return false
		}

		// Compare based on type
		switch val1 := v1.(type) {
		case int64:
			val2, ok := v2.(int64)
			if !ok || val1 != val2 {
				return false
			}
		case string:
			val2, ok := v2.(string)
			if !ok || val1 != val2 {
				return false
			}
		case []byte:
			val2, ok := v2.([]byte)
			if !ok {
				return false
			}
			if len(val1) != len(val2) {
				return false
			}
			for j := range val1 {
				if val1[j] != val2[j] {
					return false
				}
			}
		case bool:
			val2, ok := v2.(bool)
			if !ok || val1 != val2 {
				return false
			}
		case float64:
			val2, ok := v2.(float64)
			if !ok || val1 != val2 {
				return false
			}
		default:
			return false
		}
	}

	return true
}
