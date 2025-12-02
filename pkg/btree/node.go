package btree

import (
	"encoding/binary"
	"fmt"

	"github.com/khoale/godb-3000/pkg/storage"
)

// Node represents a B+tree node stored on disk.
//
// Node types:
// - Internal nodes: contain keys and pointers to child nodes
// - Leaf nodes: contain keys and values (or pointers to values)
//
// Layout on page:
// | Type (2) | NumKeys (2) | Payload |
//
// Internal node payload:
// | Child0 (8) | Key0 (var) | Child1 (8) | Key1 (var) | ... | ChildN (8) |
//
// Leaf node payload:
// | Key0 (var) | Val0 (var) | Key1 (var) | Val1 (var) | ... | Next (8) |

const (
	// Node types
	NodeTypeInternal = 1
	NodeTypeLeaf     = 2

	// Node header layout
	NodeHeaderSize = 4
	NodeTypeOffset = 0
	NodeKeysOffset = 2

	// Fanout determines how many children an internal node can have.
	// Higher fanout = shorter tree, but larger nodes.
	// We use a conservative value to ensure nodes fit in one page.
	MaxKeys     = 64 // Maximum keys per node
	MinKeys     = 32 // Minimum keys per node (for rebalancing)
	MaxChildren = 65 // MaxKeys + 1 for internal nodes
)

// Node represents a B+tree node.
type Node struct {
	nodeType uint16           // Internal or Leaf
	keys     [][]byte         // Keys stored in this node
	children []storage.PageID // Child page IDs (internal nodes only)
	values   [][]byte         // Values (leaf nodes only)
	next     storage.PageID   // Next leaf pointer (leaf nodes only)
	page     *storage.Page    // Underlying page
}

// NewInternalNode creates a new internal node.
func NewInternalNode() *Node {
	return &Node{
		nodeType: NodeTypeInternal,
		keys:     make([][]byte, 0, MaxKeys),
		children: make([]storage.PageID, 0, MaxChildren),
		page:     storage.NewPage(storage.PageTypeNode),
	}
}

// NewLeafNode creates a new leaf node.
func NewLeafNode() *Node {
	return &Node{
		nodeType: NodeTypeLeaf,
		keys:     make([][]byte, 0, MaxKeys),
		values:   make([][]byte, 0, MaxKeys),
		next:     storage.InvalidPageID,
		page:     storage.NewPage(storage.PageTypeNode),
	}
}

// IsLeaf returns true if this is a leaf node.
func (n *Node) IsLeaf() bool {
	return n.nodeType == NodeTypeLeaf
}

// IsInternal returns true if this is an internal node.
func (n *Node) IsInternal() bool {
	return n.nodeType == NodeTypeInternal
}

// NumKeys returns the number of keys in the node.
func (n *Node) NumKeys() int {
	return len(n.keys)
}

// IsFull returns true if the node cannot accept more keys.
func (n *Node) IsFull() bool {
	return len(n.keys) >= MaxKeys
}

// IsUnderflow returns true if the node has too few keys (needs rebalancing).
func (n *Node) IsUnderflow() bool {
	return len(n.keys) < MinKeys
}

// GetKey returns the key at the given index.
func (n *Node) GetKey(index int) []byte {
	if index < 0 || index >= len(n.keys) {
		return nil
	}
	return n.keys[index]
}

// GetChild returns the child page ID at the given index (internal nodes only).
func (n *Node) GetChild(index int) storage.PageID {
	if n.IsLeaf() || index < 0 || index >= len(n.children) {
		return storage.InvalidPageID
	}
	return n.children[index]
}

// GetValue returns the value at the given index (leaf nodes only).
func (n *Node) GetValue(index int) []byte {
	if n.IsInternal() || index < 0 || index >= len(n.values) {
		return nil
	}
	return n.values[index]
}

// GetNext returns the next leaf pointer (leaf nodes only).
func (n *Node) GetNext() storage.PageID {
	return n.next
}

// SetNext sets the next leaf pointer (leaf nodes only).
func (n *Node) SetNext(next storage.PageID) {
	if n.IsLeaf() {
		n.next = next
	}
}

// InsertAt inserts a key-value or key-child pair at the given index.
func (n *Node) InsertAt(index int, key []byte, valueOrChild interface{}) error {
	if index < 0 || index > len(n.keys) {
		return fmt.Errorf("invalid index %d", index)
	}

	// Insert key
	n.keys = append(n.keys[:index], append([][]byte{key}, n.keys[index:]...)...)

	if n.IsLeaf() {
		// Insert value
		value, ok := valueOrChild.([]byte)
		if !ok {
			return fmt.Errorf("leaf node requires []byte value")
		}
		n.values = append(n.values[:index], append([][]byte{value}, n.values[index:]...)...)
	} else {
		// Insert child
		child, ok := valueOrChild.(storage.PageID)
		if !ok {
			return fmt.Errorf("internal node requires PageID child")
		}
		n.children = append(n.children[:index+1], append([]storage.PageID{child}, n.children[index+1:]...)...)
	}

	return nil
}

// RemoveAt removes a key-value or key-child pair at the given index.
func (n *Node) RemoveAt(index int) error {
	if index < 0 || index >= len(n.keys) {
		return fmt.Errorf("invalid index %d", index)
	}

	// Remove key
	n.keys = append(n.keys[:index], n.keys[index+1:]...)

	if n.IsLeaf() {
		// Remove value
		n.values = append(n.values[:index], n.values[index+1:]...)
	} else {
		// Remove child (the one after the key)
		n.children = append(n.children[:index+1], n.children[index+2:]...)
	}

	return nil
}

// Split splits the node into two nodes at the midpoint.
// Returns the new node and the key to promote to the parent.
func (n *Node) Split() (*Node, []byte, error) {
	if len(n.keys) < 2 {
		return nil, nil, fmt.Errorf("cannot split node with less than 2 keys")
	}

	mid := len(n.keys) / 2
	promoteKey := n.keys[mid]

	if n.IsLeaf() {
		// Leaf split: promote a copy of the key
		newNode := NewLeafNode()
		newNode.keys = append([][]byte{}, n.keys[mid:]...)
		newNode.values = append([][]byte{}, n.values[mid:]...)
		newNode.next = n.next
		n.next = storage.InvalidPageID // Will be set when we know the new node's page ID

		// Keep left half in original node
		n.keys = n.keys[:mid]
		n.values = n.values[:mid]

		return newNode, promoteKey, nil
	} else {
		// Internal split: promote the middle key (don't duplicate)
		newNode := NewInternalNode()
		newNode.keys = append([][]byte{}, n.keys[mid+1:]...)
		newNode.children = append([]storage.PageID{}, n.children[mid+1:]...)

		// Keep left half in original node
		n.keys = n.keys[:mid]
		n.children = n.children[:mid+1]

		return newNode, promoteKey, nil
	}
}

// Serialize writes the node to a page.
// This is critical for persistence - the in-memory structure must be
// serialized to disk in a format that can be reliably deserialized.
//
// Format:
// | Type (2) | NumKeys (2) | [Keys and Values/Children] | Footer |
//
// Keys are stored with length prefix: | KeyLen (2) | KeyData (var) |
// Values/Children follow the same pattern.
func (n *Node) Serialize() (*storage.Page, error) {
	page := storage.NewPage(storage.PageTypeNode)
	payload := page.Payload()

	// Write header
	binary.LittleEndian.PutUint16(payload[0:2], n.nodeType)
	binary.LittleEndian.PutUint16(payload[2:4], uint16(len(n.keys)))

	offset := 4

	if n.IsLeaf() {
		// Serialize leaf node
		for i := 0; i < len(n.keys); i++ {
			// Write key
			keyLen := len(n.keys[i])
			if offset+2+keyLen > len(payload)-8 {
				return nil, fmt.Errorf("node too large to fit in page")
			}
			binary.LittleEndian.PutUint16(payload[offset:offset+2], uint16(keyLen))
			offset += 2
			copy(payload[offset:], n.keys[i])
			offset += keyLen

			// Write value
			valLen := len(n.values[i])
			if offset+2+valLen > len(payload)-8 {
				return nil, fmt.Errorf("node too large to fit in page")
			}
			binary.LittleEndian.PutUint16(payload[offset:offset+2], uint16(valLen))
			offset += 2
			copy(payload[offset:], n.values[i])
			offset += valLen
		}

		// Write next pointer at the end of the page
		binary.LittleEndian.PutUint64(payload[len(payload)-8:], uint64(n.next))

	} else {
		// Serialize internal node
		// Write first child
		if offset+8 > len(payload) {
			return nil, fmt.Errorf("node too large to fit in page")
		}
		binary.LittleEndian.PutUint64(payload[offset:offset+8], uint64(n.children[0]))
		offset += 8

		for i := 0; i < len(n.keys); i++ {
			// Write key
			keyLen := len(n.keys[i])
			if offset+2+keyLen+8 > len(payload) {
				return nil, fmt.Errorf("node too large to fit in page")
			}
			binary.LittleEndian.PutUint16(payload[offset:offset+2], uint16(keyLen))
			offset += 2
			copy(payload[offset:], n.keys[i])
			offset += keyLen

			// Write child
			binary.LittleEndian.PutUint64(payload[offset:offset+8], uint64(n.children[i+1]))
			offset += 8
		}
	}

	return page, nil
}

// Deserialize reads a node from a page.
func Deserialize(page *storage.Page) (*Node, error) {
	payload := page.Payload()

	// Read header
	nodeType := binary.LittleEndian.Uint16(payload[0:2])
	numKeys := binary.LittleEndian.Uint16(payload[2:4])

	node := &Node{
		nodeType: nodeType,
		keys:     make([][]byte, 0, numKeys),
		page:     page,
	}

	offset := 4

	if nodeType == NodeTypeLeaf {
		// Deserialize leaf node
		node.values = make([][]byte, 0, numKeys)

		for i := 0; i < int(numKeys); i++ {
			// Read key
			if offset+2 > len(payload) {
				return nil, fmt.Errorf("corrupted node: truncated key length")
			}
			keyLen := binary.LittleEndian.Uint16(payload[offset : offset+2])
			offset += 2

			if offset+int(keyLen) > len(payload) {
				return nil, fmt.Errorf("corrupted node: truncated key data")
			}
			key := make([]byte, keyLen)
			copy(key, payload[offset:offset+int(keyLen)])
			node.keys = append(node.keys, key)
			offset += int(keyLen)

			// Read value
			if offset+2 > len(payload) {
				return nil, fmt.Errorf("corrupted node: truncated value length")
			}
			valLen := binary.LittleEndian.Uint16(payload[offset : offset+2])
			offset += 2

			if offset+int(valLen) > len(payload) {
				return nil, fmt.Errorf("corrupted node: truncated value data")
			}
			value := make([]byte, valLen)
			copy(value, payload[offset:offset+int(valLen)])
			node.values = append(node.values, value)
			offset += int(valLen)
		}

		// Read next pointer from end of page
		node.next = storage.PageID(binary.LittleEndian.Uint64(payload[len(payload)-8:]))

	} else if nodeType == NodeTypeInternal {
		// Deserialize internal node
		node.children = make([]storage.PageID, 0, numKeys+1)

		// Read first child
		if offset+8 > len(payload) {
			return nil, fmt.Errorf("corrupted node: truncated first child")
		}
		node.children = append(node.children, storage.PageID(binary.LittleEndian.Uint64(payload[offset:offset+8])))
		offset += 8

		for i := 0; i < int(numKeys); i++ {
			// Read key
			if offset+2 > len(payload) {
				return nil, fmt.Errorf("corrupted node: truncated key length")
			}
			keyLen := binary.LittleEndian.Uint16(payload[offset : offset+2])
			offset += 2

			if offset+int(keyLen) > len(payload) {
				return nil, fmt.Errorf("corrupted node: truncated key data")
			}
			key := make([]byte, keyLen)
			copy(key, payload[offset:offset+int(keyLen)])
			node.keys = append(node.keys, key)
			offset += int(keyLen)

			// Read child
			if offset+8 > len(payload) {
				return nil, fmt.Errorf("corrupted node: truncated child")
			}
			node.children = append(node.children, storage.PageID(binary.LittleEndian.Uint64(payload[offset:offset+8])))
			offset += 8
		}
	} else {
		return nil, fmt.Errorf("invalid node type: %d", nodeType)
	}

	return node, nil
}

// Clone creates a deep copy of the node.
func (n *Node) Clone() *Node {
	clone := &Node{
		nodeType: n.nodeType,
		keys:     make([][]byte, len(n.keys)),
		next:     n.next,
	}

	// Deep copy keys
	for i, key := range n.keys {
		clone.keys[i] = append([]byte{}, key...)
	}

	if n.IsLeaf() {
		clone.values = make([][]byte, len(n.values))
		for i, val := range n.values {
			clone.values[i] = append([]byte{}, val...)
		}
	} else {
		clone.children = make([]storage.PageID, len(n.children))
		copy(clone.children, n.children)
	}

	return clone
}
