package btree

import (
	"bytes"
	"fmt"

	"github.com/khoale/godb-3000/internal/storage"
)

// BTree represents a B+tree index stored on disk.
// B+trees are optimal for databases because:
// 1. Logarithmic search time: O(log n)
// 2. Efficient range queries: leaf nodes are linked
// 3. Sequential disk access: child nodes are stored near each other
// 4. High fanout reduces tree height, minimizing disk seeks
//
// Key properties:
// - All values are stored in leaf nodes
// - Internal nodes only contain keys and child pointers
// - Leaf nodes are linked for efficient range scans
// - Tree is always balanced (all leaves at same depth)
type BTree struct {
	pager  *storage.Pager
	rootID storage.PageID
	depth  int
}

// NewBTree creates a new B+tree with an empty root.
func NewBTree(pager *storage.Pager) (*BTree, error) {
	// Allocate root page
	rootID, err := pager.AllocatePage()
	if err != nil {
		return nil, fmt.Errorf("failed to allocate root page: %w", err)
	}

	// Create empty leaf node as root
	root := NewLeafNode()
	page, err := root.Serialize()
	if err != nil {
		return nil, fmt.Errorf("failed to serialize root: %w", err)
	}

	if err := pager.WritePage(rootID, page); err != nil {
		return nil, fmt.Errorf("failed to write root: %w", err)
	}

	return &BTree{
		pager:  pager,
		rootID: rootID,
		depth:  1,
	}, nil
}

// LoadBTree loads an existing B+tree from the given root page.
func LoadBTree(pager *storage.Pager, rootID storage.PageID) (*BTree, error) {
	// Verify root exists
	page, err := pager.ReadPage(rootID)
	if err != nil {
		return nil, fmt.Errorf("failed to read root page: %w", err)
	}

	_, err = Deserialize(page)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize root: %w", err)
	}

	// Calculate depth by traversing to a leaf
	depth := 1
	currentID := rootID
	for {
		page, err := pager.ReadPage(currentID)
		if err != nil {
			return nil, err
		}
		node, err := Deserialize(page)
		if err != nil {
			return nil, err
		}
		if node.IsLeaf() {
			break
		}
		// Get leftmost child to traverse down
		childID := node.GetChild(0)
		if !childID.IsValid() {
			return nil, fmt.Errorf("corrupted internal node at page %d: missing leftmost child", currentID)
		}
		depth++
		currentID = childID
	}

	return &BTree{
		pager:  pager,
		rootID: rootID,
		depth:  depth,
	}, nil
}

// RootID returns the page ID of the root node.
func (bt *BTree) RootID() storage.PageID {
	return bt.rootID
}

// Depth returns the depth of the tree (number of levels).
func (bt *BTree) Depth() int {
	return bt.depth
}

// Get searches for a key and returns its value if found.
func (bt *BTree) Get(key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, fmt.Errorf("empty key")
	}

	// Start from root and traverse down
	currentID := bt.rootID

	for {
		page, err := bt.pager.ReadPage(currentID)
		if err != nil {
			return nil, false, err
		}

		node, err := Deserialize(page)
		if err != nil {
			return nil, false, err
		}

		if node.IsLeaf() {
			// Search in leaf node
			idx := bt.searchNode(node, key)
			if idx < node.NumKeys() && bytes.Equal(node.GetKey(idx), key) {
				return node.GetValue(idx), true, nil
			}
			return nil, false, nil
		}

		// Internal node: find child to descend into
		// In B+trees, the separator key is the first key of the right child
		// If search_key >= separator, go right; otherwise go left
		idx := 0
		for idx < node.NumKeys() && bytes.Compare(key, node.GetKey(idx)) >= 0 {
			idx++
		}
		currentID = node.GetChild(idx)
	}
}

// Insert inserts or updates a key-value pair.
// If the key exists, its value is updated. Otherwise, a new entry is created.
func (bt *BTree) Insert(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("empty key")
	}
	if len(key) > storage.MaxKeySize {
		return fmt.Errorf("key too large: %d bytes (max %d)", len(key), storage.MaxKeySize)
	}
	if len(value) > storage.MaxValueSize {
		return fmt.Errorf("value too large: %d bytes (max %d)", len(value), storage.MaxValueSize)
	}

	// Load root
	rootPage, err := bt.pager.ReadPage(bt.rootID)
	if err != nil {
		return err
	}
	root, err := Deserialize(rootPage)
	if err != nil {
		return err
	}

	// Perform insertion
	splitInfo, err := bt.insertNonFull(bt.rootID, root, key, value)
	if err != nil {
		return err
	}

	// If root split, create new root
	if splitInfo != nil {
		newRoot := NewInternalNode()
		newRoot.keys = append(newRoot.keys, splitInfo.key)
		newRoot.children = append(newRoot.children, bt.rootID, splitInfo.rightID)

		// Allocate page for new root
		newRootID, err := bt.pager.AllocatePage()
		if err != nil {
			return err
		}

		page, err := newRoot.Serialize()
		if err != nil {
			return err
		}

		if err := bt.pager.WritePage(newRootID, page); err != nil {
			return err
		}

		bt.rootID = newRootID
		bt.depth++
	}

	return nil
}

// splitInfo contains information about a node split.
type splitInfo struct {
	key     []byte         // Key to promote to parent
	rightID storage.PageID // Page ID of the new right node
}

// insertNonFull inserts a key-value pair into a node that has room.
// Returns split information if the node had to be split.
func (bt *BTree) insertNonFull(nodeID storage.PageID, node *Node, key, value []byte) (*splitInfo, error) {
	if node.IsLeaf() {
		// Find insertion position
		idx := bt.searchNode(node, key)

		// Check if key exists (update case)
		if idx < node.NumKeys() && bytes.Equal(node.GetKey(idx), key) {
			// Update existing value
			node.values[idx] = append([]byte{}, value...)
			page, err := node.Serialize()
			if err != nil {
				return nil, err
			}
			return nil, bt.pager.WritePage(nodeID, page)
		}

		// Insert new key-value
		if err := node.InsertAt(idx, key, value); err != nil {
			return nil, err
		}

		// Check if node needs splitting
		if node.IsFull() {
			return bt.splitNode(nodeID, node)
		}

		// Write updated node
		page, err := node.Serialize()
		if err != nil {
			return nil, err
		}
		return nil, bt.pager.WritePage(nodeID, page)
	}

	// Internal node: find child to insert into
	idx := bt.searchNode(node, key)
	childID := node.GetChild(idx)

	// Load child
	childPage, err := bt.pager.ReadPage(childID)
	if err != nil {
		return nil, err
	}
	child, err := Deserialize(childPage)
	if err != nil {
		return nil, err
	}

	// Recursively insert into child
	childSplit, err := bt.insertNonFull(childID, child, key, value)
	if err != nil {
		return nil, err
	}

	// If child didn't split, we're done
	if childSplit == nil {
		return nil, nil
	}

	// Child split: we need to:
	// 1. Keep the existing child pointer (it's now the left half after split)
	// 2. Insert the promoted key at position idx
	// 3. Insert the right child pointer after the promoted key
	// The existing child at position idx stays as-is (it's already the left half)
	// We just need to insert the key and add the right child

	// Insert key at idx, and right child at idx+1
	node.keys = append(node.keys[:idx], append([][]byte{childSplit.key}, node.keys[idx:]...)...)
	node.children = append(node.children[:idx+1], append([]storage.PageID{childSplit.rightID}, node.children[idx+1:]...)...)

	// Check if this node needs splitting
	if node.IsFull() {
		return bt.splitNode(nodeID, node)
	}

	// Write updated node
	page, err := node.Serialize()
	if err != nil {
		return nil, err
	}
	return nil, bt.pager.WritePage(nodeID, page)
}

// splitNode splits a full node into two nodes.
func (bt *BTree) splitNode(nodeID storage.PageID, node *Node) (*splitInfo, error) {
	// Split the node
	rightNode, promoteKey, err := node.Split()
	if err != nil {
		return nil, err
	}

	// Allocate page for right node
	rightID, err := bt.pager.AllocatePage()
	if err != nil {
		return nil, err
	}

	// If splitting leaf nodes, update next pointers
	if node.IsLeaf() {
		rightNode.SetNext(node.GetNext())
		node.SetNext(rightID)
	}

	// Write both nodes
	leftPage, err := node.Serialize()
	if err != nil {
		return nil, err
	}
	if err := bt.pager.WritePage(nodeID, leftPage); err != nil {
		return nil, err
	}

	rightPage, err := rightNode.Serialize()
	if err != nil {
		return nil, err
	}
	if err := bt.pager.WritePage(rightID, rightPage); err != nil {
		return nil, err
	}

	return &splitInfo{
		key:     promoteKey,
		rightID: rightID,
	}, nil
}

// Delete removes a key from the tree.
// Returns true if the key was found and deleted.
func (bt *BTree) Delete(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, fmt.Errorf("empty key")
	}

	rootPage, err := bt.pager.ReadPage(bt.rootID)
	if err != nil {
		return false, err
	}
	root, err := Deserialize(rootPage)
	if err != nil {
		return false, err
	}

	deleted, err := bt.deleteFromNode(bt.rootID, root, key)
	if err != nil {
		return false, err
	}

	if !deleted {
		return false, nil
	}

	// If root is internal and has only one child, make that child the new root
	if root.IsInternal() && root.NumKeys() == 0 {
		oldRootID := bt.rootID
		bt.rootID = root.GetChild(0)
		bt.depth--
		// Free the old root page
		if err := bt.pager.FreePage(oldRootID); err != nil {
			return false, fmt.Errorf("failed to free old root page: %w", err)
		}
	}

	return true, nil
}

// deleteFromNode deletes a key from a subtree.
func (bt *BTree) deleteFromNode(nodeID storage.PageID, node *Node, key []byte) (bool, error) {
	if node.IsLeaf() {
		// Find key in leaf
		idx := bt.searchNode(node, key)
		if idx >= node.NumKeys() || !bytes.Equal(node.GetKey(idx), key) {
			return false, nil // Key not found
		}

		// Remove key
		if err := node.RemoveAt(idx); err != nil {
			return false, err
		}

		// Write updated node
		page, err := node.Serialize()
		if err != nil {
			return false, err
		}
		return true, bt.pager.WritePage(nodeID, page)
	}

	// Internal node: find child containing the key
	idx := bt.searchNode(node, key)
	childID := node.GetChild(idx)

	childPage, err := bt.pager.ReadPage(childID)
	if err != nil {
		return false, err
	}
	child, err := Deserialize(childPage)
	if err != nil {
		return false, err
	}

	// Recursively delete from child
	return bt.deleteFromNode(childID, child, key)
}

// searchNode performs binary search to find the position for a key in a node.
// Returns the index where the key is or should be inserted.
func (bt *BTree) searchNode(node *Node, key []byte) int {
	left, right := 0, node.NumKeys()

	for left < right {
		mid := (left + right) / 2
		cmp := bytes.Compare(key, node.GetKey(mid))

		if cmp < 0 {
			right = mid
		} else if cmp > 0 {
			left = mid + 1
		} else {
			return mid
		}
	}

	return left
}

// Iterator provides sequential access to key-value pairs.
type Iterator struct {
	bt        *BTree
	currentID storage.PageID
	current   *Node
	index     int
	startKey  []byte
	endKey    []byte
	done      bool
}

// NewIterator creates an iterator for range queries.
// If startKey is nil, iteration starts from the beginning.
// If endKey is nil, iteration continues to the end.
func (bt *BTree) NewIterator(startKey, endKey []byte) (*Iterator, error) {
	iter := &Iterator{
		bt:       bt,
		startKey: startKey,
		endKey:   endKey,
		done:     false,
	}

	// Find the starting leaf node
	if startKey == nil {
		// Start from leftmost leaf
		currentID := bt.rootID
		for {
			page, err := bt.pager.ReadPage(currentID)
			if err != nil {
				return nil, err
			}
			node, err := Deserialize(page)
			if err != nil {
				return nil, err
			}

			if node.IsLeaf() {
				iter.currentID = currentID
				iter.current = node
				iter.index = 0
				break
			}

			currentID = node.GetChild(0)
		}
	} else {
		// Find leaf containing startKey
		currentID := bt.rootID
		for {
			page, err := bt.pager.ReadPage(currentID)
			if err != nil {
				return nil, err
			}
			node, err := Deserialize(page)
			if err != nil {
				return nil, err
			}

			if node.IsLeaf() {
				iter.currentID = currentID
				iter.current = node
				iter.index = bt.searchNode(node, startKey)
				break
			}

			idx := bt.searchNode(node, startKey)
			currentID = node.GetChild(idx)
		}
	}

	return iter, nil
}

// Next advances the iterator and returns the next key-value pair.
// Returns false when iteration is complete.
func (iter *Iterator) Next() ([]byte, []byte, bool) {
	if iter.done || iter.current == nil {
		return nil, nil, false
	}

	// Check if we're past the end of current node
	if iter.index >= iter.current.NumKeys() {
		// Move to next leaf
		nextID := iter.current.GetNext()
		if !nextID.IsValid() {
			iter.done = true
			return nil, nil, false
		}

		page, err := iter.bt.pager.ReadPage(nextID)
		if err != nil {
			iter.done = true
			return nil, nil, false
		}

		node, err := Deserialize(page)
		if err != nil {
			iter.done = true
			return nil, nil, false
		}

		iter.currentID = nextID
		iter.current = node
		iter.index = 0
	}

	// Get current key-value
	key := iter.current.GetKey(iter.index)
	value := iter.current.GetValue(iter.index)

	// Check if we've passed endKey
	if iter.endKey != nil && bytes.Compare(key, iter.endKey) >= 0 {
		iter.done = true
		return nil, nil, false
	}

	iter.index++
	return key, value, true
}

// Close releases resources associated with the iterator.
func (iter *Iterator) Close() error {
	iter.current = nil
	iter.done = true
	return nil
}

// Scan performs a range query and calls the callback for each key-value pair.
// If callback returns false, iteration stops.
func (bt *BTree) Scan(startKey, endKey []byte, callback func(key, value []byte) bool) error {
	iter, err := bt.NewIterator(startKey, endKey)
	if err != nil {
		return err
	}
	defer iter.Close()

	for {
		key, value, ok := iter.Next()
		if !ok {
			break
		}
		if !callback(key, value) {
			break
		}
	}

	return nil
}

// Stats returns statistics about the B+tree.
type BTreeStats struct {
	Depth        int
	NumNodes     int
	NumLeaves    int
	NumKeys      int
	AvgFanout    float64
	AvgKeySize   float64
	AvgValueSize float64
}

// Stats collects and returns statistics about the tree structure.
func (bt *BTree) Stats() (*BTreeStats, error) {
	stats := &BTreeStats{
		Depth: bt.depth,
	}

	var totalKeys, totalKeySize, totalValueSize int
	var totalChildren int

	// Traverse tree and collect stats
	if err := bt.traverseStats(bt.rootID, stats, &totalKeys, &totalKeySize, &totalValueSize, &totalChildren); err != nil {
		return nil, err
	}

	stats.NumKeys = totalKeys
	if totalKeys > 0 {
		stats.AvgKeySize = float64(totalKeySize) / float64(totalKeys)
		stats.AvgValueSize = float64(totalValueSize) / float64(totalKeys)
	}
	if stats.NumNodes-stats.NumLeaves > 0 {
		stats.AvgFanout = float64(totalChildren) / float64(stats.NumNodes-stats.NumLeaves)
	}

	return stats, nil
}

func (bt *BTree) traverseStats(nodeID storage.PageID, stats *BTreeStats, totalKeys, totalKeySize, totalValueSize, totalChildren *int) error {
	page, err := bt.pager.ReadPage(nodeID)
	if err != nil {
		return err
	}

	node, err := Deserialize(page)
	if err != nil {
		return err
	}

	stats.NumNodes++

	if node.IsLeaf() {
		stats.NumLeaves++
		// Only count keys in leaf nodes (actual data keys, not separator keys)
		*totalKeys += node.NumKeys()
		// Calculate key and value sizes
		for i := 0; i < node.NumKeys(); i++ {
			*totalKeySize += len(node.GetKey(i))
			*totalValueSize += len(node.GetValue(i))
		}
	} else {
		*totalChildren += len(node.children)
		// Recursively traverse children
		for i := 0; i < len(node.children); i++ {
			if err := bt.traverseStats(node.GetChild(i), stats, totalKeys, totalKeySize, totalValueSize, totalChildren); err != nil {
				return err
			}
		}
	}

	return nil
}
