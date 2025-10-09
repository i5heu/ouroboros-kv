package ouroboroskv

import (
	"os"
	"testing"

	"log/slog"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestKVForParentChild(t *testing.T) (*KV, func()) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "ouroboros-kv-parent-child-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Create a crypt instance for testing
	cryptInstance := crypt.New()

	// Create config
	config := &Config{
		Paths:            []string{tempDir},
		MinimumFreeSpace: 1, // 1GB minimum
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	// Initialize KV
	kv, err := Init(cryptInstance, config)
	if err != nil {
		t.Fatalf("Failed to initialize KV: %v", err)
	}

	// Return cleanup function
	cleanup := func() {
		kv.badgerDB.Close()
		os.RemoveAll(tempDir)
	}

	return kv, cleanup
}

func TestParentChildRelationships(t *testing.T) {
	kv, cleanup := setupTestKVForParentChild(t)
	defer cleanup()

	// Create test data with relationships
	parentData := Data{
		Content:                 []byte("I am the parent"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	child1Data := Data{
		Content:                 []byte("I am child 1"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	child2Data := Data{
		Content:                 []byte("I am child 2"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	grandchildData := Data{
		Content:                 []byte("I am a grandchild"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	// Store the parent first
	parentKey, err := kv.WriteData(parentData)
	require.NoError(t, err)

	// Store children with parent relationship
	child1Data.Parent = parentKey
	child1Key, err := kv.WriteData(child1Data)
	require.NoError(t, err)

	child2Data.Parent = parentKey
	child2Key, err := kv.WriteData(child2Data)
	require.NoError(t, err)

	// Store grandchild with child1 as parent
	grandchildData.Parent = child1Key
	grandchildKey, err := kv.WriteData(grandchildData)
	require.NoError(t, err)

	// Test GetChildren
	children, err := kv.GetChildren(parentKey)
	require.NoError(t, err)
	assert.Len(t, children, 2)

	// Check if children are correct (order doesn't matter)
	childFound1 := false
	childFound2 := false
	for _, child := range children {
		if child == child1Key {
			childFound1 = true
		}
		if child == child2Key {
			childFound2 = true
		}
	}
	assert.True(t, childFound1, "Child 1 not found")
	assert.True(t, childFound2, "Child 2 not found")

	// Test GetParent
	parent, err := kv.GetParent(child1Key)
	require.NoError(t, err)
	assert.Equal(t, parentKey, parent)

	parent, err = kv.GetParent(child2Key)
	require.NoError(t, err)
	assert.Equal(t, parentKey, parent)

	parent, err = kv.GetParent(grandchildKey)
	require.NoError(t, err)
	assert.Equal(t, child1Key, parent)

	// Test GetDescendants
	descendants, err := kv.GetDescendants(parentKey)
	require.NoError(t, err)
	assert.Len(t, descendants, 3) // child1, child2, grandchild

	// Check descendants
	descFound1 := false
	descFound2 := false
	descFoundGrand := false
	for _, desc := range descendants {
		if desc == child1Key {
			descFound1 = true
		}
		if desc == child2Key {
			descFound2 = true
		}
		if desc == grandchildKey {
			descFoundGrand = true
		}
	}
	assert.True(t, descFound1, "Child 1 not found in descendants")
	assert.True(t, descFound2, "Child 2 not found in descendants")
	assert.True(t, descFoundGrand, "Grandchild not found in descendants")

	// Test GetAncestors
	ancestors, err := kv.GetAncestors(grandchildKey)
	require.NoError(t, err)
	assert.Len(t, ancestors, 2) // child1, parent

	// Check ancestors
	ancFoundChild1 := false
	ancFoundParent := false
	for _, anc := range ancestors {
		if anc == child1Key {
			ancFoundChild1 = true
		}
		if anc == parentKey {
			ancFoundParent = true
		}
	}
	assert.True(t, ancFoundChild1, "Child 1 not found in ancestors")
	assert.True(t, ancFoundParent, "Parent not found in ancestors")

	// Test GetRoots
	roots, err := kv.GetRoots()
	require.NoError(t, err)
	assert.Len(t, roots, 1)
	assert.Equal(t, parentKey, roots[0])
}

func TestBatchWriteWithRelationships(t *testing.T) {
	kv, cleanup := setupTestKVForParentChild(t)
	defer cleanup()

	// Create test data
	parentData := Data{
		Content:                 []byte("Batch parent"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	parentKey := hash.HashBytes(parentData.Content)

	child1Data := Data{
		Content:                 []byte("Batch child 1"),
		Parent:                  parentKey,
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	child2Data := Data{
		Content:                 []byte("Batch child 2"),
		Parent:                  parentKey,
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	// Batch write all data
	dataList := []Data{parentData, child1Data, child2Data}
	keys, err := kv.BatchWriteData(dataList)
	require.NoError(t, err)
	require.Len(t, keys, len(dataList))
	parentKeyOut := keys[0]
	child1KeyOut := keys[1]
	child2KeyOut := keys[2]

	// Verify relationships
	children, err := kv.GetChildren(parentKeyOut)
	require.NoError(t, err)
	assert.Len(t, children, 2)

	parent, err := kv.GetParent(child1KeyOut)
	require.NoError(t, err)
	assert.Equal(t, parentKeyOut, parent)

	parent, err = kv.GetParent(child2KeyOut)
	require.NoError(t, err)
	assert.Equal(t, parentKeyOut, parent)

	roots, err := kv.GetRoots()
	require.NoError(t, err)
	assert.Len(t, roots, 1)
	assert.Equal(t, parentKeyOut, roots[0])
}

func TestListRootKeys(t *testing.T) {
	kv, cleanup := setupTestKVForParentChild(t)
	defer cleanup()

	root1Data := Data{
		Content:                 []byte("Root 1"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}
	root1Key, err := kv.WriteData(root1Data)
	require.NoError(t, err)

	root2Data := Data{
		Content:                 []byte("Root 2"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}
	root2Key, err := kv.WriteData(root2Data)
	require.NoError(t, err)

	childData := Data{
		Content:                 []byte("Child of root 1"),
		Parent:                  root1Key,
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}
	_, err = kv.WriteData(childData)
	require.NoError(t, err)

	roots, err := kv.ListRootKeys()
	require.NoError(t, err)
	assert.Len(t, roots, 2)
	assert.ElementsMatch(t, []hash.Hash{root1Key, root2Key}, roots)
}
