package ouroboroskv

import (
	"os"
	"testing"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/sirupsen/logrus"
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
		Logger:           logrus.New(),
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
		Key:                     hash.HashString("parent-test"),
		Content:                 []byte("I am the parent"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	child1Data := Data{
		Key:                     hash.HashString("child1-test"),
		Content:                 []byte("I am child 1"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	child2Data := Data{
		Key:                     hash.HashString("child2-test"),
		Content:                 []byte("I am child 2"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	grandchildData := Data{
		Key:                     hash.HashString("grandchild-test"),
		Content:                 []byte("I am a grandchild"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	// Store the parent first
	err := kv.WriteData(parentData)
	require.NoError(t, err)

	// Store children with parent relationship
	child1Data.Parent = parentData.Key
	err = kv.WriteData(child1Data)
	require.NoError(t, err)

	child2Data.Parent = parentData.Key
	err = kv.WriteData(child2Data)
	require.NoError(t, err)

	// Store grandchild with child1 as parent
	grandchildData.Parent = child1Data.Key
	err = kv.WriteData(grandchildData)
	require.NoError(t, err)

	// Test GetChildren
	children, err := kv.GetChildren(parentData.Key)
	require.NoError(t, err)
	assert.Len(t, children, 2)

	// Check if children are correct (order doesn't matter)
	childFound1 := false
	childFound2 := false
	for _, child := range children {
		if child == child1Data.Key {
			childFound1 = true
		}
		if child == child2Data.Key {
			childFound2 = true
		}
	}
	assert.True(t, childFound1, "Child 1 not found")
	assert.True(t, childFound2, "Child 2 not found")

	// Test GetParent
	parent, err := kv.GetParent(child1Data.Key)
	require.NoError(t, err)
	assert.Equal(t, parentData.Key, parent)

	parent, err = kv.GetParent(child2Data.Key)
	require.NoError(t, err)
	assert.Equal(t, parentData.Key, parent)

	parent, err = kv.GetParent(grandchildData.Key)
	require.NoError(t, err)
	assert.Equal(t, child1Data.Key, parent)

	// Test GetDescendants
	descendants, err := kv.GetDescendants(parentData.Key)
	require.NoError(t, err)
	assert.Len(t, descendants, 3) // child1, child2, grandchild

	// Check descendants
	descFound1 := false
	descFound2 := false
	descFoundGrand := false
	for _, desc := range descendants {
		if desc == child1Data.Key {
			descFound1 = true
		}
		if desc == child2Data.Key {
			descFound2 = true
		}
		if desc == grandchildData.Key {
			descFoundGrand = true
		}
	}
	assert.True(t, descFound1, "Child 1 not found in descendants")
	assert.True(t, descFound2, "Child 2 not found in descendants")
	assert.True(t, descFoundGrand, "Grandchild not found in descendants")

	// Test GetAncestors
	ancestors, err := kv.GetAncestors(grandchildData.Key)
	require.NoError(t, err)
	assert.Len(t, ancestors, 2) // child1, parent

	// Check ancestors
	ancFoundChild1 := false
	ancFoundParent := false
	for _, anc := range ancestors {
		if anc == child1Data.Key {
			ancFoundChild1 = true
		}
		if anc == parentData.Key {
			ancFoundParent = true
		}
	}
	assert.True(t, ancFoundChild1, "Child 1 not found in ancestors")
	assert.True(t, ancFoundParent, "Parent not found in ancestors")

	// Test GetRoots
	roots, err := kv.GetRoots()
	require.NoError(t, err)
	assert.Len(t, roots, 1)
	assert.Equal(t, parentData.Key, roots[0])
}

func TestBatchWriteWithRelationships(t *testing.T) {
	kv, cleanup := setupTestKVForParentChild(t)
	defer cleanup()

	// Create test data
	parentData := Data{
		Key:                     hash.HashString("batch-parent"),
		Content:                 []byte("Batch parent"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	child1Data := Data{
		Key:                     hash.HashString("batch-child1"),
		Content:                 []byte("Batch child 1"),
		Parent:                  parentData.Key,
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	child2Data := Data{
		Key:                     hash.HashString("batch-child2"),
		Content:                 []byte("Batch child 2"),
		Parent:                  parentData.Key,
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	// Batch write all data
	dataList := []Data{parentData, child1Data, child2Data}
	err := kv.BatchWriteData(dataList)
	require.NoError(t, err)

	// Verify relationships
	children, err := kv.GetChildren(parentData.Key)
	require.NoError(t, err)
	assert.Len(t, children, 2)

	parent, err := kv.GetParent(child1Data.Key)
	require.NoError(t, err)
	assert.Equal(t, parentData.Key, parent)

	roots, err := kv.GetRoots()
	require.NoError(t, err)
	assert.Len(t, roots, 1)
	assert.Equal(t, parentData.Key, roots[0])
}
