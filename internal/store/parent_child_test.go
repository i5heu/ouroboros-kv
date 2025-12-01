package store

import (
	"fmt"
	"os"
	"testing"

	"log/slog"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	"github.com/i5heu/ouroboros-kv/internal/testutil"
	"github.com/i5heu/ouroboros-kv/internal/types"
	"github.com/i5heu/ouroboros-kv/pkg/config"
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
	config := &config.Config{
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
	parentData := applyTestDefaults(types.Data{
		Meta:           []byte("parent metadata"),
		Content:        []byte("I am the parent"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	})

	child1Data := applyTestDefaults(types.Data{
		Meta:           []byte("child1 metadata"),
		Content:        []byte("I am child 1"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	})

	child2Data := applyTestDefaults(types.Data{
		Meta:           []byte("child2 metadata"),
		Content:        []byte("I am child 2"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	})

	grandchildData := applyTestDefaults(types.Data{
		Meta:           []byte("grandchild metadata"),
		Content:        []byte("I am a grandchild"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	})

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
	parentData := applyTestDefaults(types.Data{
		Meta:           []byte("batch parent metadata"),
		Content:        []byte("Batch parent"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	})

	parentKey := expectedKeyForData(parentData)

	child1Data := applyTestDefaults(types.Data{
		Meta:           []byte("batch child1 metadata"),
		Content:        []byte("Batch child 1"),
		Parent:         parentKey,
		RSDataSlices:   3,
		RSParitySlices: 2,
	})

	child2Data := applyTestDefaults(types.Data{
		Meta:           []byte("batch child2 metadata"),
		Content:        []byte("Batch child 2"),
		Parent:         parentKey,
		RSDataSlices:   3,
		RSParitySlices: 2,
	})

	// Batch write all data
	dataList := []types.Data{parentData, child1Data, child2Data}
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

	root1Data := applyTestDefaults(types.Data{
		Content:        []byte("Root 1"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	})
	root1Key, err := kv.WriteData(root1Data)
	require.NoError(t, err)

	root2Data := applyTestDefaults(types.Data{
		Content:        []byte("Root 2"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	})
	root2Key, err := kv.WriteData(root2Data)
	require.NoError(t, err)

	childData := applyTestDefaults(types.Data{
		Content:        []byte("Child of root 1"),
		Parent:         root1Key,
		RSDataSlices:   3,
		RSParitySlices: 2,
	})
	_, err = kv.WriteData(childData)
	require.NoError(t, err)

	roots, err := kv.ListRootKeys()
	require.NoError(t, err)
	assert.Len(t, roots, 2)
	assert.ElementsMatch(t, []hash.Hash{root1Key, root2Key}, roots)
}

func TestDeleteDataLeavesNoRelationshipResidue(t *testing.T) {
	kv, cleanup := setupTestKVForParentChild(t)
	defer cleanup()

	cycles := 5
	childrenPerParent := 3
	grandchildrenPerChild := 2
	if testutil.IsLongEnabled() {
		cycles = 100
		childrenPerParent = 10
		grandchildrenPerChild = 10
	}

	for cycle := 0; cycle < cycles; cycle++ {
		parentLabel := fmt.Sprintf("cycle-%d-parent", cycle)
		parentKey := writeRelationshipData(t, kv, parentLabel, hash.Hash{})

		childKeys := make([]hash.Hash, 0, childrenPerParent)
		grandchildren := make(map[hash.Hash][]hash.Hash)

		for childIdx := 0; childIdx < childrenPerParent; childIdx++ {
			childLabel := fmt.Sprintf("%s-child-%d", parentLabel, childIdx)
			childKey := writeRelationshipData(t, kv, childLabel, parentKey)
			childKeys = append(childKeys, childKey)

			for grandIdx := 0; grandIdx < grandchildrenPerChild; grandIdx++ {
				gcLabel := fmt.Sprintf("%s-grandchild-%d", childLabel, grandIdx)
				gcKey := writeRelationshipData(t, kv, gcLabel, childKey)
				grandchildren[childKey] = append(grandchildren[childKey], gcKey)
			}
		}

		expectedEdges := childrenPerParent + childrenPerParent*grandchildrenPerChild
		assertRelationshipCounts(t, kv, expectedEdges, expectedEdges, "cycle %d after writes", cycle)

		require.NoErrorf(t, kv.DeleteData(parentKey), "cycle %d: delete parent", cycle)
		expectedEdges -= childrenPerParent
		assertRelationshipCounts(t, kv, expectedEdges, expectedEdges, "cycle %d after parent delete", cycle)

		for _, childKey := range childKeys {
			require.NoErrorf(t, kv.DeleteData(childKey), "cycle %d: delete child", cycle)
			expectedEdges -= grandchildrenPerChild
			assertRelationshipCounts(t, kv, expectedEdges, expectedEdges, "cycle %d after child delete", cycle)
			for _, gcKey := range grandchildren[childKey] {
				require.NoErrorf(t, kv.DeleteData(gcKey), "cycle %d: delete grandchild", cycle)
			}
		}

		assertRelationshipCounts(t, kv, 0, 0, "cycle %d final", cycle)

		keys, err := kv.ListKeys()
		require.NoErrorf(t, err, "cycle %d: list keys", cycle)
		if len(keys) != 0 {
			t.Fatalf("cycle %d: expected empty store, found %d keys", cycle, len(keys))
		}
	}
}

func writeRelationshipData(t *testing.T, kv *KV, label string, parent hash.Hash) hash.Hash {
	t.Helper()

	data := applyTestDefaults(types.Data{
		Content:        []byte("rel-content-" + label),
		Meta:           []byte("rel-meta-" + label),
		Parent:         parent,
		RSDataSlices:   3,
		RSParitySlices: 2,
	})

	key, err := kv.WriteData(data)
	require.NoErrorf(t, err, "WriteData(%s)", label)
	return key
}

func assertRelationshipCounts(t *testing.T, kv *KV, expectedParent, expectedChild int, context string, args ...interface{}) {
	t.Helper()
	parentCount, childCount, err := kv.InspectRelationshipCounts()
	require.NoError(t, err)
	require.Equalf(t, expectedParent, parentCount, "parent entries mismatch %s", fmt.Sprintf(context, args...))
	require.Equalf(t, expectedChild, childCount, "child entries mismatch %s", fmt.Sprintf(context, args...))
}
