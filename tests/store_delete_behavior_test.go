package ouroboroskv__test

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"testing"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	ouroboroskv "github.com/i5heu/ouroboros-kv"
)

func TestDeleteOrphanedChildren(t *testing.T) {
	store, cleanup := newDeleteTestStore(t)
	defer cleanup()

	// Create hierarchy: root -> parent -> child
	rootData := newDeleteTestData("root", ouroboroskv.Hash{})
	rootKey, err := store.WriteData(rootData)
	if err != nil {
		t.Fatalf("WriteData(root) failed: %v", err)
	}

	parentData := newDeleteTestData("parent", rootKey)
	parentKey, err := store.WriteData(parentData)
	if err != nil {
		t.Fatalf("WriteData(parent) failed: %v", err)
	}

	childData := newDeleteTestData("child", parentKey)
	childKey, err := store.WriteData(childData)
	if err != nil {
		t.Fatalf("WriteData(child) failed: %v", err)
	}

	// Verify hierarchy exists
	children, err := store.GetChildren(parentKey)
	if err != nil {
		t.Fatalf("GetChildren(parent) failed: %v", err)
	}
	if len(children) != 1 || children[0] != childKey {
		t.Fatalf("Expected parent to have one child")
	}

	// Delete parent - child should become orphaned (parent reset to zero)
	if err := store.DeleteData(parentKey); err != nil {
		t.Fatalf("DeleteData(parent) failed: %v", err)
	}

	// Child should still exist
	exists, err := store.DataExists(childKey)
	if err != nil {
		t.Fatalf("DataExists(child) failed: %v", err)
	}
	if !exists {
		t.Fatalf("Child should still exist after parent deletion")
	}

	// Child's parent should now be empty
	childParent, err := store.GetParent(childKey)
	if err != nil {
		t.Fatalf("GetParent(child) failed: %v", err)
	}
	if childParent != (ouroboroskv.Hash{}) {
		t.Fatalf("Child's parent should be empty after parent deletion, got %x", childParent)
	}

	// Child should now be a root
	roots, err := store.GetRoots()
	if err != nil {
		t.Fatalf("GetRoots failed: %v", err)
	}
	foundChild := false
	for _, root := range roots {
		if root == childKey {
			foundChild = true
			break
		}
	}
	if !foundChild {
		t.Fatalf("Orphaned child should become a root")
	}

	// Root should no longer have parent as descendant
	descendants, err := store.GetDescendants(rootKey)
	if err != nil {
		t.Fatalf("GetDescendants(root) failed: %v", err)
	}
	for _, desc := range descendants {
		if desc == parentKey {
			t.Fatalf("Deleted parent should not appear in descendants")
		}
	}
}

func TestDeleteWithMultipleChildren(t *testing.T) {
	store, cleanup := newDeleteTestStore(t)
	defer cleanup()

	// Create parent with 3 children
	parentData := newDeleteTestData("parent", ouroboroskv.Hash{})
	parentKey, err := store.WriteData(parentData)
	if err != nil {
		t.Fatalf("WriteData(parent) failed: %v", err)
	}

	var childKeys []ouroboroskv.Hash
	for i := 0; i < 3; i++ {
		childData := newDeleteTestData(fmt.Sprintf("child-%d", i), parentKey)
		childKey, err := store.WriteData(childData)
		if err != nil {
			t.Fatalf("WriteData(child-%d) failed: %v", i, err)
		}
		childKeys = append(childKeys, childKey)
	}

	// Verify all children are linked
	children, err := store.GetChildren(parentKey)
	if err != nil {
		t.Fatalf("GetChildren(parent) failed: %v", err)
	}
	if len(children) != 3 {
		t.Fatalf("Expected 3 children, got %d", len(children))
	}

	// Delete parent
	if err := store.DeleteData(parentKey); err != nil {
		t.Fatalf("DeleteData(parent) failed: %v", err)
	}

	// All children should become roots
	roots, err := store.GetRoots()
	if err != nil {
		t.Fatalf("GetRoots failed: %v", err)
	}

	for _, childKey := range childKeys {
		found := false
		for _, root := range roots {
			if root == childKey {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Child %x should be a root after parent deletion", childKey)
		}

		// Verify each child has no parent
		parent, err := store.GetParent(childKey)
		if err != nil {
			t.Fatalf("GetParent(child) failed: %v", err)
		}
		if parent != (ouroboroskv.Hash{}) {
			t.Fatalf("Child should have no parent after parent deletion")
		}
	}
}

func TestDeleteMiddleOfChain(t *testing.T) {
	store, cleanup := newDeleteTestStore(t)
	defer cleanup()

	// Create chain: grandparent -> parent -> child -> grandchild
	grandparentData := newDeleteTestData("grandparent", ouroboroskv.Hash{})
	grandparentKey, err := store.WriteData(grandparentData)
	if err != nil {
		t.Fatalf("WriteData(grandparent) failed: %v", err)
	}

	parentData := newDeleteTestData("parent", grandparentKey)
	parentKey, err := store.WriteData(parentData)
	if err != nil {
		t.Fatalf("WriteData(parent) failed: %v", err)
	}

	childData := newDeleteTestData("child", parentKey)
	childKey, err := store.WriteData(childData)
	if err != nil {
		t.Fatalf("WriteData(child) failed: %v", err)
	}

	grandchildData := newDeleteTestData("grandchild", childKey)
	grandchildKey, err := store.WriteData(grandchildData)
	if err != nil {
		t.Fatalf("WriteData(grandchild) failed: %v", err)
	}

	// Verify chain before deletion
	descendants, err := store.GetDescendants(grandparentKey)
	if err != nil {
		t.Fatalf("GetDescendants(grandparent) failed: %v", err)
	}
	if len(descendants) != 3 {
		t.Fatalf("Expected 3 descendants, got %d", len(descendants))
	}

	// Delete middle node (child)
	if err := store.DeleteData(childKey); err != nil {
		t.Fatalf("DeleteData(child) failed: %v", err)
	}

	// Grandchild should be orphaned, not connected to grandparent
	grandchildParent, err := store.GetParent(grandchildKey)
	if err != nil {
		t.Fatalf("GetParent(grandchild) failed: %v", err)
	}
	if grandchildParent != (ouroboroskv.Hash{}) {
		t.Fatalf("Grandchild should be orphaned after middle deletion")
	}

	// Grandparent descendants should only include parent now
	descendants, err = store.GetDescendants(grandparentKey)
	if err != nil {
		t.Fatalf("GetDescendants(grandparent) after delete failed: %v", err)
	}
	if len(descendants) != 1 || descendants[0] != parentKey {
		t.Fatalf("Grandparent should only have parent as descendant")
	}

	// Grandchild should be a root
	grandchildParent, err = store.GetParent(grandchildKey)
	if err != nil {
		t.Fatalf("GetParent(grandchild) failed: %v", err)
	}
	if grandchildParent != (ouroboroskv.Hash{}) {
		t.Fatalf("Grandchild should have no parent")
	}
}

func TestDeleteNonExistentKey(t *testing.T) {
	store, cleanup := newDeleteTestStore(t)
	defer cleanup()

	nonExistentKey := hash.HashString("non-existent")

	err := store.DeleteData(nonExistentKey)
	if err == nil {
		t.Fatalf("DeleteData should fail for non-existent key")
	}
}

func TestDeleteSameKeyTwice(t *testing.T) {
	store, cleanup := newDeleteTestStore(t)
	defer cleanup()

	data := newDeleteTestData("test", ouroboroskv.Hash{})
	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	// First deletion should succeed
	if err := store.DeleteData(key); err != nil {
		t.Fatalf("First DeleteData failed: %v", err)
	}

	// Second deletion should fail
	if err := store.DeleteData(key); err == nil {
		t.Fatalf("Second DeleteData should fail")
	}
}

func TestDeletePreservesUnrelatedData(t *testing.T) {
	store, cleanup := newDeleteTestStore(t)
	defer cleanup()

	// Create two independent trees
	root1Data := newDeleteTestData("root1", ouroboroskv.Hash{})
	root1Key, err := store.WriteData(root1Data)
	if err != nil {
		t.Fatalf("WriteData(root1) failed: %v", err)
	}

	child1Data := newDeleteTestData("child1", root1Key)
	child1Key, err := store.WriteData(child1Data)
	if err != nil {
		t.Fatalf("WriteData(child1) failed: %v", err)
	}

	root2Data := newDeleteTestData("root2", ouroboroskv.Hash{})
	root2Key, err := store.WriteData(root2Data)
	if err != nil {
		t.Fatalf("WriteData(root2) failed: %v", err)
	}

	child2Data := newDeleteTestData("child2", root2Key)
	child2Key, err := store.WriteData(child2Data)
	if err != nil {
		t.Fatalf("WriteData(child2) failed: %v", err)
	}

	// Delete first tree
	if err := store.DeleteData(root1Key); err != nil {
		t.Fatalf("DeleteData(root1) failed: %v", err)
	}

	// Second tree should be intact
	exists, err := store.DataExists(root2Key)
	if err != nil || !exists {
		t.Fatalf("root2 should still exist")
	}

	exists, err = store.DataExists(child2Key)
	if err != nil || !exists {
		t.Fatalf("child2 should still exist")
	}

	// Verify relationships in second tree
	children, err := store.GetChildren(root2Key)
	if err != nil {
		t.Fatalf("GetChildren(root2) failed: %v", err)
	}
	if len(children) != 1 || children[0] != child2Key {
		t.Fatalf("root2 should still have child2")
	}

	parent, err := store.GetParent(child2Key)
	if err != nil {
		t.Fatalf("GetParent(child2) failed: %v", err)
	}
	if parent != root2Key {
		t.Fatalf("child2 should still have root2 as parent")
	}

	// First tree should be gone
	exists, err = store.DataExists(root1Key)
	if err != nil || exists {
		t.Fatalf("root1 should not exist")
	}

	// child1 should be orphaned
	exists, err = store.DataExists(child1Key)
	if err != nil || !exists {
		t.Fatalf("child1 should still exist but orphaned")
	}

	parent, err = store.GetParent(child1Key)
	if err != nil {
		t.Fatalf("GetParent(child1) failed: %v", err)
	}
	if parent != (ouroboroskv.Hash{}) {
		t.Fatalf("child1 should have no parent")
	}
}

func TestDeleteVerifyContentRemoved(t *testing.T) {
	store, cleanup := newDeleteTestStore(t)
	defer cleanup()

	originalContent := []byte("sensitive data that must be deleted")
	data := ouroboroskv.Data{
		Content:        originalContent,
		Meta:           []byte("metadata"),
		RSDataSlices:   3,
		RSParitySlices: 2,
		ContentType:    "text/plain",
	}

	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	// Verify data is readable
	readData, err := store.ReadData(key)
	if err != nil {
		t.Fatalf("ReadData failed: %v", err)
	}
	if !bytes.Equal(readData.Content, originalContent) {
		t.Fatalf("Content mismatch before deletion")
	}

	// Delete data
	if err := store.DeleteData(key); err != nil {
		t.Fatalf("DeleteData failed: %v", err)
	}

	// Attempt to read should fail
	_, err = store.ReadData(key)
	if err == nil {
		t.Fatalf("ReadData should fail after deletion")
	}

	// DataExists should return false
	exists, err := store.DataExists(key)
	if err != nil {
		t.Fatalf("DataExists failed: %v", err)
	}
	if exists {
		t.Fatalf("Data should not exist after deletion")
	}

	// GetDataInfo should fail
	_, err = store.GetDataInfo(key)
	if err == nil {
		t.Fatalf("GetDataInfo should fail after deletion")
	}

	// Validate should fail
	err = store.Validate(key)
	if err == nil {
		t.Fatalf("Validate should fail after deletion")
	}
}

func TestDeleteWithComplexHierarchy(t *testing.T) {
	store, cleanup := newDeleteTestStore(t)
	defer cleanup()

	// Create complex tree:
	//        root
	//       /    \
	//    child1  child2
	//    /   \      \
	//  gc1   gc2    gc3

	rootData := newDeleteTestData("root", ouroboroskv.Hash{})
	rootKey, err := store.WriteData(rootData)
	if err != nil {
		t.Fatalf("WriteData(root) failed: %v", err)
	}

	child1Data := newDeleteTestData("child1", rootKey)
	child1Key, err := store.WriteData(child1Data)
	if err != nil {
		t.Fatalf("WriteData(child1) failed: %v", err)
	}

	child2Data := newDeleteTestData("child2", rootKey)
	child2Key, err := store.WriteData(child2Data)
	if err != nil {
		t.Fatalf("WriteData(child2) failed: %v", err)
	}

	gc1Data := newDeleteTestData("gc1", child1Key)
	gc1Key, err := store.WriteData(gc1Data)
	if err != nil {
		t.Fatalf("WriteData(gc1) failed: %v", err)
	}

	gc2Data := newDeleteTestData("gc2", child1Key)
	gc2Key, err := store.WriteData(gc2Data)
	if err != nil {
		t.Fatalf("WriteData(gc2) failed: %v", err)
	}

	gc3Data := newDeleteTestData("gc3", child2Key)
	gc3Key, err := store.WriteData(gc3Data)
	if err != nil {
		t.Fatalf("WriteData(gc3) failed: %v", err)
	}

	// Delete child1 - should orphan gc1 and gc2
	if err := store.DeleteData(child1Key); err != nil {
		t.Fatalf("DeleteData(child1) failed: %v", err)
	}

	// gc1 and gc2 should be orphaned
	gc1Parent, err := store.GetParent(gc1Key)
	if err != nil {
		t.Fatalf("GetParent(gc1) failed: %v", err)
	}
	if gc1Parent != (ouroboroskv.Hash{}) {
		t.Fatalf("gc1 should be orphaned")
	}

	gc2Parent, err := store.GetParent(gc2Key)
	if err != nil {
		t.Fatalf("GetParent(gc2) failed: %v", err)
	}
	if gc2Parent != (ouroboroskv.Hash{}) {
		t.Fatalf("gc2 should be orphaned")
	}

	// child2 and gc3 should still be connected
	child2Children, err := store.GetChildren(child2Key)
	if err != nil {
		t.Fatalf("GetChildren(child2) failed: %v", err)
	}
	if len(child2Children) != 1 || child2Children[0] != gc3Key {
		t.Fatalf("child2 should still have gc3 as child")
	}

	// Root should only have child2 as child now
	rootChildren, err := store.GetChildren(rootKey)
	if err != nil {
		t.Fatalf("GetChildren(root) failed: %v", err)
	}
	if len(rootChildren) != 1 || rootChildren[0] != child2Key {
		t.Fatalf("root should only have child2 as child")
	}

	// Root descendants should be child2 and gc3
	descendants, err := store.GetDescendants(rootKey)
	if err != nil {
		t.Fatalf("GetDescendants(root) failed: %v", err)
	}
	if len(descendants) != 2 {
		t.Fatalf("Expected 2 descendants, got %d", len(descendants))
	}

	// Roots should now include root, gc1, gc2
	roots, err := store.GetRoots()
	if err != nil {
		t.Fatalf("GetRoots failed: %v", err)
	}

	expectedRoots := []ouroboroskv.Hash{rootKey, gc1Key, gc2Key}
	for _, expectedRoot := range expectedRoots {
		found := false
		for _, root := range roots {
			if root == expectedRoot {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Expected root %x not found", expectedRoot)
		}
	}
}

func TestDeleteRootWithDescendants(t *testing.T) {
	store, cleanup := newDeleteTestStore(t)
	defer cleanup()

	// Create hierarchy
	rootData := newDeleteTestData("root", ouroboroskv.Hash{})
	rootKey, err := store.WriteData(rootData)
	if err != nil {
		t.Fatalf("WriteData(root) failed: %v", err)
	}

	child1Data := newDeleteTestData("child1", rootKey)
	child1Key, err := store.WriteData(child1Data)
	if err != nil {
		t.Fatalf("WriteData(child1) failed: %v", err)
	}

	child2Data := newDeleteTestData("child2", rootKey)
	child2Key, err := store.WriteData(child2Data)
	if err != nil {
		t.Fatalf("WriteData(child2) failed: %v", err)
	}

	// Delete root
	if err := store.DeleteData(rootKey); err != nil {
		t.Fatalf("DeleteData(root) failed: %v", err)
	}

	// Both children should become roots
	roots, err := store.GetRoots()
	if err != nil {
		t.Fatalf("GetRoots failed: %v", err)
	}

	foundChild1 := false
	foundChild2 := false
	for _, root := range roots {
		if root == child1Key {
			foundChild1 = true
		}
		if root == child2Key {
			foundChild2 = true
		}
	}

	if !foundChild1 || !foundChild2 {
		t.Fatalf("Both children should become roots after root deletion")
	}

	// Original root should not exist
	exists, err := store.DataExists(rootKey)
	if err != nil || exists {
		t.Fatalf("Original root should not exist")
	}
}

func TestDeleteValidateAllAfter(t *testing.T) {
	store, cleanup := newDeleteTestStore(t)
	defer cleanup()

	// Create some data
	var keys []ouroboroskv.Hash
	for i := 0; i < 5; i++ {
		data := newDeleteTestData(fmt.Sprintf("data-%d", i), ouroboroskv.Hash{})
		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("WriteData(%d) failed: %v", i, err)
		}
		keys = append(keys, key)
	}

	// Delete middle entry
	if err := store.DeleteData(keys[2]); err != nil {
		t.Fatalf("DeleteData failed: %v", err)
	}

	// ValidateAll should succeed for remaining entries
	results, err := store.ValidateAll()
	if err != nil {
		t.Fatalf("ValidateAll failed: %v", err)
	}

	if len(results) != 4 {
		t.Fatalf("Expected 4 validation results, got %d", len(results))
	}

	for _, res := range results {
		if res.Err != nil {
			t.Fatalf("Validation failed for key %x: %v", res.Key, res.Err)
		}
		if res.Key == keys[2] {
			t.Fatalf("Deleted key should not appear in validation results")
		}
	}
}

func newDeleteTestStore(t *testing.T) (ouroboroskv.Store, func()) {
	t.Helper()
	tempDir := t.TempDir()

	cfg := &ouroboroskv.Config{
		Paths:            []string{tempDir},
		MinimumFreeSpace: 0,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	kv, err := ouroboroskv.Init(crypt.New(), cfg)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	cleanup := func() {
		if err := kv.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	}

	return kv, cleanup
}

func newDeleteTestData(label string, parent ouroboroskv.Hash) ouroboroskv.Data {
	return ouroboroskv.Data{
		Content:        []byte(fmt.Sprintf("content-%s", label)),
		Meta:           []byte(fmt.Sprintf("meta-%s", label)),
		Parent:         parent,
		RSDataSlices:   3,
		RSParitySlices: 2,
		ContentType:    "text/plain",
	}
}
