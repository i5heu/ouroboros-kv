package ouroboroskv__test

import (
	"fmt"
	"io"
	"log/slog"
	"testing"

	_ "github.com/i5heu/ouroboros-kv/internal/testutil"

	crypt "github.com/i5heu/ouroboros-crypt"
	ouroboroskv "github.com/i5heu/ouroboros-kv"
)

func newDeleteAdvancedTestStore(t *testing.T) (ouroboroskv.Store, func()) {
	t.Helper()
	cryptInstance := crypt.New()
	tmpDir := t.TempDir()

	cfg := &ouroboroskv.Config{
		Paths:            []string{tmpDir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	kv, err := ouroboroskv.Init(cryptInstance, cfg)
	if err != nil {
		t.Fatalf("Failed to init store: %v", err)
	}

	return kv, func() {
		if err := kv.Close(); err != nil {
			t.Errorf("Failed to close store: %v", err)
		}
	}
}

// TestDeleteWithMetadataChunks tests deletion of data with metadata that has chunks
func TestDeleteWithMetadataChunks(t *testing.T) {
	store, cleanup := newDeleteAdvancedTestStore(t)
	defer cleanup()

	// Create data with large metadata that will be chunked
	largeMetadata := make([]byte, 2*1024*1024) // 2MB metadata
	for i := range largeMetadata {
		largeMetadata[i] = byte(i % 256)
	}

	data := ouroboroskv.Data{
		Content:        []byte("content with large metadata"),
		Meta:           largeMetadata,
		RSDataSlices:   3,
		RSParitySlices: 2,
	}

	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	// Verify data exists
	exists, err := store.DataExists(key)
	if err != nil {
		t.Fatalf("DataExists failed: %v", err)
	}
	if !exists {
		t.Fatalf("Data should exist before deletion")
	}

	// Delete the data
	if err := store.DeleteData(key); err != nil {
		t.Fatalf("DeleteData failed: %v", err)
	}

	// Verify deletion
	exists, err = store.DataExists(key)
	if err != nil {
		t.Fatalf("DataExists after deletion failed: %v", err)
	}
	if exists {
		t.Fatalf("Data should not exist after deletion")
	}
}

// TestDeleteWithContentType tests deletion of data with content type
func TestDeleteWithContentType(t *testing.T) {
	store, cleanup := newDeleteAdvancedTestStore(t)
	defer cleanup()

	data := ouroboroskv.Data{
		Content:        []byte("test content with type"),
		ContentType:    "application/json",
		RSDataSlices:   3,
		RSParitySlices: 2,
	}

	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	// Verify content type
	readData, err := store.ReadData(key)
	if err != nil {
		t.Fatalf("ReadData failed: %v", err)
	}
	if readData.ContentType != "application/json" {
		t.Fatalf("Expected content type 'application/json', got %s", readData.ContentType)
	}

	// Delete the data
	if err := store.DeleteData(key); err != nil {
		t.Fatalf("DeleteData failed: %v", err)
	}

	// Verify deletion
	exists, err := store.DataExists(key)
	if err != nil {
		t.Fatalf("DataExists after deletion failed: %v", err)
	}
	if exists {
		t.Fatalf("Data should not exist after deletion")
	}
}

// TestDeleteParentWithMultipleChildren tests relationship cleanup when deleting parent
func TestDeleteParentWithMultipleChildren(t *testing.T) {
	store, cleanup := newDeleteAdvancedTestStore(t)
	defer cleanup()

	// Create parent
	parentData := ouroboroskv.Data{
		Content:        []byte("parent"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	parentKey, err := store.WriteData(parentData)
	if err != nil {
		t.Fatalf("WriteData(parent) failed: %v", err)
	}

	// Create 3 children
	var childKeys []ouroboroskv.Hash
	for i := 0; i < 3; i++ {
		childData := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("child %d", i)),
			Parent:         parentKey,
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		childKey, err := store.WriteData(childData)
		if err != nil {
			t.Fatalf("WriteData(child %d) failed: %v", i, err)
		}
		childKeys = append(childKeys, childKey)
	}

	// Verify all children have parent
	for i, childKey := range childKeys {
		parent, err := store.GetParent(childKey)
		if err != nil {
			t.Fatalf("GetParent(child %d) failed: %v", i, err)
		}
		if parent != parentKey {
			t.Fatalf("Child %d parent mismatch", i)
		}
	}

	// Delete parent
	if err := store.DeleteData(parentKey); err != nil {
		t.Fatalf("DeleteData(parent) failed: %v", err)
	}

	// Verify all children are now orphans (parent is empty hash)
	for i, childKey := range childKeys {
		parent, err := store.GetParent(childKey)
		if err != nil {
			t.Fatalf("GetParent(child %d) after parent deletion failed: %v", i, err)
		}
		if parent != (ouroboroskv.Hash{}) {
			t.Fatalf("Child %d should have empty parent after parent deletion", i)
		}
	}

	// All children should still exist
	for i, childKey := range childKeys {
		exists, err := store.DataExists(childKey)
		if err != nil {
			t.Fatalf("DataExists(child %d) failed: %v", i, err)
		}
		if !exists {
			t.Fatalf("Child %d should still exist after parent deletion", i)
		}
	}
}

// TestDeleteAliasedDataWithRefCount tests deletion when multiple aliases exist
func TestDeleteAliasedDataWithRefCount(t *testing.T) {
	store, cleanup := newDeleteAdvancedTestStore(t)
	defer cleanup()

	// Write data once
	data := ouroboroskv.Data{
		Content:        []byte("shared content"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	key1, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData(1) failed: %v", err)
	}

	// Write same data again (should create alias)
	key2, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData(2) failed: %v", err)
	}

	// Keys should be different but both should work
	if key1 == key2 {
		t.Fatalf("Expected different keys for aliased data")
	}

	// Both should be readable
	_, err = store.ReadData(key1)
	if err != nil {
		t.Fatalf("ReadData(key1) failed: %v", err)
	}
	_, err = store.ReadData(key2)
	if err != nil {
		t.Fatalf("ReadData(key2) failed: %v", err)
	}

	// Delete first key - should only decrement ref count
	if err := store.DeleteData(key1); err != nil {
		t.Fatalf("DeleteData(key1) failed: %v", err)
	}

	// First key should not exist
	exists, err := store.DataExists(key1)
	if err != nil {
		t.Fatalf("DataExists(key1) failed: %v", err)
	}
	if exists {
		t.Fatalf("key1 should not exist after deletion")
	}

	// Second key should still work (data not deleted, just alias removed)
	_, err = store.ReadData(key2)
	if err != nil {
		t.Fatalf("ReadData(key2) after key1 deletion failed: %v", err)
	}

	// Delete second key - should now delete actual data
	if err := store.DeleteData(key2); err != nil {
		t.Fatalf("DeleteData(key2) failed: %v", err)
	}

	// Second key should not exist
	exists, err = store.DataExists(key2)
	if err != nil {
		t.Fatalf("DataExists(key2) failed: %v", err)
	}
	if exists {
		t.Fatalf("key2 should not exist after deletion")
	}
}

// TestDeleteChainedHierarchy tests deletion in a deep parent-child chain
func TestDeleteChainedHierarchy(t *testing.T) {
	store, cleanup := newDeleteAdvancedTestStore(t)
	defer cleanup()

	// Create chain: root -> level1 -> level2 -> level3
	var keys []ouroboroskv.Hash
	var parent ouroboroskv.Hash

	for i := 0; i < 4; i++ {
		data := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("level %d", i)),
			Parent:         parent,
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("WriteData(level %d) failed: %v", i, err)
		}
		keys = append(keys, key)
		parent = key
	}

	// Delete middle node (level1)
	middleKey := keys[1]
	if err := store.DeleteData(middleKey); err != nil {
		t.Fatalf("DeleteData(middle) failed: %v", err)
	}

	// level2 should now be orphaned (no parent)
	level2Key := keys[2]
	level2Parent, err := store.GetParent(level2Key)
	if err != nil {
		t.Fatalf("GetParent(level2) failed: %v", err)
	}
	if level2Parent != (ouroboroskv.Hash{}) {
		t.Fatalf("level2 should have empty parent after middle deletion")
	}

	// level2 and level3 should still exist
	for i := 2; i < 4; i++ {
		exists, err := store.DataExists(keys[i])
		if err != nil {
			t.Fatalf("DataExists(level %d) failed: %v", i, err)
		}
		if !exists {
			t.Fatalf("level %d should still exist", i)
		}
	}
}

// TestDeleteWithEmptyData tests deletion of data with no content
func TestDeleteWithEmptyData(t *testing.T) {
	store, cleanup := newDeleteAdvancedTestStore(t)
	defer cleanup()

	data := ouroboroskv.Data{
		Content:        []byte{},
		Meta:           []byte("metadata only"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}

	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	if err := store.DeleteData(key); err != nil {
		t.Fatalf("DeleteData failed: %v", err)
	}

	exists, err := store.DataExists(key)
	if err != nil {
		t.Fatalf("DataExists failed: %v", err)
	}
	if exists {
		t.Fatalf("Data should not exist after deletion")
	}
}

// TestDeleteMissingKey tests attempting to delete a key that doesn't exist
func TestDeleteMissingKey(t *testing.T) {
	store, cleanup := newDeleteAdvancedTestStore(t)
	defer cleanup()

	// Create a fake key that doesn't exist
	fakeKey := ouroboroskv.Hash{}
	for i := range fakeKey {
		fakeKey[i] = byte(i)
	}

	// Attempt to delete should fail gracefully
	err := store.DeleteData(fakeKey)
	if err == nil {
		t.Fatalf("Expected error when deleting non-existent key")
	}
}

// TestDeleteWithLargeContent tests deletion of large content with multiple chunks
func TestDeleteWithLargeContent(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large content test in short mode")
	}

	store, cleanup := newDeleteAdvancedTestStore(t)
	defer cleanup()

	// Create 5MB of content that will be split into multiple chunks
	largeContent := make([]byte, 5*1024*1024)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}

	data := ouroboroskv.Data{
		Content:        largeContent,
		RSDataSlices:   8,
		RSParitySlices: 4,
	}

	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	// Verify data exists and can be read
	readData, err := store.ReadData(key)
	if err != nil {
		t.Fatalf("ReadData failed: %v", err)
	}
	if len(readData.Content) == 0 {
		t.Fatalf("Expected non-empty content")
	}

	// Delete the large data
	if err := store.DeleteData(key); err != nil {
		t.Fatalf("DeleteData failed: %v", err)
	}

	// Verify deletion
	exists, err := store.DataExists(key)
	if err != nil {
		t.Fatalf("DataExists failed: %v", err)
	}
	if exists {
		t.Fatalf("Data should not exist after deletion")
	}
}

// TestDeleteRootInHierarchy tests deletion of root node in a hierarchy
func TestDeleteRootInHierarchy(t *testing.T) {
	store, cleanup := newDeleteAdvancedTestStore(t)
	defer cleanup()

	// Create root with children
	rootData := ouroboroskv.Data{
		Content:        []byte("root"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	rootKey, err := store.WriteData(rootData)
	if err != nil {
		t.Fatalf("WriteData(root) failed: %v", err)
	}

	// Create 2 children
	var childKeys []ouroboroskv.Hash
	for i := 0; i < 2; i++ {
		childData := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("child %d", i)),
			Parent:         rootKey,
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		childKey, err := store.WriteData(childData)
		if err != nil {
			t.Fatalf("WriteData(child %d) failed: %v", i, err)
		}
		childKeys = append(childKeys, childKey)
	}

	// Get roots before deletion
	rootsBefore, err := store.GetRoots()
	if err != nil {
		t.Fatalf("GetRoots before deletion failed: %v", err)
	}

	// Delete root
	if err := store.DeleteData(rootKey); err != nil {
		t.Fatalf("DeleteData(root) failed: %v", err)
	}

	// Children should become new roots
	rootsAfter, err := store.GetRoots()
	if err != nil {
		t.Fatalf("GetRoots after deletion failed: %v", err)
	}

	// Should have 2 new roots (the orphaned children)
	newRootCount := len(rootsAfter) - len(rootsBefore) + 1 // -1 for deleted root, +2 for new roots
	if newRootCount < 2 {
		t.Fatalf("Expected at least 2 new roots after root deletion, got %d new roots", newRootCount)
	}

	// Verify children exist and have no parent
	for i, childKey := range childKeys {
		exists, err := store.DataExists(childKey)
		if err != nil {
			t.Fatalf("DataExists(child %d) failed: %v", i, err)
		}
		if !exists {
			t.Fatalf("Child %d should exist after root deletion", i)
		}

		parent, err := store.GetParent(childKey)
		if err != nil {
			t.Fatalf("GetParent(child %d) failed: %v", i, err)
		}
		if parent != (ouroboroskv.Hash{}) {
			t.Fatalf("Child %d should have empty parent after root deletion", i)
		}
	}
}
