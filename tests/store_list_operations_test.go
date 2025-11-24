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

func newListTestStore(t *testing.T) (ouroboroskv.Store, func()) {
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

// TestListKeysEmpty tests listing keys from an empty store
func TestListKeysEmpty(t *testing.T) {
	store, cleanup := newListTestStore(t)
	defer cleanup()

	keys, err := store.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys failed: %v", err)
	}

	if len(keys) != 0 {
		t.Fatalf("Expected 0 keys in empty store, got %d", len(keys))
	}
}

// TestListKeysMultiple tests listing multiple keys
func TestListKeysMultiple(t *testing.T) {
	store, cleanup := newListTestStore(t)
	defer cleanup()

	// Write multiple data items
	var expectedKeys []ouroboroskv.Hash
	for i := 0; i < 5; i++ {
		data := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("content %d", i)),
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("WriteData(%d) failed: %v", i, err)
		}
		expectedKeys = append(expectedKeys, key)
	}

	// List keys
	keys, err := store.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys failed: %v", err)
	}

	if len(keys) != len(expectedKeys) {
		t.Fatalf("Expected %d keys, got %d", len(expectedKeys), len(keys))
	}

	// Verify all expected keys are present
	for _, expectedKey := range expectedKeys {
		found := false
		for _, key := range keys {
			if key == expectedKey {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected key %x not found in list", expectedKey)
		}
	}
}

// TestListKeysWithDeletion tests listing keys after some deletions
func TestListKeysWithDeletion(t *testing.T) {
	store, cleanup := newListTestStore(t)
	defer cleanup()

	// Write 5 items
	var keys []ouroboroskv.Hash
	for i := 0; i < 5; i++ {
		data := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("content %d", i)),
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("WriteData(%d) failed: %v", i, err)
		}
		keys = append(keys, key)
	}

	// Delete 2 items
	if err := store.DeleteData(keys[1]); err != nil {
		t.Fatalf("DeleteData(1) failed: %v", err)
	}
	if err := store.DeleteData(keys[3]); err != nil {
		t.Fatalf("DeleteData(3) failed: %v", err)
	}

	// List remaining keys
	remainingKeys, err := store.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys failed: %v", err)
	}

	if len(remainingKeys) != 3 {
		t.Fatalf("Expected 3 remaining keys, got %d", len(remainingKeys))
	}

	// Verify deleted keys are not in list
	for _, key := range remainingKeys {
		if key == keys[1] || key == keys[3] {
			t.Errorf("Deleted key should not appear in list: %x", key)
		}
	}
}

// TestListRootKeysEmpty tests listing root keys from empty store
func TestListRootKeysEmpty(t *testing.T) {
	store, cleanup := newListTestStore(t)
	defer cleanup()

	roots, err := store.ListRootKeys()
	if err != nil {
		t.Fatalf("ListRootKeys failed: %v", err)
	}

	if len(roots) != 0 {
		t.Fatalf("Expected 0 roots in empty store, got %d", len(roots))
	}
}

// TestListRootKeysOnlyRoots tests listing when all items are roots
func TestListRootKeysOnlyRoots(t *testing.T) {
	store, cleanup := newListTestStore(t)
	defer cleanup()

	// Write 3 root items (no parent)
	var rootKeys []ouroboroskv.Hash
	for i := 0; i < 3; i++ {
		data := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("root %d", i)),
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("WriteData(root %d) failed: %v", i, err)
		}
		rootKeys = append(rootKeys, key)
	}

	// List roots
	roots, err := store.ListRootKeys()
	if err != nil {
		t.Fatalf("ListRootKeys failed: %v", err)
	}

	if len(roots) != 3 {
		t.Fatalf("Expected 3 roots, got %d", len(roots))
	}

	// Verify all roots are present
	for _, expectedRoot := range rootKeys {
		found := false
		for _, root := range roots {
			if root == expectedRoot {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected root %x not found in list", expectedRoot)
		}
	}
}

// TestListRootKeysWithHierarchy tests listing roots when there are parent-child relationships
func TestListRootKeysWithHierarchy(t *testing.T) {
	store, cleanup := newListTestStore(t)
	defer cleanup()

	// Create 2 roots
	var rootKeys []ouroboroskv.Hash
	for i := 0; i < 2; i++ {
		data := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("root %d", i)),
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("WriteData(root %d) failed: %v", i, err)
		}
		rootKeys = append(rootKeys, key)
	}

	// Create children for each root
	for i, rootKey := range rootKeys {
		for j := 0; j < 2; j++ {
			childData := ouroboroskv.Data{
				Content:        []byte(fmt.Sprintf("child %d-%d", i, j)),
				Parent:         rootKey,
				RSDataSlices:   3,
				RSParitySlices: 2,
			}
			_, err := store.WriteData(childData)
			if err != nil {
				t.Fatalf("WriteData(child %d-%d) failed: %v", i, j, err)
			}
		}
	}

	// List roots - should only return the 2 original roots
	roots, err := store.ListRootKeys()
	if err != nil {
		t.Fatalf("ListRootKeys failed: %v", err)
	}

	if len(roots) != 2 {
		t.Fatalf("Expected 2 roots, got %d", len(roots))
	}

	// Verify only roots are returned
	for _, root := range roots {
		isRoot := false
		for _, expectedRoot := range rootKeys {
			if root == expectedRoot {
				isRoot = true
				break
			}
		}
		if !isRoot {
			t.Errorf("Non-root key found in root list: %x", root)
		}
	}
}

// TestListRootKeysAfterParentDeletion tests that orphaned children become roots
func TestListRootKeysAfterParentDeletion(t *testing.T) {
	store, cleanup := newListTestStore(t)
	defer cleanup()

	// Create root
	rootData := ouroboroskv.Data{
		Content:        []byte("root"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	rootKey, err := store.WriteData(rootData)
	if err != nil {
		t.Fatalf("WriteData(root) failed: %v", err)
	}

	// Create children
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

	// Before deletion, should have 1 root
	rootsBefore, err := store.ListRootKeys()
	if err != nil {
		t.Fatalf("ListRootKeys before deletion failed: %v", err)
	}
	if len(rootsBefore) != 1 {
		t.Fatalf("Expected 1 root before deletion, got %d", len(rootsBefore))
	}

	// Delete root
	if err := store.DeleteData(rootKey); err != nil {
		t.Fatalf("DeleteData(root) failed: %v", err)
	}

	// After deletion, orphaned children should be new roots
	rootsAfter, err := store.ListRootKeys()
	if err != nil {
		t.Fatalf("ListRootKeys after deletion failed: %v", err)
	}

	if len(rootsAfter) != 2 {
		t.Fatalf("Expected 2 roots after deletion (orphaned children), got %d", len(rootsAfter))
	}

	// Verify children are in root list
	for _, childKey := range childKeys {
		found := false
		for _, root := range rootsAfter {
			if root == childKey {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Orphaned child %x should be a root", childKey)
		}
	}
}

// TestListStoredDataEmpty tests listing stored data info from empty store
func TestListStoredDataEmpty(t *testing.T) {
	store, cleanup := newListTestStore(t)
	defer cleanup()

	dataInfos, err := store.ListStoredData()
	if err != nil {
		t.Fatalf("ListStoredData failed: %v", err)
	}

	if len(dataInfos) != 0 {
		t.Fatalf("Expected 0 data infos in empty store, got %d", len(dataInfos))
	}
}

// TestListStoredDataMultiple tests listing stored data info with multiple items
func TestListStoredDataMultiple(t *testing.T) {
	store, cleanup := newListTestStore(t)
	defer cleanup()

	// Write multiple items with varying properties
	items := []struct {
		content     string
		contentType string
		meta        string
	}{
		{"content 1", "text/plain", "meta 1"},
		{"content 2", "application/json", "meta 2"},
		{"content 3", "", "meta 3"},
	}

	var expectedKeys []ouroboroskv.Hash
	for i, item := range items {
		data := ouroboroskv.Data{
			Content:        []byte(item.content),
			ContentType:    item.contentType,
			Meta:           []byte(item.meta),
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("WriteData(%d) failed: %v", i, err)
		}
		expectedKeys = append(expectedKeys, key)
	}

	// List stored data
	dataInfos, err := store.ListStoredData()
	if err != nil {
		t.Fatalf("ListStoredData failed: %v", err)
	}

	if len(dataInfos) != len(items) {
		t.Fatalf("Expected %d data infos, got %d", len(items), len(dataInfos))
	}

	// Verify all keys are present
	for _, expectedKey := range expectedKeys {
		found := false
		for _, info := range dataInfos {
			if info.Key == expectedKey {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected key %x not found in data info list", expectedKey)
		}
	}
}

// TestListStoredDataWithHierarchy tests listing data with parent-child relationships
func TestListStoredDataWithHierarchy(t *testing.T) {
	store, cleanup := newListTestStore(t)
	defer cleanup()

	// Create root
	rootData := ouroboroskv.Data{
		Content:        []byte("root"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	rootKey, err := store.WriteData(rootData)
	if err != nil {
		t.Fatalf("WriteData(root) failed: %v", err)
	}

	// Create children
	for i := 0; i < 2; i++ {
		childData := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("child %d", i)),
			Parent:         rootKey,
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		_, err := store.WriteData(childData)
		if err != nil {
			t.Fatalf("WriteData(child %d) failed: %v", i, err)
		}
	}

	// List stored data
	dataInfos, err := store.ListStoredData()
	if err != nil {
		t.Fatalf("ListStoredData failed: %v", err)
	}

	if len(dataInfos) != 3 {
		t.Fatalf("Expected 3 data infos (1 root + 2 children), got %d", len(dataInfos))
	}

	// Find root info
	var rootInfo *ouroboroskv.DataInfo
	for i := range dataInfos {
		if dataInfos[i].Key == rootKey {
			rootInfo = &dataInfos[i]
			break
		}
	}

	if rootInfo == nil {
		t.Fatalf("Root key not found in data info list")
	}
}

// TestListKeysWithAliases tests listing when aliased data exists
func TestListKeysWithAliases(t *testing.T) {
	store, cleanup := newListTestStore(t)
	defer cleanup()

	// Write same content twice (creates aliases)
	data := ouroboroskv.Data{
		Content:        []byte("shared content"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}

	key1, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData(1) failed: %v", err)
	}

	key2, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData(2) failed: %v", err)
	}

	// Both keys should resolve to the same content hash (deduplication)
	// So we expect only one key in the list (the canonical content hash)
	keys, err := store.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys failed: %v", err)
	}

	if len(keys) != 1 {
		t.Fatalf("Expected 1 key (deduplicated), got %d", len(keys))
	}

	// Verify the listed key matches one of the returned keys
	listedKey := keys[0]
	if listedKey != key1 && listedKey != key2 {
		t.Errorf("Listed key %x doesn't match key1 or key2", listedKey)
	}
}

// TestListKeysAfterAliasDelete tests listing after deleting one alias
func TestListKeysAfterAliasDelete(t *testing.T) {
	store, cleanup := newListTestStore(t)
	defer cleanup()

	// Write same content twice
	data := ouroboroskv.Data{
		Content:        []byte("shared content"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}

	key1, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData(1) failed: %v", err)
	}

	key2, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData(2) failed: %v", err)
	}

	// Delete one alias
	if err := store.DeleteData(key1); err != nil {
		t.Fatalf("DeleteData(key1) failed: %v", err)
	}

	// List keys - since they were aliased, both should now be gone or just the canonical one should remain
	// The actual behavior depends on refcount implementation
	keys, err := store.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys failed: %v", err)
	}

	// After deleting one alias, the content may still exist if refcount > 0
	// We just verify the operation succeeded
	t.Logf("After deleting key1=%x, %d keys remain (key2=%x)", key1, len(keys), key2)
}

// TestListRootKeysWithContentType tests listing roots with content types
func TestListRootKeysWithContentType(t *testing.T) {
	store, cleanup := newListTestStore(t)
	defer cleanup()

	// Create roots with different content types
	contentTypes := []string{"text/plain", "application/json", "image/png"}
	for i, ct := range contentTypes {
		data := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("content %d", i)),
			ContentType:    ct,
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		_, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("WriteData(%d) failed: %v", i, err)
		}
	}

	// List roots
	roots, err := store.ListRootKeys()
	if err != nil {
		t.Fatalf("ListRootKeys failed: %v", err)
	}

	if len(roots) != 3 {
		t.Fatalf("Expected 3 roots, got %d", len(roots))
	}
}

// TestListKeysLarge tests listing with many items
func TestListKeysLarge(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large list test in short mode")
	}

	store, cleanup := newListTestStore(t)
	defer cleanup()

	// Write 100 items
	count := 100
	for i := 0; i < count; i++ {
		data := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("content %d", i)),
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		_, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("WriteData(%d) failed: %v", i, err)
		}
	}

	// List all keys
	keys, err := store.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys failed: %v", err)
	}

	if len(keys) != count {
		t.Fatalf("Expected %d keys, got %d", count, len(keys))
	}
}
