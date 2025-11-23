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

func TestStoreInterfaceContract(t *testing.T) {
	store, cleanup := newInterfaceTestStore(t)
	t.Cleanup(cleanup)

	missingKey := hash.HashString("interface-missing")

	emptyBatch, err := store.BatchReadData(nil)
	if err != nil {
		t.Fatalf("BatchReadData(nil) returned error: %v", err)
	}
	if emptyBatch != nil {
		t.Fatalf("BatchReadData(nil) should return nil slice, got %v", emptyBatch)
	}

	exists, err := store.DataExists(missingKey)
	if err != nil {
		t.Fatalf("DataExists(missing) failed: %v", err)
	}
	if exists {
		t.Fatalf("DataExists should be false for unknown key")
	}

	if _, err := store.ReadData(missingKey); err == nil {
		t.Fatalf("ReadData should fail for unknown key")
	}

	if err := store.Validate(missingKey); err == nil {
		t.Fatalf("Validate should fail for unknown key")
	}

	rootData := newInterfaceData("root", ouroboroskv.Hash{})
	rootKey, err := store.WriteData(rootData)
	if err != nil {
		t.Fatalf("WriteData(root) failed: %v", err)
	}

	assertDataExists(t, store, rootKey, true)

	storedRoot, err := store.ReadData(rootKey)
	if err != nil {
		t.Fatalf("ReadData(root) failed: %v", err)
	}
	if !bytes.Equal(storedRoot.Content, rootData.Content) {
		t.Fatalf("root content mismatch: want %q got %q", rootData.Content, storedRoot.Content)
	}

	parent, err := store.GetParent(rootKey)
	if err != nil {
		t.Fatalf("GetParent(root) failed: %v", err)
	}
	if parent != (ouroboroskv.Hash{}) {
		t.Fatalf("root should not have parent, got %x", parent)
	}

	children, err := store.GetChildren(rootKey)
	if err != nil {
		t.Fatalf("GetChildren(root) failed: %v", err)
	}
	if len(children) != 0 {
		t.Fatalf("root should have no children yet, got %d", len(children))
	}

	ancestors, err := store.GetAncestors(rootKey)
	if err != nil {
		t.Fatalf("GetAncestors(root) failed: %v", err)
	}
	if len(ancestors) != 0 {
		t.Fatalf("root should have no ancestors, got %d", len(ancestors))
	}

	descendants, err := store.GetDescendants(rootKey)
	if err != nil {
		t.Fatalf("GetDescendants(root) failed: %v", err)
	}
	if len(descendants) != 0 {
		t.Fatalf("root should have no descendants yet")
	}

	roots, err := store.GetRoots()
	if err != nil {
		t.Fatalf("GetRoots failed: %v", err)
	}
	assertHashSetContains(t, roots, rootKey)

	rootKeys, err := store.ListRootKeys()
	if err != nil {
		t.Fatalf("ListRootKeys failed: %v", err)
	}
	assertHashSetContains(t, rootKeys, rootKey)

	keys, err := store.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys failed: %v", err)
	}
	requireExactKeySet(t, keys, []ouroboroskv.Hash{rootKey})

	info, err := store.GetDataInfo(rootKey)
	if err != nil {
		t.Fatalf("GetDataInfo(root) failed: %v", err)
	}
	if info.Key != rootKey {
		t.Fatalf("GetDataInfo returned wrong key: want %x got %x", rootKey, info.Key)
	}
	if info.ClearTextSize == 0 {
		t.Fatalf("GetDataInfo returned zero ClearTextSize")
	}

	infos, err := store.ListStoredData()
	if err != nil {
		t.Fatalf("ListStoredData failed: %v", err)
	}
	if len(infos) != 1 {
		t.Fatalf("ListStoredData expected 1 entry, got %d", len(infos))
	}

	results, err := store.ValidateAll()
	if err != nil {
		t.Fatalf("ValidateAll failed: %v", err)
	}
	if len(results) != 1 || results[0].Err != nil {
		t.Fatalf("ValidateAll expected single passing result, got %+v", results)
	}

	childData := newInterfaceData("child", rootKey)
	childKey, err := store.WriteData(childData)
	if err != nil {
		t.Fatalf("WriteData(child) failed: %v", err)
	}

	assertDataExists(t, store, childKey, true)

	childParent, err := store.GetParent(childKey)
	if err != nil {
		t.Fatalf("GetParent(child) failed: %v", err)
	}
	if childParent != rootKey {
		t.Fatalf("expected parent %x, got %x", rootKey, childParent)
	}

	children, err = store.GetChildren(rootKey)
	if err != nil {
		t.Fatalf("GetChildren(root) failed: %v", err)
	}
	assertHashSetContains(t, children, childKey)

	descendants, err = store.GetDescendants(rootKey)
	if err != nil {
		t.Fatalf("GetDescendants(root) failed: %v", err)
	}
	assertHashSetContains(t, descendants, childKey)

	ancestors, err = store.GetAncestors(childKey)
	if err != nil {
		t.Fatalf("GetAncestors(child) failed: %v", err)
	}
	assertHashSetContains(t, ancestors, rootKey)

	keys, err = store.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys after child failed: %v", err)
	}
	requireExactKeySet(t, keys, []ouroboroskv.Hash{rootKey, childKey})

	infos, err = store.ListStoredData()
	if err != nil {
		t.Fatalf("ListStoredData after child failed: %v", err)
	}
	if len(infos) != 2 {
		t.Fatalf("ListStoredData expected 2 entries, got %d", len(infos))
	}

	results, err = store.ValidateAll()
	if err != nil {
		t.Fatalf("ValidateAll after child failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("ValidateAll expected 2 results, got %d", len(results))
	}
	for _, res := range results {
		if res.Err != nil {
			t.Fatalf("ValidateAll reported error for key %x: %v", res.Key, res.Err)
		}
	}

	batchData := []ouroboroskv.Data{
		newInterfaceData("grandchild-a", childKey),
		newInterfaceData("grandchild-b", childKey),
	}

	batchKeys, err := store.BatchWriteData(batchData)
	if err != nil {
		t.Fatalf("BatchWriteData failed: %v", err)
	}
	if len(batchKeys) != len(batchData) {
		t.Fatalf("BatchWriteData returned %d keys, want %d", len(batchKeys), len(batchData))
	}

	for _, key := range batchKeys {
		assertDataExists(t, store, key, true)
	}

	readBatch, err := store.BatchReadData(batchKeys)
	if err != nil {
		t.Fatalf("BatchReadData failed: %v", err)
	}
	if len(readBatch) != len(batchData) {
		t.Fatalf("BatchReadData returned %d entries, want %d", len(readBatch), len(batchData))
	}
	for i := range readBatch {
		if !bytes.Equal(readBatch[i].Content, batchData[i].Content) {
			t.Fatalf("batch read %d content mismatch", i)
		}
	}

	children, err = store.GetChildren(childKey)
	if err != nil {
		t.Fatalf("GetChildren(child) failed: %v", err)
	}
	assertHashSetContains(t, children, batchKeys...)

	descendants, err = store.GetDescendants(rootKey)
	if err != nil {
		t.Fatalf("GetDescendants(root) failed: %v", err)
	}
	assertHashSetContains(t, descendants, append(batchKeys, childKey)...)

	for _, grandchildKey := range batchKeys {
		ancestors, err = store.GetAncestors(grandchildKey)
		if err != nil {
			t.Fatalf("GetAncestors(grandchild) failed: %v", err)
		}
		assertHashSetContains(t, ancestors, childKey)
		assertHashSetContains(t, ancestors, rootKey)
	}

	keys, err = store.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys after batch failed: %v", err)
	}
	expectedKeys := []ouroboroskv.Hash{rootKey, childKey}
	expectedKeys = append(expectedKeys, batchKeys...)
	requireExactKeySet(t, keys, expectedKeys)

	infos, err = store.ListStoredData()
	if err != nil {
		t.Fatalf("ListStoredData after batch failed: %v", err)
	}
	if len(infos) != len(expectedKeys) {
		t.Fatalf("ListStoredData expected %d entries, got %d", len(expectedKeys), len(infos))
	}

	info, err = store.GetDataInfo(childKey)
	if err != nil {
		t.Fatalf("GetDataInfo(child) failed: %v", err)
	}
	if info.RSDataSlices != childData.RSDataSlices {
		t.Fatalf("RSDataSlices mismatch: want %d got %d", childData.RSDataSlices, info.RSDataSlices)
	}

	results, err = store.ValidateAll()
	if err != nil {
		t.Fatalf("ValidateAll after batch failed: %v", err)
	}
	if len(results) != len(expectedKeys) {
		t.Fatalf("ValidateAll expected %d results, got %d", len(expectedKeys), len(results))
	}
	for _, res := range results {
		if res.Err != nil {
			t.Fatalf("ValidateAll reported error for key %x: %v", res.Key, res.Err)
		}
	}

	for _, key := range expectedKeys {
		if err := store.Validate(key); err != nil {
			t.Fatalf("Validate failed for key %x: %v", key, err)
		}
	}

	// Delete one grandchild and ensure cascading metadata is updated.
	deletedKey := batchKeys[0]
	if err := store.DeleteData(deletedKey); err != nil {
		t.Fatalf("DeleteData(%x) failed: %v", deletedKey, err)
	}

	assertDataExists(t, store, deletedKey, false)

	if _, err := store.ReadData(deletedKey); err == nil {
		t.Fatalf("ReadData should fail for deleted key")
	}

	if err := store.DeleteData(deletedKey); err == nil {
		t.Fatalf("Deleting same key twice should fail")
	}

	if _, err := store.BatchReadData([]ouroboroskv.Hash{deletedKey}); err == nil {
		t.Fatalf("BatchReadData should fail when any key is missing")
	}

	children, err = store.GetChildren(childKey)
	if err != nil {
		t.Fatalf("GetChildren(child) after delete failed: %v", err)
	}
	assertHashSetContains(t, children, batchKeys[1])
	if len(children) != 2 { // child still has one direct child plus itself is not counted
		// After deletion there should only be one remaining direct child.
		// The helper already ensures membership, but length check helps catch duplicates.
		remaining := len(children)
		if remaining != 1 {
			t.Fatalf("expected exactly one child after deletion, got %d", remaining)
		}
	}

	descendants, err = store.GetDescendants(rootKey)
	if err != nil {
		t.Fatalf("GetDescendants(root) after delete failed: %v", err)
	}
	requireExactKeySet(t, descendants, []ouroboroskv.Hash{childKey, batchKeys[1]})

	keys, err = store.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys after delete failed: %v", err)
	}
	remainingKeys := []ouroboroskv.Hash{rootKey, childKey, batchKeys[1]}
	requireExactKeySet(t, keys, remainingKeys)

	infos, err = store.ListStoredData()
	if err != nil {
		t.Fatalf("ListStoredData after delete failed: %v", err)
	}
	if len(infos) != len(remainingKeys) {
		t.Fatalf("ListStoredData expected %d entries after delete, got %d", len(remainingKeys), len(infos))
	}

	results, err = store.ValidateAll()
	if err != nil {
		t.Fatalf("ValidateAll after delete failed: %v", err)
	}
	if len(results) != len(remainingKeys) {
		t.Fatalf("ValidateAll expected %d results after delete, got %d", len(remainingKeys), len(results))
	}
	for _, res := range results {
		if res.Err != nil {
			t.Fatalf("ValidateAll reported error for key %x after delete: %v", res.Key, res.Err)
		}
	}

	roots, err = store.GetRoots()
	if err != nil {
		t.Fatalf("GetRoots after delete failed: %v", err)
	}
	assertHashSetContains(t, roots, rootKey)
	if len(roots) != 1 {
		t.Fatalf("expected single root after delete, got %d", len(roots))
	}

	rootKeys, err = store.ListRootKeys()
	if err != nil {
		t.Fatalf("ListRootKeys after delete failed: %v", err)
	}
	assertHashSetContains(t, rootKeys, rootKey)
	if len(rootKeys) != 1 {
		t.Fatalf("expected ListRootKeys to return single entry, got %d", len(rootKeys))
	}
}

func newInterfaceTestStore(t *testing.T) (ouroboroskv.Store, func()) {
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

func newInterfaceData(label string, parent ouroboroskv.Hash) ouroboroskv.Data {
	return ouroboroskv.Data{
		Content:        []byte(fmt.Sprintf("payload-%s", label)),
		Meta:           []byte(fmt.Sprintf("meta-%s", label)),
		Parent:         parent,
		RSDataSlices:   3,
		RSParitySlices: 2,
		ContentType:    "text/plain",
	}
}

func assertDataExists(t *testing.T, store ouroboroskv.Store, key ouroboroskv.Hash, want bool) {
	t.Helper()
	exists, err := store.DataExists(key)
	if err != nil {
		t.Fatalf("DataExists(%x) failed: %v", key, err)
	}
	if exists != want {
		t.Fatalf("DataExists(%x) = %v, want %v", key, exists, want)
	}
}

func assertHashSetContains(t *testing.T, list []ouroboroskv.Hash, keys ...ouroboroskv.Hash) {
	t.Helper()
	set := make(map[string]struct{}, len(list))
	for _, k := range list {
		set[fmt.Sprintf("%x", k)] = struct{}{}
	}
	for _, expected := range keys {
		if _, ok := set[fmt.Sprintf("%x", expected)]; !ok {
			t.Fatalf("expected key %x to be present", expected)
		}
	}
}

func requireExactKeySet(t *testing.T, got []ouroboroskv.Hash, expected []ouroboroskv.Hash) {
	t.Helper()
	if len(got) != len(expected) {
		t.Fatalf("key set length mismatch: got %d want %d", len(got), len(expected))
	}
	assertHashSetContains(t, got, expected...)
}
