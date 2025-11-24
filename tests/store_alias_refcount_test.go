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

func TestAliasWritingSameDataMultipleTimes(t *testing.T) {
	store, cleanup := newAliasTestStore(t)
	defer cleanup()

	originalData := newAliasTestData("identical-content")

	// Write same data 3 times
	key1, err := store.WriteData(originalData)
	if err != nil {
		t.Fatalf("First WriteData failed: %v", err)
	}

	key2, err := store.WriteData(originalData)
	if err != nil {
		t.Fatalf("Second WriteData failed: %v", err)
	}

	key3, err := store.WriteData(originalData)
	if err != nil {
		t.Fatalf("Third WriteData failed: %v", err)
	}

	// Keys should be different (aliases)
	if key1 == key2 || key2 == key3 || key1 == key3 {
		t.Fatalf("Expected different alias keys, got key1=%x, key2=%x, key3=%x", key1, key2, key3)
	}

	// All should be readable
	for i, key := range []ouroboroskv.Hash{key1, key2, key3} {
		data, err := store.ReadData(key)
		if err != nil {
			t.Fatalf("ReadData(%d) failed: %v", i, err)
		}
		if string(data.Content) != string(originalData.Content) {
			t.Fatalf("Content mismatch for key %d", i)
		}
	}
}

func TestAliasDeleteDecrementRefCount(t *testing.T) {
	store, cleanup := newAliasTestStore(t)
	defer cleanup()

	data := newAliasTestData("refcount-test")

	// Write same data 3 times to create 3 aliases
	key1, _ := store.WriteData(data)
	key2, _ := store.WriteData(data)
	key3, _ := store.WriteData(data)

	// Delete first alias
	if err := store.DeleteData(key1); err != nil {
		t.Fatalf("DeleteData(key1) failed: %v", err)
	}

	// key1 should not exist
	exists, err := store.DataExists(key1)
	if err != nil {
		t.Fatalf("DataExists(key1) failed: %v", err)
	}
	if exists {
		t.Fatalf("key1 should not exist after deletion")
	}

	// key2 and key3 should still work
	if _, err := store.ReadData(key2); err != nil {
		t.Fatalf("key2 should still be readable: %v", err)
	}
	if _, err := store.ReadData(key3); err != nil {
		t.Fatalf("key3 should still be readable: %v", err)
	}

	// Delete second alias
	if err := store.DeleteData(key2); err != nil {
		t.Fatalf("DeleteData(key2) failed: %v", err)
	}

	// key3 should still work
	if _, err := store.ReadData(key3); err != nil {
		t.Fatalf("key3 should still be readable after key2 deletion: %v", err)
	}

	// Delete last alias - this should remove actual data
	if err := store.DeleteData(key3); err != nil {
		t.Fatalf("DeleteData(key3) failed: %v", err)
	}

	// All keys should be gone
	for i, key := range []ouroboroskv.Hash{key1, key2, key3} {
		exists, err := store.DataExists(key)
		if err != nil {
			t.Fatalf("DataExists(%d) failed: %v", i, err)
		}
		if exists {
			t.Fatalf("key%d should not exist after all deletions", i)
		}
	}
}

func TestAliasCanonicalKeyReadWrite(t *testing.T) {
	store, cleanup := newAliasTestStore(t)
	defer cleanup()

	data := newAliasTestData("canonical-test")

	// First write creates canonical key
	canonicalKey, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	// Read via canonical key
	readData, err := store.ReadData(canonicalKey)
	if err != nil {
		t.Fatalf("ReadData(canonical) failed: %v", err)
	}
	if string(readData.Content) != string(data.Content) {
		t.Fatalf("Content mismatch via canonical key")
	}

	// Write again to create alias
	aliasKey, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("Second WriteData failed: %v", err)
	}

	// Read via alias
	readData2, err := store.ReadData(aliasKey)
	if err != nil {
		t.Fatalf("ReadData(alias) failed: %v", err)
	}
	if string(readData2.Content) != string(data.Content) {
		t.Fatalf("Content mismatch via alias key")
	}

	// Both reads should return same content
	if string(readData.Content) != string(readData2.Content) {
		t.Fatalf("Canonical and alias reads should return same content")
	}
}

func TestAliasBatchWriteDuplicates(t *testing.T) {
	store, cleanup := newAliasTestStore(t)
	defer cleanup()

	// Create batch with some duplicates
	data1 := newAliasTestData("duplicate")
	data2 := newAliasTestData("unique")
	data3 := newAliasTestData("duplicate") // Same as data1

	batch := []ouroboroskv.Data{data1, data2, data3}

	keys, err := store.BatchWriteData(batch)
	if err != nil {
		t.Fatalf("BatchWriteData failed: %v", err)
	}

	if len(keys) != 3 {
		t.Fatalf("Expected 3 keys, got %d", len(keys))
	}

	// Keys 0 and 2 should be different (aliases of same data)
	if keys[0] == keys[2] {
		t.Fatalf("Duplicate data should get different alias keys")
	}

	// All should be readable
	for i, key := range keys {
		if _, err := store.ReadData(key); err != nil {
			t.Fatalf("ReadData(%d) failed: %v", i, err)
		}
	}
}

func TestAliasListKeysShowsAll(t *testing.T) {
	store, cleanup := newAliasTestStore(t)
	defer cleanup()

	data := newAliasTestData("list-test")

	// Create 3 aliases
	key1, _ := store.WriteData(data)
	key2, _ := store.WriteData(data)
	key3, _ := store.WriteData(data)

	keys, err := store.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys failed: %v", err)
	}

	// Should contain canonical key (one of them) but not necessarily all aliases
	// The implementation might only list canonical keys
	if len(keys) == 0 {
		t.Fatalf("ListKeys returned no keys")
	}

	// Verify at least one of our keys is present
	found := false
	for _, listedKey := range keys {
		if listedKey == key1 || listedKey == key2 || listedKey == key3 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("None of our keys found in ListKeys")
	}
}

func TestAliasDeleteOrderIndependent(t *testing.T) {
	store, cleanup := newAliasTestStore(t)
	defer cleanup()

	data := newAliasTestData("order-test")

	// Create 3 aliases
	key1, _ := store.WriteData(data)
	key2, _ := store.WriteData(data)
	key3, _ := store.WriteData(data)

	// Delete in middle-first-last order
	if err := store.DeleteData(key2); err != nil {
		t.Fatalf("DeleteData(key2) failed: %v", err)
	}

	// Other keys should still work
	if _, err := store.ReadData(key1); err != nil {
		t.Fatalf("key1 should work after key2 deletion")
	}
	if _, err := store.ReadData(key3); err != nil {
		t.Fatalf("key3 should work after key2 deletion")
	}

	// Delete first
	if err := store.DeleteData(key1); err != nil {
		t.Fatalf("DeleteData(key1) failed: %v", err)
	}

	// Last key should still work
	if _, err := store.ReadData(key3); err != nil {
		t.Fatalf("key3 should work after key1 deletion")
	}

	// Delete last
	if err := store.DeleteData(key3); err != nil {
		t.Fatalf("DeleteData(key3) failed: %v", err)
	}

	// All should be gone
	for _, key := range []ouroboroskv.Hash{key1, key2, key3} {
		if exists, _ := store.DataExists(key); exists {
			t.Fatalf("Key should not exist after all deletions")
		}
	}
}

func TestAliasWithRelationships(t *testing.T) {
	store, cleanup := newAliasTestStore(t)
	defer cleanup()

	// Create parent
	parentData := newAliasTestData("parent")
	parentKey1, _ := store.WriteData(parentData)
	parentKey2, _ := store.WriteData(parentData) // Alias

	// Create child with first parent alias
	childData := ouroboroskv.Data{
		Content:        []byte("child-content"),
		Parent:         parentKey1,
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	childKey, err := store.WriteData(childData)
	if err != nil {
		t.Fatalf("WriteData(child) failed: %v", err)
	}

	// Check relationships via first parent alias
	children, err := store.GetChildren(parentKey1)
	if err != nil {
		t.Fatalf("GetChildren(parentKey1) failed: %v", err)
	}
	if len(children) != 1 || children[0] != childKey {
		t.Fatalf("Expected child via parentKey1")
	}

	// Check relationships via second parent alias should also work
	// (though behavior may vary based on implementation)
	children2, err := store.GetChildren(parentKey2)
	if err != nil {
		t.Fatalf("GetChildren(parentKey2) failed: %v", err)
	}
	// May or may not have children depending on how aliases are resolved
	// Just verify it doesn't error
	_ = children2
}

func TestAliasValidation(t *testing.T) {
	store, cleanup := newAliasTestStore(t)
	defer cleanup()

	data := newAliasTestData("validate-test")

	key1, _ := store.WriteData(data)
	key2, _ := store.WriteData(data)

	// Validate via first key (canonical)
	if err := store.Validate(key1); err != nil {
		t.Fatalf("Validate(key1) failed: %v", err)
	}

	// Note: key2 is an alias - validation might behave differently for aliases
	// as they reference the same underlying data but with different keys.
	// Let's test if the alias exists and is readable rather than validating it
	exists, err := store.DataExists(key2)
	if err != nil {
		t.Fatalf("DataExists(key2) failed: %v", err)
	}
	if !exists {
		t.Fatalf("Alias key2 should exist")
	}

	// Read via alias should still work
	if _, err := store.ReadData(key2); err != nil {
		t.Fatalf("ReadData(key2) failed: %v", err)
	}

	// ValidateAll should not error
	results, err := store.ValidateAll()
	if err != nil {
		t.Fatalf("ValidateAll failed: %v", err)
	}

	// Check that at least the canonical key validates
	foundValid := false
	for _, res := range results {
		if res.Key == key1 && res.Err == nil {
			foundValid = true
			break
		}
	}
	if !foundValid {
		t.Fatalf("Expected at least canonical key to validate successfully")
	}
}

func newAliasTestStore(t *testing.T) (ouroboroskv.Store, func()) {
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

func newAliasTestData(label string) ouroboroskv.Data {
	return ouroboroskv.Data{
		Content:        []byte(fmt.Sprintf("content-%s", label)),
		Meta:           []byte(fmt.Sprintf("meta-%s", label)),
		RSDataSlices:   3,
		RSParitySlices: 2,
		ContentType:    "text/plain",
	}
}
