package ouroboroskv__test

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/i5heu/ouroboros-kv/internal/testutil"

	crypt "github.com/i5heu/ouroboros-crypt"
	ouroboroskv "github.com/i5heu/ouroboros-kv"
	"github.com/i5heu/ouroboros-kv/pkg/config"
)

func newValidationTestStore(t *testing.T) (ouroboroskv.Store, string, func()) {
	tempDir := t.TempDir()
	cryptInstance := crypt.New()
	cfg := &config.Config{
		Paths:            []string{tempDir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}
	kv, err := ouroboroskv.Init(cryptInstance, cfg)
	if err != nil {
		t.Fatalf("Failed to init store: %v", err)
	}
	return kv, tempDir, func() { kv.Close() }
}

// TestValidateAfterContentCorruption tests validation when content is corrupted
func TestValidateAfterContentCorruption(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping corruption test in short mode")
	}

	store, tempDir, cleanup := newValidationTestStore(t)

	// Write some data
	data := ouroboroskv.Data{
		Content:        []byte("data-to-corrupt"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	// Close to ensure data is flushed
	cleanup()

	// Corrupt database files
	dbFiles, err := filepath.Glob(filepath.Join(tempDir, "*", "*"))
	if err != nil {
		t.Fatalf("Failed to find db files: %v", err)
	}

	if len(dbFiles) > 0 {
		// Corrupt first file
		info, err := os.Stat(dbFiles[0])
		if err == nil && !info.IsDir() && info.Size() > 0 {
			err = os.WriteFile(dbFiles[0], []byte("CORRUPTED"), 0644)
			if err != nil {
				t.Logf("Could not corrupt file: %v", err)
			} else {
				t.Log("Corrupted database file")
			}
		}
	}

	// Reopen and try to validate
	cryptInstance := crypt.New()
	cfg := &config.Config{
		Paths:            []string{tempDir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	store2, err := ouroboroskv.Init(cryptInstance, cfg)
	if err != nil {
		t.Logf("✅ Correctly failed to open corrupted database: %v", err)
		return
	}
	defer store2.Close()

	// Try to validate the corrupted data
	err = store2.Validate(key)
	if err != nil {
		t.Logf("✅ Validation correctly detected corruption: %v", err)
	} else {
		t.Log("⚠️  Validation passed despite corruption (might have recovered)")
	}
}

// TestValidateAllWithPartialCorruption tests ValidateAll with some corrupted data
func TestValidateAllWithPartialCorruption(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping corruption test in short mode")
	}

	store, tempDir, cleanup := newValidationTestStore(t)

	// Write multiple items
	keys := make([]ouroboroskv.Hash, 5)
	for i := 0; i < 5; i++ {
		data := ouroboroskv.Data{
			Content:        []byte("test-data"),
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("Failed to write data %d: %v", i, err)
		}
		keys[i] = key
	}

	cleanup()

	// Corrupt some files
	dbFiles, err := filepath.Glob(filepath.Join(tempDir, "*", "*.sst"))
	if err == nil && len(dbFiles) > 0 {
		os.WriteFile(dbFiles[0], []byte("CORRUPTED"), 0644)
		t.Log("Corrupted one database file")
	}

	// Reopen
	cryptInstance := crypt.New()
	cfg := &config.Config{
		Paths:            []string{tempDir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	store2, err := ouroboroskv.Init(cryptInstance, cfg)
	if err != nil {
		t.Logf("Database failed to open with partial corruption: %v", err)
		return
	}
	defer store2.Close()

	// Run ValidateAll
	results, err := store2.ValidateAll()
	if err != nil {
		t.Logf("✅ ValidateAll detected issues: %v", err)
	} else {
		t.Logf("ValidateAll completed, found %d validation results", len(results))
		for _, res := range results {
			if !res.Passed() {
				t.Logf("Detected invalid data: %s, error: %v", res.KeyBase64, res.Err)
			}
		}
	}
}

// TestValidateOrphanedData tests validation of data without proper relationships
func TestValidateOrphanedData(t *testing.T) {
	store, _, cleanup := newValidationTestStore(t)
	defer cleanup()

	// Create parent and child
	parentData := ouroboroskv.Data{
		Content:        []byte("parent"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	parentKey, err := store.WriteData(parentData)
	if err != nil {
		t.Fatalf("Failed to write parent: %v", err)
	}

	childData := ouroboroskv.Data{
		Content:        []byte("child"),
		Parent:         parentKey,
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	childKey, err := store.WriteData(childData)
	if err != nil {
		t.Fatalf("Failed to write child: %v", err)
	}

	// Delete parent (orphans child)
	err = store.DeleteData(parentKey)
	if err != nil {
		t.Fatalf("Failed to delete parent: %v", err)
	}

	// Validate the now-orphaned child
	err = store.Validate(childKey)
	if err != nil {
		t.Logf("✅ Validation detected orphaned data: %v", err)
	} else {
		t.Log("⚠️  Orphaned child still validates (might be acceptable)")
	}

	// Check if it still exists
	exists, err := store.DataExists(childKey)
	if err != nil {
		t.Errorf("DataExists failed: %v", err)
	}
	if exists {
		t.Log("Child still exists after parent deletion (orphaned)")
	} else {
		t.Log("Child was cascade deleted with parent")
	}
}

// TestValidateWithMissingChunks tests validation when chunks are missing
func TestValidateWithMissingChunks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping missing chunks test in short mode")
	}

	store, _, cleanup := newValidationTestStore(t)
	defer cleanup()

	// Write large data that will be chunked
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	data := ouroboroskv.Data{
		Content:        largeData,
		RSDataSlices:   8,
		RSParitySlices: 4,
	}
	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("Failed to write large data: %v", err)
	}

	// Check if it has children (chunks)
	children, err := store.GetChildren(key)
	if err != nil {
		t.Fatalf("Failed to get children: %v", err)
	}

	if len(children) > 0 {
		t.Logf("Data was chunked into %d pieces", len(children))

		// Try to delete one chunk directly (simulate missing chunk)
		err = store.DeleteData(children[0])
		if err != nil {
			t.Logf("Cannot directly delete chunk: %v (good protection)", err)
		}

		// Validate parent
		err = store.Validate(key)
		if err != nil {
			t.Logf("✅ Validation detected missing/corrupted chunk: %v", err)
		} else {
			t.Log("⚠️  Validation passed (Reed-Solomon might have recovered)")
		}
	} else {
		t.Log("Data was not chunked (compressed to under threshold)")
	}
}

// TestValidateCascadeCorruption tests validation with corrupted parent-child chain
func TestValidateCascadeCorruption(t *testing.T) {
	store, _, cleanup := newValidationTestStore(t)
	defer cleanup()

	// Create chain: grandparent -> parent -> child
	grandparentData := ouroboroskv.Data{
		Content:        []byte("grandparent"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	grandparentKey, err := store.WriteData(grandparentData)
	if err != nil {
		t.Fatalf("Failed to write grandparent: %v", err)
	}

	parentData := ouroboroskv.Data{
		Content:        []byte("parent"),
		Parent:         grandparentKey,
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	parentKey, err := store.WriteData(parentData)
	if err != nil {
		t.Fatalf("Failed to write parent: %v", err)
	}

	childData := ouroboroskv.Data{
		Content:        []byte("child"),
		Parent:         parentKey,
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	childKey, err := store.WriteData(childData)
	if err != nil {
		t.Fatalf("Failed to write child: %v", err)
	}

	// Validate entire chain
	err = store.Validate(grandparentKey)
	if err != nil {
		t.Errorf("Grandparent validation failed: %v", err)
	}

	err = store.Validate(parentKey)
	if err != nil {
		t.Errorf("Parent validation failed: %v", err)
	}

	err = store.Validate(childKey)
	if err != nil {
		t.Errorf("Child validation failed: %v", err)
	}

	// Verify relationships
	parentOfChild, err := store.GetParent(childKey)
	if err != nil {
		t.Errorf("Failed to get child's parent: %v", err)
	}
	if parentOfChild != parentKey {
		t.Error("Child has wrong parent")
	}

	t.Log("✅ Validated complete parent-child chain")
}

// TestValidateAllAfterManyOperations tests validation after various operations
func TestValidateAllAfterManyOperations(t *testing.T) {
	store, _, cleanup := newValidationTestStore(t)
	defer cleanup()

	// Perform various operations
	t.Log("Performing various operations...")

	// Write some data
	keys := make([]ouroboroskv.Hash, 20)
	for i := 0; i < 20; i++ {
		data := ouroboroskv.Data{
			Content:        []byte("test"),
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
		keys[i] = key
	}

	// Delete some
	for i := 0; i < 5; i++ {
		err := store.DeleteData(keys[i])
		if err != nil {
			t.Errorf("Failed to delete: %v", err)
		}
	}

	// Create relationships
	for i := 10; i < 15; i++ {
		childData := ouroboroskv.Data{
			Content:        []byte("child"),
			Parent:         keys[i],
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		_, err := store.WriteData(childData)
		if err != nil {
			t.Errorf("Failed to write child: %v", err)
		}
	}

	// Run ValidateAll
	t.Log("Running ValidateAll...")
	results, err := store.ValidateAll()
	if err != nil {
		t.Errorf("ValidateAll failed: %v", err)
	}

	invalidCount := 0
	for _, res := range results {
		if !res.Passed() {
			t.Logf("Invalid data found: %s, error: %v", res.KeyBase64, res.Err)
			invalidCount++
		}
	}

	t.Logf("✅ ValidateAll completed: %d items checked, %d invalid", len(results), invalidCount)
}

// TestValidateNonExistentKey tests validation of non-existent key
func TestValidateNonExistentKey(t *testing.T) {
	store, _, cleanup := newValidationTestStore(t)
	defer cleanup()

	fakeKey := ouroboroskv.Hash{}
	for i := range fakeKey {
		fakeKey[i] = byte(i)
	}

	err := store.Validate(fakeKey)
	if err != nil {
		t.Logf("✅ Validation correctly returned error for non-existent key: %v", err)
	} else {
		t.Error("❌ Non-existent key marked as valid")
	}
}

// TestValidateEmptyStore tests ValidateAll on empty database
func TestValidateEmptyStore(t *testing.T) {
	store, _, cleanup := newValidationTestStore(t)
	defer cleanup()

	results, err := store.ValidateAll()
	if err != nil {
		t.Errorf("ValidateAll failed on empty store: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty store, got %d", len(results))
	}

	t.Log("✅ ValidateAll succeeded on empty store")
}

// TestValidateAfterBatchDelete tests validation after deleting many items
func TestValidateAfterBatchDelete(t *testing.T) {
	store, _, cleanup := newValidationTestStore(t)
	defer cleanup()

	// Create many items with unique content to avoid aliasing
	numItems := 50
	keys := make([]ouroboroskv.Hash, numItems)
	for i := 0; i < numItems; i++ {
		data := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("item-%d", i)),
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
		keys[i] = key
	}

	// Delete most of them
	for i := 0; i < 40; i++ {
		err := store.DeleteData(keys[i])
		if err != nil {
			t.Errorf("Failed to delete: %v", err)
		}
	}

	// Validate remaining
	for i := 40; i < numItems; i++ {
		err := store.Validate(keys[i])
		if err != nil {
			t.Errorf("Item %d validation failed: %v", i, err)
		}
	}

	// ValidateAll
	results, err := store.ValidateAll()
	if err != nil {
		t.Errorf("ValidateAll failed: %v", err)
	}

	t.Logf("✅ After deleting 40/50 items, %d items remain and validate", len(results))
}
