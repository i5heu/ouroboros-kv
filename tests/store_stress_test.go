package ouroboroskv__test

import (
	"fmt"
	"log/slog"
	"os"
	"testing"

	crypt "github.com/i5heu/ouroboros-crypt"
	ouroboroskv "github.com/i5heu/ouroboros-kv"
	"github.com/i5heu/ouroboros-kv/pkg/config"
)

func newStressTestStore(t *testing.T) (ouroboroskv.Store, func()) {
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
	return kv, func() { kv.Close() }
}

// TestDeepHierarchy tests a very deep parent-child chain
func TestDeepHierarchy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping deep hierarchy test in short mode")
	}

	store, cleanup := newStressTestStore(t)
	defer cleanup()

	depth := 100
	keys := make([]ouroboroskv.Hash, depth)

	// Create deep chain
	t.Logf("Creating %d-level deep hierarchy...", depth)
	for i := 0; i < depth; i++ {
		var parentKey *ouroboroskv.Hash
		if i > 0 {
			parentKey = &keys[i-1]
		}

		var parentHash ouroboroskv.Hash
		if parentKey != nil {
			parentHash = *parentKey
		}

		data := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("level-%d", i)),
			Parent:         parentHash,
			RSDataSlices:   3,
			RSParitySlices: 2,
		}

		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("Failed at level %d: %v", i, err)
		}
		keys[i] = key
	}

	// Verify the chain
	t.Log("Verifying hierarchy...")

	// Check bottom has correct ancestry
	ancestors, err := store.GetAncestors(keys[depth-1])
	if err != nil {
		t.Fatalf("Failed to get ancestors: %v", err)
	}
	if len(ancestors) != depth-1 {
		t.Errorf("Expected %d ancestors, got %d", depth-1, len(ancestors))
	}

	// Check top has correct descendants
	descendants, err := store.GetDescendants(keys[0])
	if err != nil {
		t.Fatalf("Failed to get descendants: %v", err)
	}
	if len(descendants) != depth-1 {
		t.Errorf("Expected %d descendants, got %d", depth-1, len(descendants))
	}

	t.Logf("✅ Successfully created and verified %d-level deep hierarchy", depth)
}

// TestWideHierarchy tests one parent with many children
func TestWideHierarchy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping wide hierarchy test in short mode")
	}

	store, cleanup := newStressTestStore(t)
	defer cleanup()

	numChildren := 1000

	// Create parent
	parentData := ouroboroskv.Data{
		Content:        []byte("parent-with-many-children"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	parentKey, err := store.WriteData(parentData)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	// Create many children
	t.Logf("Creating parent with %d children...", numChildren)
	childKeys := make([]ouroboroskv.Hash, numChildren)
	for i := 0; i < numChildren; i++ {
		childData := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("child-%d", i)),
			Parent:         parentKey,
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		key, err := store.WriteData(childData)
		if err != nil {
			t.Fatalf("Failed to create child %d: %v", i, err)
		}
		childKeys[i] = key

		if (i+1)%100 == 0 {
			t.Logf("Created %d children...", i+1)
		}
	}

	// Verify parent has all children
	t.Log("Verifying children...")
	children, err := store.GetChildren(parentKey)
	if err != nil {
		t.Fatalf("Failed to get children: %v", err)
	}

	if len(children) != numChildren {
		t.Errorf("Expected %d children, got %d", numChildren, len(children))
	}

	// Verify each child has correct parent
	for i := 0; i < numChildren; i += 100 {
		parent, err := store.GetParent(childKeys[i])
		if err != nil {
			t.Errorf("Failed to get parent for child %d: %v", i, err)
		}
		if parent != parentKey {
			t.Errorf("Child %d has wrong parent", i)
		}
	}

	t.Logf("✅ Successfully created and verified parent with %d children", numChildren)
}

// TestLargeBatchWrite tests writing many items in a single batch
func TestLargeBatchWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large batch test in short mode")
	}

	store, cleanup := newStressTestStore(t)
	defer cleanup()

	batchSize := 1000
	t.Logf("Creating batch of %d items...", batchSize)

	batch := make([]ouroboroskv.Data, batchSize)
	for i := 0; i < batchSize; i++ {
		batch[i] = ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("batch-item-%d", i)),
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
	}

	keys, err := store.BatchWriteData(batch)
	if err != nil {
		t.Fatalf("Batch write failed: %v", err)
	}

	if len(keys) != batchSize {
		t.Errorf("Expected %d keys, got %d", batchSize, len(keys))
	}

	// Verify random samples
	t.Log("Verifying sample items...")
	samples := []int{0, 100, 500, 999}
	for _, idx := range samples {
		if idx < len(keys) {
			data, err := store.ReadData(keys[idx])
			if err != nil {
				t.Errorf("Failed to read item %d: %v", idx, err)
			}
			expected := fmt.Sprintf("batch-item-%d", idx)
			if string(data.Content) != expected {
				t.Errorf("Item %d content mismatch", idx)
			}
		}
	}

	t.Logf("✅ Successfully wrote and verified batch of %d items", batchSize)
}

// TestSingleItemBatch tests edge case of batch with one item
func TestSingleItemBatch(t *testing.T) {
	store, cleanup := newStressTestStore(t)
	defer cleanup()

	batch := []ouroboroskv.Data{
		{
			Content:        []byte("single-item"),
			RSDataSlices:   3,
			RSParitySlices: 2,
		},
	}

	keys, err := store.BatchWriteData(batch)
	if err != nil {
		t.Fatalf("Single item batch failed: %v", err)
	}

	if len(keys) != 1 {
		t.Errorf("Expected 1 key, got %d", len(keys))
	}

	// Verify
	data, err := store.ReadData(keys[0])
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	if string(data.Content) != "single-item" {
		t.Error("Content mismatch")
	}

	t.Log("✅ Single item batch succeeded")
}

// TestManySmallWrites tests database performance with many small writes
func TestManySmallWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping many writes test in short mode")
	}

	store, cleanup := newStressTestStore(t)
	defer cleanup()

	numWrites := 500
	t.Logf("Writing %d small items...", numWrites)

	keys := make([]ouroboroskv.Hash, numWrites)
	for i := 0; i < numWrites; i++ {
		data := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("item-%d", i)),
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
		keys[i] = key

		if (i+1)%100 == 0 {
			t.Logf("Wrote %d items...", i+1)
		}
	}

	// List all keys
	allKeys, err := store.ListKeys()
	if err != nil {
		t.Fatalf("Failed to list keys: %v", err)
	}

	if len(allKeys) < numWrites {
		t.Errorf("Expected at least %d keys, got %d", numWrites, len(allKeys))
	}

	t.Logf("✅ Successfully wrote %d items, database has %d total keys", numWrites, len(allKeys))
}

// TestComplexMultiLevelHierarchy tests a realistic tree structure
func TestComplexMultiLevelHierarchy(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping complex hierarchy test in short mode")
	}

	store, cleanup := newStressTestStore(t)
	defer cleanup()

	// Create root
	rootData := ouroboroskv.Data{
		Content:        []byte("root"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	rootKey, err := store.WriteData(rootData)
	if err != nil {
		t.Fatalf("Failed to create root: %v", err)
	}

	// Level 1: 10 children
	level1Keys := make([]ouroboroskv.Hash, 10)
	for i := 0; i < 10; i++ {
		data := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("level1-%d", i)),
			Parent:         rootKey,
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("Failed to create level1-%d: %v", i, err)
		}
		level1Keys[i] = key
	}

	// Level 2: Each level1 has 5 children (50 total)
	totalLevel2 := 0
	for i, parent := range level1Keys {
		for j := 0; j < 5; j++ {
			data := ouroboroskv.Data{
				Content:        []byte(fmt.Sprintf("level2-%d-%d", i, j)),
				Parent:         parent,
				RSDataSlices:   3,
				RSParitySlices: 2,
			}
			_, err := store.WriteData(data)
			if err != nil {
				t.Fatalf("Failed to create level2-%d-%d: %v", i, j, err)
			}
			totalLevel2++
		}
	}

	// Verify structure
	t.Log("Verifying hierarchy structure...")

	rootChildren, err := store.GetChildren(rootKey)
	if err != nil {
		t.Fatalf("Failed to get root children: %v", err)
	}
	if len(rootChildren) != 10 {
		t.Errorf("Root should have 10 children, got %d", len(rootChildren))
	}

	rootDescendants, err := store.GetDescendants(rootKey)
	if err != nil {
		t.Fatalf("Failed to get root descendants: %v", err)
	}
	if len(rootDescendants) != 60 { // 10 + 50
		t.Errorf("Root should have 60 descendants, got %d", len(rootDescendants))
	}

	t.Logf("✅ Successfully created complex hierarchy: 1 root + 10 L1 + 50 L2 = 61 nodes")
}

// TestDataSizeMediumRange tests various medium-sized data (1KB - 100KB)
func TestDataSizeMediumRange(t *testing.T) {
	store, cleanup := newStressTestStore(t)
	defer cleanup()

	sizes := []int{
		1 * 1024,   // 1KB
		10 * 1024,  // 10KB
		50 * 1024,  // 50KB
		100 * 1024, // 100KB
		250 * 1024, // 250KB
	}

	for _, size := range sizes {
		content := make([]byte, size)
		for i := range content {
			content[i] = byte(i % 256)
		}

		data := ouroboroskv.Data{
			Content:        content,
			RSDataSlices:   3,
			RSParitySlices: 2,
		}

		key, err := store.WriteData(data)
		if err != nil {
			t.Errorf("Failed to write %d KB: %v", size/1024, err)
			continue
		}

		readData, err := store.ReadData(key)
		if err != nil {
			t.Errorf("Failed to read %d KB: %v", size/1024, err)
			continue
		}

		if len(readData.Content) != size {
			t.Errorf("Size %d KB: expected %d bytes, got %d", size/1024, size, len(readData.Content))
		}

		t.Logf("✅ %d KB: write and read successful", size/1024)
	}
}

// TestEmptyContentWithMetadata tests data with no content but metadata
func TestEmptyContentWithMetadata(t *testing.T) {
	store, cleanup := newStressTestStore(t)
	defer cleanup()

	data := ouroboroskv.Data{
		Content:        []byte{}, // Empty content
		Meta:           []byte(`{"type": "metadata-only", "timestamp": "2025-11-23"}`),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}

	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("Failed to write empty content with metadata: %v", err)
	}

	readData, err := store.ReadData(key)
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	if len(readData.Content) != 0 {
		t.Errorf("Expected empty content, got %d bytes", len(readData.Content))
	}

	if len(readData.Meta) == 0 {
		t.Error("Metadata was lost")
	}

	t.Log("✅ Empty content with metadata test passed")
}

// TestRSParameterVariations tests different Reed-Solomon configurations
func TestRSParameterVariations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping RS variations test in short mode")
	}

	configs := []struct {
		name         string
		dataSlices   int
		paritySlices int
	}{
		{"Minimal", 2, 1},
		{"Standard", 3, 2},
		{"High-Redundancy", 4, 4},
		{"Large-File", 8, 4},
	}

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			// Create fresh store for each RS configuration
			store, cleanup := newStressTestStore(t)
			defer cleanup()

			content := []byte("test-content-for-rs-variations-" + cfg.name)
			data := ouroboroskv.Data{
				Content:        content,
				RSDataSlices:   uint8(cfg.dataSlices),
				RSParitySlices: uint8(cfg.paritySlices),
			}

			key, err := store.WriteData(data)
			if err != nil {
				t.Fatalf("Failed with RS %d/%d: %v", cfg.dataSlices, cfg.paritySlices, err)
			}

			readData, err := store.ReadData(key)
			if err != nil {
				t.Fatalf("Failed to read RS %d/%d: %v", cfg.dataSlices, cfg.paritySlices, err)
			}

			if string(readData.Content) != string(content) {
				t.Error("Content mismatch")
			}

			t.Logf("✅ RS %d/%d succeeded", cfg.dataSlices, cfg.paritySlices)
		})
	}
}

// TestListStoredDataPerformance tests ListStoredData with many items
func TestListStoredDataPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	store, cleanup := newStressTestStore(t)
	defer cleanup()

	// Create various items
	numItems := 100
	t.Logf("Creating %d items for listing...", numItems)

	for i := 0; i < numItems; i++ {
		data := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("item-%d", i)),
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		_, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("Failed to write item %d: %v", i, err)
		}
	}

	// List all
	items, err := store.ListStoredData()
	if err != nil {
		t.Fatalf("ListStoredData failed: %v", err)
	}

	if len(items) < numItems {
		t.Errorf("Expected at least %d items, got %d", numItems, len(items))
	}

	// Verify a sample has proper info
	if len(items) > 0 {
		// items is already []DataInfo, just use it directly
		firstInfo := items[0]
		t.Logf("Sample item info: key=%s, chunks=%d", firstInfo.KeyBase64, firstInfo.NumChunks)
	}

	t.Logf("✅ Listed %d items successfully", len(items))
}
