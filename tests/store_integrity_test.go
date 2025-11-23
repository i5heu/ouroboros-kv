package ouroboroskv__test

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"testing"

	crypt "github.com/i5heu/ouroboros-crypt"
	ouroboroskv "github.com/i5heu/ouroboros-kv"
	"github.com/i5heu/ouroboros-kv/pkg/config"
)

func newIntegrityTestStore(t *testing.T) (ouroboroskv.Store, func()) {
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

// TestIntegritySmallData verifies data and metadata integrity for small files
func TestIntegritySmallData(t *testing.T) {
	store, cleanup := newIntegrityTestStore(t)
	defer cleanup()

	// Create test data
	content := []byte("This is a small test file with some content to verify integrity")
	meta := []byte(`{"timestamp": "2025-11-23", "version": "1.0", "author": "integrity-test"}`)

	// Hash the original content and metadata
	contentHashArray := sha256.Sum256(content)
	contentHashHex := hex.EncodeToString(contentHashArray[:])
	metaHashArray := sha256.Sum256(meta)
	metaHashHex := hex.EncodeToString(metaHashArray[:])

	t.Logf("Original content hash: %s", contentHashHex)
	t.Logf("Original meta hash: %s", metaHashHex)

	// Write data
	key, err := store.WriteData(ouroboroskv.Data{
		Content:        content,
		ContentType:    "text/plain",
		Meta:           meta,
		RSDataSlices:   3,
		RSParitySlices: 2,
	})
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}
	t.Logf("Written with key: %x", key)

	// Read data back
	readData, err := store.ReadData(key)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	// Hash the retrieved content and metadata
	retrievedContentHashArray := sha256.Sum256(readData.Content)
	retrievedContentHashHex := hex.EncodeToString(retrievedContentHashArray[:])
	retrievedMetaHashArray := sha256.Sum256(readData.Meta)
	retrievedMetaHashHex := hex.EncodeToString(retrievedMetaHashArray[:])

	t.Logf("Retrieved content hash: %s", retrievedContentHashHex)
	t.Logf("Retrieved meta hash: %s", retrievedMetaHashHex)

	// Verify integrity
	if contentHashHex != retrievedContentHashHex {
		t.Errorf("Content hash mismatch!\nExpected: %s\nGot: %s", contentHashHex, retrievedContentHashHex)
	}
	if metaHashHex != retrievedMetaHashHex {
		t.Errorf("Meta hash mismatch!\nExpected: %s\nGot: %s", metaHashHex, retrievedMetaHashHex)
	}
	if readData.ContentType != "text/plain" {
		t.Errorf("ContentType mismatch! Expected: text/plain, Got: %s", readData.ContentType)
	}

	t.Log("✅ Small data integrity verified!")
}

// TestIntegrityLargeData verifies data and metadata integrity for large files (>1MB)
func TestIntegrityLargeData(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "ouroboros-kv-integrity-large-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	cryptInstance := crypt.New()
	cfg := &config.Config{
		Paths:            []string{tempDir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}
	kv, err := ouroboroskv.Init(cryptInstance, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()

	// Create 2MB of random data
	dataSize := 2 * 1024 * 1024
	content := make([]byte, dataSize)
	_, err = rand.Read(content)
	if err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}

	// Create large metadata
	meta := []byte(`{
"timestamp": "2025-11-23",
"version": "2.0",
"author": "integrity-test",
"description": "Testing integrity of large files with chunking",
"tags": ["test", "integrity", "large-file", "chunking"],
"properties": {
"size": 2097152,
"type": "binary",
"compression": true,
"encryption": true
}
}`)

	// Hash the original content and metadata
	contentHashArray := sha256.Sum256(content)
	contentHashHex := hex.EncodeToString(contentHashArray[:])
	metaHashArray := sha256.Sum256(meta)
	metaHashHex := hex.EncodeToString(metaHashArray[:])

	t.Logf("Original content hash: %s (size: %d bytes)", contentHashHex, len(content))
	t.Logf("Original meta hash: %s (size: %d bytes)", metaHashHex, len(meta))

	// Write data
	key, err := kv.WriteData(ouroboroskv.Data{
		Content:        content,
		ContentType:    "application/octet-stream",
		Meta:           meta,
		RSDataSlices:   8,
		RSParitySlices: 4,
	})
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}
	t.Logf("Written with key: %x", key)

	// Log chunking information (chunking is optional based on compression)
	children, err := kv.GetChildren(key)
	if err != nil {
		t.Fatalf("Failed to get children: %v", err)
	}
	t.Logf("Number of chunks: %d", len(children)) // Read data back
	readData, err := kv.ReadData(key)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	// Hash the retrieved content and metadata
	retrievedContentHashArray := sha256.Sum256(readData.Content)
	retrievedContentHashHex := hex.EncodeToString(retrievedContentHashArray[:])
	retrievedMetaHashArray := sha256.Sum256(readData.Meta)
	retrievedMetaHashHex := hex.EncodeToString(retrievedMetaHashArray[:])

	t.Logf("Retrieved content hash: %s (size: %d bytes)", retrievedContentHashHex, len(readData.Content))
	t.Logf("Retrieved meta hash: %s (size: %d bytes)", retrievedMetaHashHex, len(readData.Meta))

	// Verify integrity
	if len(readData.Content) != dataSize {
		t.Errorf("Content size mismatch! Expected: %d, Got: %d", dataSize, len(readData.Content))
	}
	if contentHashHex != retrievedContentHashHex {
		t.Errorf("Content hash mismatch!\nExpected: %s\nGot: %s", contentHashHex, retrievedContentHashHex)
	}
	if metaHashHex != retrievedMetaHashHex {
		t.Errorf("Meta hash mismatch!\nExpected: %s\nGot: %s", metaHashHex, retrievedMetaHashHex)
	}
	if readData.ContentType != "application/octet-stream" {
		t.Errorf("ContentType mismatch! Expected: application/octet-stream, Got: %s", readData.ContentType)
	}

	t.Log("✅ Large data (2MB) integrity verified across chunks!")
}

// TestIntegrityBatchOperations verifies integrity for batch writes and reads
func TestIntegrityBatchOperations(t *testing.T) {
	store, cleanup := newIntegrityTestStore(t)
	defer cleanup()

	// Create batch data with hashes
	numItems := 10
	batchData := make([]ouroboroskv.Data, numItems)
	contentHashes := make([]string, numItems)
	metaHashes := make([]string, numItems)

	for i := 0; i < numItems; i++ {
		content := []byte(fmt.Sprintf("Batch item %d with unique content for integrity testing", i))
		meta := []byte(fmt.Sprintf(`{"item": %d, "batch": true}`, i))

		contentHashArray := sha256.Sum256(content)
		contentHashes[i] = hex.EncodeToString(contentHashArray[:])
		metaHashArray := sha256.Sum256(meta)
		metaHashes[i] = hex.EncodeToString(metaHashArray[:])

		batchData[i] = ouroboroskv.Data{
			Content:        content,
			ContentType:    fmt.Sprintf("batch/item-%d", i),
			Meta:           meta,
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
	}

	// Batch write
	keys, err := store.BatchWriteData(batchData)
	if err != nil {
		t.Fatalf("Failed to batch write data: %v", err)
	}
	if len(keys) != numItems {
		t.Fatalf("Expected %d keys, got %d", numItems, len(keys))
	}

	// Batch read
	readData, err := store.BatchReadData(keys)
	if err != nil {
		t.Fatalf("Failed to batch read data: %v", err)
	}
	if len(readData) != numItems {
		t.Fatalf("Expected %d items, got %d", numItems, len(readData))
	}

	// Verify integrity for all items
	for i := 0; i < numItems; i++ {
		retrievedContentHashArray := sha256.Sum256(readData[i].Content)
		retrievedContentHashHex := hex.EncodeToString(retrievedContentHashArray[:])
		retrievedMetaHashArray := sha256.Sum256(readData[i].Meta)
		retrievedMetaHashHex := hex.EncodeToString(retrievedMetaHashArray[:])

		if contentHashes[i] != retrievedContentHashHex {
			t.Errorf("Item %d: Content hash mismatch!\nExpected: %s\nGot: %s",
				i, contentHashes[i], retrievedContentHashHex)
		}
		if metaHashes[i] != retrievedMetaHashHex {
			t.Errorf("Item %d: Meta hash mismatch!\nExpected: %s\nGot: %s",
				i, metaHashes[i], retrievedMetaHashHex)
		}
	}

	t.Logf("✅ Batch integrity verified for %d items!", numItems)
}

// TestIntegrityWithRelationships verifies integrity is maintained through parent-child relationships
func TestIntegrityWithRelationships(t *testing.T) {
	store, cleanup := newIntegrityTestStore(t)
	defer cleanup()

	// Create parent
	parentContent := []byte("Parent document with important data")
	parentMeta := []byte(`{"type": "parent", "level": 0}`)
	parentContentHashArray := sha256.Sum256(parentContent)
	parentContentHash := hex.EncodeToString(parentContentHashArray[:])
	parentMetaHashArray := sha256.Sum256(parentMeta)
	parentMetaHash := hex.EncodeToString(parentMetaHashArray[:])

	parentKey, err := store.WriteData(ouroboroskv.Data{
		Content:        parentContent,
		ContentType:    "parent",
		Meta:           parentMeta,
		RSDataSlices:   3,
		RSParitySlices: 2,
	})
	if err != nil {
		t.Fatalf("Failed to write parent: %v", err)
	}

	// Create children with parent relationship
	numChildren := 3
	childContentHashes := make([]string, numChildren)
	childMetaHashes := make([]string, numChildren)
	childKeys := make([]ouroboroskv.Hash, numChildren)

	for i := 0; i < numChildren; i++ {
		content := []byte(fmt.Sprintf("Child %d content linked to parent", i))
		meta := []byte(fmt.Sprintf(`{"type": "child", "index": %d, "parent": "%x"}`, i, parentKey))

		contentHashArray := sha256.Sum256(content)
		childContentHashes[i] = hex.EncodeToString(contentHashArray[:])
		metaHashArray := sha256.Sum256(meta)
		childMetaHashes[i] = hex.EncodeToString(metaHashArray[:])

		key, err := store.WriteData(ouroboroskv.Data{
			Content:        content,
			ContentType:    fmt.Sprintf("child-%d", i),
			Meta:           meta,
			Parent:         parentKey,
			RSDataSlices:   3,
			RSParitySlices: 2,
		})
		if err != nil {
			t.Fatalf("Failed to write child %d: %v", i, err)
		}
		childKeys[i] = key
	}

	// Verify parent integrity
	parentData, err := store.ReadData(parentKey)
	if err != nil {
		t.Fatalf("Failed to read parent: %v", err)
	}
	retrievedParentContentHashArray := sha256.Sum256(parentData.Content)
	retrievedParentContentHash := hex.EncodeToString(retrievedParentContentHashArray[:])
	retrievedParentMetaHashArray := sha256.Sum256(parentData.Meta)
	retrievedParentMetaHash := hex.EncodeToString(retrievedParentMetaHashArray[:])
	if parentContentHash != retrievedParentContentHash {
		t.Errorf("Parent content hash mismatch!")
	}
	if parentMetaHash != retrievedParentMetaHash {
		t.Errorf("Parent meta hash mismatch!")
	}

	// Verify children integrity
	for i := 0; i < numChildren; i++ {
		childData, err := store.ReadData(childKeys[i])
		if err != nil {
			t.Fatalf("Failed to read child %d: %v", i, err)
		}
		retrievedContentHashArray := sha256.Sum256(childData.Content)
		retrievedContentHash := hex.EncodeToString(retrievedContentHashArray[:])
		retrievedMetaHashArray := sha256.Sum256(childData.Meta)
		retrievedMetaHash := hex.EncodeToString(retrievedMetaHashArray[:])
		if childContentHashes[i] != retrievedContentHash {
			t.Errorf("Child %d content hash mismatch!", i)
		}
		if childMetaHashes[i] != retrievedMetaHash {
			t.Errorf("Child %d meta hash mismatch!", i)
		}
	}

	// Verify relationships are intact
	children, err := store.GetChildren(parentKey)
	if err != nil {
		t.Fatalf("Failed to get children: %v", err)
	}
	if len(children) != numChildren {
		t.Errorf("Expected %d children, got %d", numChildren, len(children))
	}

	// Verify each child reports correct parent
	for i := 0; i < numChildren; i++ {
		parent, err := store.GetParent(childKeys[i])
		if err != nil {
			t.Fatalf("Failed to get parent for child %d: %v", i, err)
		}
		if parent != parentKey {
			t.Errorf("Child %d has wrong parent! Expected: %x, Got: %x", i, parentKey, parent)
		}
	}

	t.Logf("✅ Integrity verified through parent-child relationships!")
}

// TestIntegrityBinaryData verifies integrity for binary data with various byte patterns
func TestIntegrityBinaryData(t *testing.T) {
	store, cleanup := newIntegrityTestStore(t)
	defer cleanup()

	testCases := []struct {
		name        string
		contentFunc func() []byte
		metaFunc    func() []byte
	}{
		{
			name: "All zeros",
			contentFunc: func() []byte {
				return make([]byte, 1024)
			},
			metaFunc: func() []byte {
				return []byte(`{"pattern": "zeros"}`)
			},
		},
		{
			name: "All ones",
			contentFunc: func() []byte {
				data := make([]byte, 1024)
				for i := range data {
					data[i] = 0xFF
				}
				return data
			},
			metaFunc: func() []byte {
				return []byte(`{"pattern": "ones"}`)
			},
		},
		{
			name: "Alternating pattern",
			contentFunc: func() []byte {
				data := make([]byte, 1024)
				for i := range data {
					if i%2 == 0 {
						data[i] = 0xAA
					} else {
						data[i] = 0x55
					}
				}
				return data
			},
			metaFunc: func() []byte {
				return []byte(`{"pattern": "alternating"}`)
			},
		},
		{
			name: "Random bytes",
			contentFunc: func() []byte {
				data := make([]byte, 1024)
				rand.Read(data)
				return data
			},
			metaFunc: func() []byte {
				return []byte(`{"pattern": "random"}`)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			content := tc.contentFunc()
			meta := tc.metaFunc()

			// Hash originals
			contentHashArray := sha256.Sum256(content)
			contentHash := hex.EncodeToString(contentHashArray[:])
			metaHashArray := sha256.Sum256(meta)
			metaHash := hex.EncodeToString(metaHashArray[:])

			// Write
			key, err := store.WriteData(ouroboroskv.Data{
				Content:        content,
				ContentType:    "application/binary",
				Meta:           meta,
				RSDataSlices:   3,
				RSParitySlices: 2,
			})
			if err != nil {
				t.Fatalf("Failed to write data: %v", err)
			}

			// Read
			readData, err := store.ReadData(key)
			if err != nil {
				t.Fatalf("Failed to read data: %v", err)
			}

			// Verify hashes
			retrievedContentHashArray := sha256.Sum256(readData.Content)
			retrievedContentHash := hex.EncodeToString(retrievedContentHashArray[:])
			retrievedMetaHashArray := sha256.Sum256(readData.Meta)
			retrievedMetaHash := hex.EncodeToString(retrievedMetaHashArray[:])

			if contentHash != retrievedContentHash {
				t.Errorf("Content hash mismatch!\nExpected: %s\nGot: %s", contentHash, retrievedContentHash)
			}
			if metaHash != retrievedMetaHash {
				t.Errorf("Meta hash mismatch!\nExpected: %s\nGot: %s", metaHash, retrievedMetaHash)
			}

			t.Logf("✅ %s: Integrity verified (hash: %s)", tc.name, contentHash[:16])
		})
	}
}
