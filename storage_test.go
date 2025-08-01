package ouroboroskv

import (
	"bytes"
	"os"
	"testing"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/sirupsen/logrus"
)

// setupTestKVForStorage creates a test KV instance for storage tests
func setupTestKVForStorage(t *testing.T) (*KV, func()) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "ouroboros-kv-storage-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	// Create a crypt instance for testing
	cryptInstance := crypt.New()

	// Create config
	config := &StoreConfig{
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

// createTestStorageData creates test data for storage tests
func createTestStorageData() Data {
	return Data{
		Key:                     hash.HashString("test-storage-key"),
		Content:                 []byte("This is test content for storage and retrieval testing. It should be long enough to test the full pipeline."),
		Parent:                  hash.HashString("parent-storage-key"),
		Children:                []hash.Hash{hash.HashString("child1-storage"), hash.HashString("child2-storage")},
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}
}

func TestWriteData(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	testData := createTestStorageData()

	err := kv.WriteData(testData)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	// Verify data exists
	exists, err := kv.DataExists(testData.Key)
	if err != nil {
		t.Fatalf("DataExists failed: %v", err)
	}
	if !exists {
		t.Error("Data should exist after writing")
	}
}

func TestReadData(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	// Write test data
	originalData := createTestStorageData()
	err := kv.WriteData(originalData)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	// Read the data back
	readData, err := kv.ReadData(originalData.Key)
	if err != nil {
		t.Fatalf("ReadData failed: %v", err)
	}

	// Verify the data matches
	if readData.Key != originalData.Key {
		t.Errorf("Key mismatch: expected %v, got %v", originalData.Key, readData.Key)
	}
	if !bytes.Equal(readData.Content, originalData.Content) {
		t.Errorf("Content mismatch: expected %s, got %s", originalData.Content, readData.Content)
	}
	if readData.Parent != originalData.Parent {
		t.Errorf("Parent mismatch: expected %v, got %v", originalData.Parent, readData.Parent)
	}
	if len(readData.Children) != len(originalData.Children) {
		t.Errorf("Children count mismatch: expected %d, got %d", len(originalData.Children), len(readData.Children))
	}
	for i, child := range readData.Children {
		if child != originalData.Children[i] {
			t.Errorf("Child %d mismatch: expected %v, got %v", i, originalData.Children[i], child)
		}
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	// Test cases with different content sizes
	testCases := []struct {
		name        string
		contentSize int
		shards      uint8
		parity      uint8
	}{
		{"tiny", 10, 2, 1},
		{"small", 100, 3, 2},
		{"medium", 1000, 4, 2},
		{"large", 10000, 5, 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create test content
			content := make([]byte, tc.contentSize)
			for i := range content {
				content[i] = byte(i % 256)
			}

			originalData := Data{
				Key:                     hash.HashString("roundtrip-storage-" + tc.name),
				Content:                 content,
				Parent:                  hash.HashString("parent"),
				Children:                []hash.Hash{hash.HashString("child1")},
				ReedSolomonShards:       tc.shards,
				ReedSolomonParityShards: tc.parity,
			}

			// Write
			err := kv.WriteData(originalData)
			if err != nil {
				t.Fatalf("WriteData failed for %s: %v", tc.name, err)
			}

			// Read
			readData, err := kv.ReadData(originalData.Key)
			if err != nil {
				t.Fatalf("ReadData failed for %s: %v", tc.name, err)
			}

			// Verify round-trip integrity
			if !bytes.Equal(readData.Content, originalData.Content) {
				t.Errorf("Round-trip failed for %s: content mismatch", tc.name)
			}
			if readData.Key != originalData.Key {
				t.Errorf("Round-trip failed for %s: key mismatch", tc.name)
			}
		})
	}
}

func TestReadDataNotFound(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	// Try to read non-existent data
	nonExistentKey := hash.HashString("non-existent-key")
	_, err := kv.ReadData(nonExistentKey)
	if err == nil {
		t.Error("Expected error when reading non-existent data")
	}
}

func TestDataExists(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	testData := createTestStorageData()

	// Check non-existent data
	exists, err := kv.DataExists(testData.Key)
	if err != nil {
		t.Fatalf("DataExists failed: %v", err)
	}
	if exists {
		t.Error("Data should not exist before writing")
	}

	// Write data
	err = kv.WriteData(testData)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	// Check existing data
	exists, err = kv.DataExists(testData.Key)
	if err != nil {
		t.Fatalf("DataExists failed: %v", err)
	}
	if !exists {
		t.Error("Data should exist after writing")
	}
}

func TestBatchWriteData(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	// Create multiple test data objects
	var dataList []Data
	for i := 0; i < 5; i++ {
		data := Data{
			Key:                     hash.HashString("batch-test-" + string(rune('0'+i))),
			Content:                 []byte("Batch test content " + string(rune('0'+i))),
			Parent:                  hash.HashString("batch-parent"),
			Children:                []hash.Hash{},
			ReedSolomonShards:       2,
			ReedSolomonParityShards: 1,
		}
		dataList = append(dataList, data)
	}

	// Batch write
	err := kv.BatchWriteData(dataList)
	if err != nil {
		t.Fatalf("BatchWriteData failed: %v", err)
	}

	// Verify all data exists
	for _, data := range dataList {
		exists, err := kv.DataExists(data.Key)
		if err != nil {
			t.Fatalf("DataExists failed for key %x: %v", data.Key, err)
		}
		if !exists {
			t.Errorf("Data should exist for key %x", data.Key)
		}
	}
}

func TestBatchReadData(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	// Create and write multiple test data objects
	var originalDataList []Data
	var keys []hash.Hash
	for i := 0; i < 3; i++ {
		data := Data{
			Key:                     hash.HashString("batch-read-test-" + string(rune('0'+i))),
			Content:                 []byte("Batch read test content " + string(rune('0'+i))),
			Parent:                  hash.HashString("batch-read-parent"),
			Children:                []hash.Hash{},
			ReedSolomonShards:       2,
			ReedSolomonParityShards: 1,
		}
		originalDataList = append(originalDataList, data)
		keys = append(keys, data.Key)

		// Write individual data
		err := kv.WriteData(data)
		if err != nil {
			t.Fatalf("WriteData failed for key %x: %v", data.Key, err)
		}
	}

	// Batch read
	readDataList, err := kv.BatchReadData(keys)
	if err != nil {
		t.Fatalf("BatchReadData failed: %v", err)
	}

	// Verify all data matches
	if len(readDataList) != len(originalDataList) {
		t.Fatalf("Expected %d data objects, got %d", len(originalDataList), len(readDataList))
	}

	for i, readData := range readDataList {
		originalData := originalDataList[i]
		if readData.Key != originalData.Key {
			t.Errorf("Key mismatch for item %d: expected %v, got %v", i, originalData.Key, readData.Key)
		}
		if !bytes.Equal(readData.Content, originalData.Content) {
			t.Errorf("Content mismatch for item %d", i)
		}
	}
}

func TestListKeys(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	// Write multiple data objects
	var originalKeys []hash.Hash
	for i := 0; i < 3; i++ {
		data := Data{
			Key:                     hash.HashString("list-test-" + string(rune('0'+i))),
			Content:                 []byte("List test content " + string(rune('0'+i))),
			ReedSolomonShards:       2,
			ReedSolomonParityShards: 1,
		}
		originalKeys = append(originalKeys, data.Key)

		err := kv.WriteData(data)
		if err != nil {
			t.Fatalf("WriteData failed: %v", err)
		}
	}

	// List keys
	listedKeys, err := kv.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys failed: %v", err)
	}

	// Verify all keys are present
	if len(listedKeys) < len(originalKeys) {
		t.Errorf("Expected at least %d keys, got %d", len(originalKeys), len(listedKeys))
	}

	// Check that all our keys are in the list
	keyMap := make(map[hash.Hash]bool)
	for _, key := range listedKeys {
		keyMap[key] = true
	}

	for _, originalKey := range originalKeys {
		if !keyMap[originalKey] {
			t.Errorf("Key %x not found in listed keys", originalKey)
		}
	}
}

func TestWriteDataEmptyContent(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	// Test data with empty content
	testData := Data{
		Key:                     hash.HashString("empty-content-test"),
		Content:                 []byte{},
		Parent:                  hash.HashString("parent"),
		Children:                []hash.Hash{},
		ReedSolomonShards:       2,
		ReedSolomonParityShards: 1,
	}

	// Write empty content
	err := kv.WriteData(testData)
	if err != nil {
		t.Fatalf("WriteData with empty content failed: %v", err)
	}

	// Read back and verify
	readData, err := kv.ReadData(testData.Key)
	if err != nil {
		t.Fatalf("ReadData failed: %v", err)
	}

	if len(readData.Content) != 0 {
		t.Errorf("Expected empty content, got %d bytes", len(readData.Content))
	}
}

func TestBatchWriteEmptyList(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	// Test with empty list
	err := kv.BatchWriteData([]Data{})
	if err != nil {
		t.Errorf("BatchWriteData with empty list should not fail: %v", err)
	}
}

func TestBatchReadEmptyList(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	// Test with empty list
	result, err := kv.BatchReadData([]hash.Hash{})
	if err != nil {
		t.Errorf("BatchReadData with empty list should not fail: %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil result for empty list, got %v", result)
	}
}
