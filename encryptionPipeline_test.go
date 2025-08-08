package ouroboroskv

import (
	"bytes"
	"os"
	"testing"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/encrypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/sirupsen/logrus"
)

// setupTestKV creates a test KV instance with temporary directory
func setupTestKV(t *testing.T) (*KV, func()) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "ouroboros-kv-test-*")
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

// createTestData creates test data for encryption pipeline tests
func createTestData() Data {
	return Data{
		Key:                     hash.HashString("test-key"),
		Content:                 []byte("This is test content for the encryption pipeline. It should be long enough to test chunking functionality."),
		Parent:                  hash.HashString("parent-key"),
		Children:                []hash.Hash{hash.HashString("child1"), hash.HashString("child2")},
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}
}

func TestEncodeDataPipeline(t *testing.T) {
	kv, cleanup := setupTestKV(t)
	defer cleanup()

	testData := createTestData()

	result, err := kv.encodeDataPipeline(testData)
	if err != nil {
		t.Fatalf("encodeDataPipeline failed: %v", err)
	}

	// Verify basic structure
	if result.Key != testData.Key {
		t.Errorf("Expected key %v, got %v", testData.Key, result.Key)
	}

	if result.Parent != testData.Parent {
		t.Errorf("Expected parent %v, got %v", testData.Parent, result.Parent)
	}

	if len(result.Children) != len(testData.Children) {
		t.Errorf("Expected %d children, got %d", len(testData.Children), len(result.Children))
	}

	// Verify chunks were created
	if len(result.Chunks) == 0 {
		t.Error("Expected chunks to be created, but got none")
	}

	// Verify Reed-Solomon settings
	totalShards := testData.ReedSolomonShards + testData.ReedSolomonParityShards
	for i, chunk := range result.Chunks {
		if chunk.ReedSolomonShards != testData.ReedSolomonShards {
			t.Errorf("Chunk %d: expected %d Reed-Solomon shards, got %d", i, testData.ReedSolomonShards, chunk.ReedSolomonShards)
		}
		if chunk.ReedSolomonParityShards != testData.ReedSolomonParityShards {
			t.Errorf("Chunk %d: expected %d Reed-Solomon parity shards, got %d", i, testData.ReedSolomonParityShards, chunk.ReedSolomonParityShards)
		}
		if chunk.ReedSolomonIndex >= totalShards {
			t.Errorf("Chunk %d: Reed-Solomon index %d is out of range (max %d)", i, chunk.ReedSolomonIndex, totalShards-1)
		}
	}
}

func TestEncodeDataPipelineEmptyContent(t *testing.T) {
	kv, cleanup := setupTestKV(t)
	defer cleanup()

	testData := createTestData()
	testData.Content = []byte{}

	result, err := kv.encodeDataPipeline(testData)
	if err != nil {
		t.Fatalf("encodeDataPipeline with empty content failed: %v", err)
	}

	// Empty content should result in no chunks since chunker returns 0 chunks
	if len(result.Chunks) != 0 {
		t.Errorf("Expected 0 chunks for empty content, got %d", len(result.Chunks))
	}
}

func TestChunker(t *testing.T) {
	kv, cleanup := setupTestKV(t)
	defer cleanup()

	testData := createTestData()

	chunks, err := kv.chunker(testData)
	if err != nil {
		t.Fatalf("chunker failed: %v", err)
	}

	if len(chunks) == 0 {
		t.Error("Expected at least one chunk")
	}

	// Verify that concatenating chunks gives back original content
	var reconstructed bytes.Buffer
	for _, chunk := range chunks {
		reconstructed.Write(chunk)
	}

	if !bytes.Equal(reconstructed.Bytes(), testData.Content) {
		t.Error("Reconstructed content doesn't match original")
	}
}

func TestChunkerEmptyContent(t *testing.T) {
	kv, cleanup := setupTestKV(t)
	defer cleanup()

	testData := createTestData()
	testData.Content = []byte{}

	chunks, err := kv.chunker(testData)
	if err != nil {
		t.Fatalf("chunker with empty content failed: %v", err)
	}

	// Empty content should result in no chunks
	if len(chunks) != 0 {
		t.Errorf("Expected 0 chunks for empty content, got %d", len(chunks))
	}
}

func TestCompressWithZstd(t *testing.T) {
	testData := []byte("This is test data for Zstd compression. It should compress well if it's repetitive. " +
		"This is test data for Zstd compression. It should compress well if it's repetitive.")

	compressed, err := compressWithZstd(testData)
	if err != nil {
		t.Fatalf("compressWithZstd failed: %v", err)
	}

	if len(compressed) == 0 {
		t.Error("Expected compressed data to have non-zero length")
	}

	// For repetitive data, compression should reduce size
	// Note: This might not always be true for very small data
	if len(compressed) >= len(testData) {
		t.Logf("Warning: Compressed size (%d) is not smaller than original (%d)", len(compressed), len(testData))
	}
}

func TestCompressWithZstdEmptyData(t *testing.T) {
       compressed, err := compressWithZstd([]byte{})
       if err != nil {
	       t.Fatalf("compressWithZstd with empty data failed: %v", err)
       }

       // Zstd returns an empty slice for empty input
       if len(compressed) != 0 {
	       t.Errorf("Expected compressed empty data to have zero length, got %d", len(compressed))
       }
}

func TestReedSolomonSplitter(t *testing.T) {
	kv, cleanup := setupTestKV(t)
	defer cleanup()

	// Create test encrypted chunks
	testData := createTestData()
	testContent := []byte("test content for reed solomon")

	encryptedChunk, err := kv.crypt.Encrypt(testContent)
	if err != nil {
		t.Fatalf("Failed to encrypt test content: %v", err)
	}

	encryptedChunks := []*encrypt.EncryptResult{encryptedChunk}
	chunkHashes := []hash.Hash{hash.HashBytes(testContent)}

	chunks, err := kv.reedSolomonSplitter(testData, encryptedChunks, chunkHashes)
	if err != nil {
		t.Fatalf("reedSolomonSplitter failed: %v", err)
	}

	expectedTotalShards := int(testData.ReedSolomonShards + testData.ReedSolomonParityShards)
	if len(chunks) != expectedTotalShards {
		t.Errorf("Expected %d chunks, got %d", expectedTotalShards, len(chunks))
	}

	// Verify chunk properties
	for i, chunk := range chunks {
		if chunk.ChunkHash != chunkHashes[0] {
			t.Errorf("Chunk %d: wrong chunk hash", i)
		}
		if chunk.ReedSolomonShards != testData.ReedSolomonShards {
			t.Errorf("Chunk %d: wrong Reed-Solomon shards count", i)
		}
		if chunk.ReedSolomonParityShards != testData.ReedSolomonParityShards {
			t.Errorf("Chunk %d: wrong Reed-Solomon parity shards count", i)
		}
		if chunk.ReedSolomonIndex != uint8(i) {
			t.Errorf("Chunk %d: expected index %d, got %d", i, i, chunk.ReedSolomonIndex)
		}
		if len(chunk.ChunkContent) == 0 {
			t.Errorf("Chunk %d: empty content", i)
		}
		if len(chunk.EncapsulatedKey) == 0 {
			t.Errorf("Chunk %d: empty encapsulated key", i)
		}
		if len(chunk.Nonce) == 0 {
			t.Errorf("Chunk %d: empty nonce", i)
		}
	}
}

func TestReedSolomonSplitterInvalidShardConfig(t *testing.T) {
	kv, cleanup := setupTestKV(t)
	defer cleanup()

	testData := createTestData()
	testData.ReedSolomonShards = 0 // Invalid configuration
	testData.ReedSolomonParityShards = 1

	testContent := []byte("test content")
	encryptedChunk, err := kv.crypt.Encrypt(testContent)
	if err != nil {
		t.Fatalf("Failed to encrypt test content: %v", err)
	}

	encryptedChunks := []*encrypt.EncryptResult{encryptedChunk}
	chunkHashes := []hash.Hash{hash.HashBytes(testContent)}

	_, err = kv.reedSolomonSplitter(testData, encryptedChunks, chunkHashes)
	if err == nil {
		t.Error("Expected error for invalid Reed-Solomon configuration, but got none")
	}
}

func TestEncodeDataPipelineIntegration(t *testing.T) {
	kv, cleanup := setupTestKV(t)
	defer cleanup()

	// Test with various content sizes
	testCases := []struct {
		name        string
		contentSize int
	}{
		{"small", 10},
		{"medium", 1000},
		{"large", 10000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			content := make([]byte, tc.contentSize)
			for i := range content {
				content[i] = byte(i % 256)
			}

			testData := Data{
				Key:                     hash.HashString("test-key-" + tc.name),
				Content:                 content,
				Parent:                  hash.HashString("parent"),
				Children:                []hash.Hash{},
				ReedSolomonShards:       2,
				ReedSolomonParityShards: 1,
			}

			result, err := kv.encodeDataPipeline(testData)
			if err != nil {
				t.Fatalf("encodeDataPipeline failed for %s: %v", tc.name, err)
			}

			// Verify result structure
			if len(result.Chunks) == 0 {
				t.Errorf("No chunks created for %s", tc.name)
			}

			// Verify all chunks have consistent Reed-Solomon settings
			expectedTotal := testData.ReedSolomonShards + testData.ReedSolomonParityShards
			chunksByGroup := make(map[hash.Hash][]KvContentChunk)

			for _, chunk := range result.Chunks {
				chunksByGroup[chunk.ChunkHash] = append(chunksByGroup[chunk.ChunkHash], chunk)
			}

			for chunkHash, chunks := range chunksByGroup {
				if len(chunks) != int(expectedTotal) {
					t.Errorf("Chunk group %v: expected %d shards, got %d", chunkHash, expectedTotal, len(chunks))
				}
			}
		})
	}
}
