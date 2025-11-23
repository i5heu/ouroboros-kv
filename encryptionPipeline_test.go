package ouroboroskv

import (
	"bytes"
	"os"
	"testing"

	"log/slog"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/pkg/encrypt"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
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
	config := &Config{
		Paths:            []string{tempDir},
		MinimumFreeSpace: 1, // 1GB minimum
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
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
	data := applyTestDefaults(Data{
		Meta:           []byte("Test metadata"),
		Content:        []byte("This is test content for the encryption pipeline. It should be long enough to test chunking functionality."),
		Parent:         hash.HashString("parent-key"),
		Children:       []hash.Hash{hash.HashString("child1"), hash.HashString("child2")},
		RSDataSlices:   3,
		RSParitySlices: 2,
	})
	data.Key = expectedKeyForData(data)
	return data
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

	if result.Created != testData.Created {
		t.Errorf("Expected created %d, got %d", testData.Created, result.Created)
	}

	if len(result.Aliases) != len(testData.Aliases) {
		t.Errorf("Expected %d aliases, got %d", len(testData.Aliases), len(result.Aliases))
	}

	// Verify chunks were created
	if len(result.Slices) == 0 {
		t.Error("Expected content slices to be created, but got none")
	}

	if len(result.MetaSlices) == 0 {
		t.Error("Expected metadata slices to be created, but got none")
	}

	// Verify Reed-Solomon settings
	totalSlices := testData.RSDataSlices + testData.RSParitySlices
	for i, chunk := range result.Slices {
		if chunk.RSDataSlices != testData.RSDataSlices {
			t.Errorf("Chunk %d: expected %d Reed-Solomon slices, got %d", i, testData.RSDataSlices, chunk.RSDataSlices)
		}
		if chunk.RSParitySlices != testData.RSParitySlices {
			t.Errorf("Chunk %d: expected %d Reed-Solomon parity slices, got %d", i, testData.RSParitySlices, chunk.RSParitySlices)
		}
		if chunk.RSSliceIndex >= totalSlices {
			t.Errorf("Chunk %d: Reed-Solomon index %d is out of range (max %d)", i, chunk.RSSliceIndex, totalSlices-1)
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

	if len(result.Slices) != 0 {
		t.Errorf("Expected no content slices, got %d", len(result.Slices))
	}

	if len(result.MetaSlices) == 0 {
		t.Error("Expected metadata slices to be created")
	}

	decoded, err := kv.decodeDataPipeline(result)
	if err != nil {
		t.Fatalf("decodeDataPipeline failed: %v", err)
	}

	if len(decoded.Content) != 0 {
		t.Errorf("Expected empty content, got %d bytes", len(decoded.Content))
	}

	if !bytes.Equal(decoded.Meta, testData.Meta) {
		t.Error("Metadata round-trip mismatch for empty content case")
	}
}

func TestChunker(t *testing.T) {
	kv, cleanup := setupTestKV(t)
	defer cleanup()

	testData := createTestData()

	chunks, err := kv.chunker(testData.Content)
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
	chunks, err := kv.chunker(testData.Content)
	if err != nil {
		t.Fatalf("chunker with empty content failed: %v", err)
	}

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

	chunks, err := kv.splitIntoRSSlices(testData, encryptedChunks, chunkHashes)
	if err != nil {
		t.Fatalf("splitIntoRSSlices failed: %v", err)
	}

	expectedTotalSlices := int(testData.RSDataSlices + testData.RSParitySlices)
	if len(chunks) != expectedTotalSlices {
		t.Errorf("Expected %d chunks, got %d", expectedTotalSlices, len(chunks))
	}

	// Verify chunk properties
	for i, chunk := range chunks {
		if chunk.ChunkHash != chunkHashes[0] {
			t.Errorf("Chunk %d: wrong chunk hash", i)
		}
		if chunk.RSDataSlices != testData.RSDataSlices {
			t.Errorf("Chunk %d: wrong Reed-Solomon slices count", i)
		}
		if chunk.RSParitySlices != testData.RSParitySlices {
			t.Errorf("Chunk %d: wrong Reed-Solomon parity slices count", i)
		}
		if chunk.RSSliceIndex != uint8(i) {
			t.Errorf("Chunk %d: expected index %d, got %d", i, i, chunk.RSSliceIndex)
		}
		if len(chunk.Payload) == 0 {
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

func TestReedSolomonSplitterInvalidSliceConfig(t *testing.T) {
	kv, cleanup := setupTestKV(t)
	defer cleanup()

	testData := createTestData()
	testData.RSDataSlices = 0 // Invalid configuration
	testData.RSParitySlices = 1

	testContent := []byte("test content")
	encryptedChunk, err := kv.crypt.Encrypt(testContent)
	if err != nil {
		t.Fatalf("Failed to encrypt test content: %v", err)
	}

	encryptedChunks := []*encrypt.EncryptResult{encryptedChunk}
	chunkHashes := []hash.Hash{hash.HashBytes(testContent)}

	_, err = kv.splitIntoRSSlices(testData, encryptedChunks, chunkHashes)
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
				Key:            hash.HashString("test-key-" + tc.name),
				Meta:           []byte("metadata-" + tc.name),
				Content:        content,
				Parent:         hash.HashString("parent"),
				Children:       []hash.Hash{},
				RSDataSlices:   2,
				RSParitySlices: 1,
			}

			result, err := kv.encodeDataPipeline(testData)
			if err != nil {
				t.Fatalf("encodeDataPipeline failed for %s: %v", tc.name, err)
			}

			// Verify result structure
			if len(result.Slices) == 0 {
				t.Errorf("No chunks created for %s", tc.name)
			}

			if len(result.MetaSlices) == 0 {
				t.Errorf("No metadata slices created for %s", tc.name)
			}

			// Verify all chunks have consistent Reed-Solomon settings
			expectedTotal := testData.RSDataSlices + testData.RSParitySlices
			chunksByGroup := make(map[hash.Hash][]SealedSlice)

			for _, chunk := range result.Slices {
				chunksByGroup[chunk.ChunkHash] = append(chunksByGroup[chunk.ChunkHash], chunk)
			}

			for chunkHash, chunks := range chunksByGroup {
				if len(chunks) != int(expectedTotal) {
					t.Errorf("Chunk group %v: expected %d slices, got %d", chunkHash, expectedTotal, len(chunks))
				}
			}

			decoded, err := kv.decodeDataPipeline(result)
			if err != nil {
				t.Fatalf("decodeDataPipeline failed for %s: %v", tc.name, err)
			}

			if !bytes.Equal(decoded.Content, testData.Content) {
				t.Errorf("Decoded content mismatch for %s", tc.name)
			}

			if !bytes.Equal(decoded.Meta, testData.Meta) {
				t.Errorf("Decoded metadata mismatch for %s", tc.name)
			}
		})
	}
}
