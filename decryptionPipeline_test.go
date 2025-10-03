package ouroboroskv

import (
	"bytes"
	"os"
	"testing"

	"log/slog"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/hash"
)

// setupTestKVForDecryption creates a test KV instance for decryption tests
func setupTestKVForDecryption(t *testing.T) (*KV, func()) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "ouroboros-kv-decrypt-test-*")
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

func TestDecodeDataPipeline(t *testing.T) {
	kv, cleanup := setupTestKVForDecryption(t)
	defer cleanup()

	// Create test data
	originalData := Data{
		Key:                     hash.HashString("test-key-decode"),
		Content:                 []byte("This is test content for the decryption pipeline. It should be long enough to test the full pipeline."),
		Parent:                  hash.HashString("parent-key"),
		Children:                []hash.Hash{hash.HashString("child1"), hash.HashString("child2")},
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	// Encode the data using the encryption pipeline
	encoded, err := kv.encodeDataPipeline(originalData)
	if err != nil {
		t.Fatalf("Failed to encode data: %v", err)
	}

	// Decode the data using the decryption pipeline
	decoded, err := kv.decodeDataPipeline(encoded)
	if err != nil {
		t.Fatalf("Failed to decode data: %v", err)
	}

	// Verify the decoded data matches the original
	if decoded.Key != originalData.Key {
		t.Errorf("Key mismatch: expected %v, got %v", originalData.Key, decoded.Key)
	}
	if !bytes.Equal(decoded.Content, originalData.Content) {
		t.Errorf("Content mismatch: expected %s, got %s", originalData.Content, decoded.Content)
	}
	if decoded.Parent != originalData.Parent {
		t.Errorf("Parent mismatch: expected %v, got %v", originalData.Parent, decoded.Parent)
	}
	if len(decoded.Children) != len(originalData.Children) {
		t.Errorf("Children count mismatch: expected %d, got %d", len(originalData.Children), len(decoded.Children))
	}
	for i, child := range decoded.Children {
		if child != originalData.Children[i] {
			t.Errorf("Child %d mismatch: expected %v, got %v", i, originalData.Children[i], child)
		}
	}
}

func TestDecodeDataPipelineEmptyContent(t *testing.T) {
	kv, cleanup := setupTestKVForDecryption(t)
	defer cleanup()

	// Create test data with empty content
	originalData := Data{
		Key:                     hash.HashString("test-key-empty"),
		Content:                 []byte{},
		Parent:                  hash.HashString("parent-key"),
		Children:                []hash.Hash{},
		ReedSolomonShards:       2,
		ReedSolomonParityShards: 1,
	}

	// Encode the data
	encoded, err := kv.encodeDataPipeline(originalData)
	if err != nil {
		t.Fatalf("Failed to encode empty data: %v", err)
	}

	// Decode the data
	decoded, err := kv.decodeDataPipeline(encoded)
	if err != nil {
		t.Fatalf("Failed to decode empty data: %v", err)
	}

	// Verify the decoded data
	if !bytes.Equal(decoded.Content, originalData.Content) {
		t.Errorf("Content mismatch for empty data: expected %v, got %v", originalData.Content, decoded.Content)
	}
}

func TestReedSolomonReconstructor(t *testing.T) {
	kv, cleanup := setupTestKVForDecryption(t)
	defer cleanup()

	// Create test data and encode it to get Reed-Solomon chunks
	testData := Data{
		Key:                     hash.HashString("test-rs-reconstruct"),
		Content:                 []byte("This is test content for Reed-Solomon reconstruction testing."),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	encoded, err := kv.encodeDataPipeline(testData)
	if err != nil {
		t.Fatalf("Failed to encode test data: %v", err)
	}

	// Group chunks by chunk hash (simulating what decodeDataPipeline does)
	chunkGroups := make(map[hash.Hash][]kvDataShard)
	for _, chunk := range encoded.Shards {
		chunkGroups[chunk.ChunkHash] = append(chunkGroups[chunk.ChunkHash], chunk)
	}

	// Test reconstruction for each chunk group
	for chunkHash, chunks := range chunkGroups {
		reconstructed, err := kv.reedSolomonReconstructor(chunks)
		if err != nil {
			t.Fatalf("Failed to reconstruct chunk group %v: %v", chunkHash, err)
		}

		// Verify the reconstructed chunk is valid
		if len(reconstructed.Ciphertext) == 0 {
			t.Error("Reconstructed ciphertext is empty")
		}
		if len(reconstructed.EncapsulatedKey) == 0 {
			t.Error("Reconstructed encapsulated key is empty")
		}
		if len(reconstructed.Nonce) == 0 {
			t.Error("Reconstructed nonce is empty")
		}

		// Test that we can decrypt the reconstructed data
		_, err = kv.crypt.Decrypt(reconstructed)
		if err != nil {
			t.Errorf("Failed to decrypt reconstructed data: %v", err)
		}
	}
}

func TestReedSolomonReconstructorWithMissingShards(t *testing.T) {
	kv, cleanup := setupTestKVForDecryption(t)
	defer cleanup()

	// Create test data with Reed-Solomon configuration that allows for missing shards
	testData := Data{
		Key:                     hash.HashString("test-rs-missing"),
		Content:                 []byte("This is test content for testing Reed-Solomon reconstruction with missing shards."),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2, // Can lose up to 2 shards
	}

	encoded, err := kv.encodeDataPipeline(testData)
	if err != nil {
		t.Fatalf("Failed to encode test data: %v", err)
	}

	// Group chunks by chunk hash
	chunkGroups := make(map[hash.Hash][]kvDataShard)
	for _, chunk := range encoded.Shards {
		chunkGroups[chunk.ChunkHash] = append(chunkGroups[chunk.ChunkHash], chunk)
	}

	// Test reconstruction with missing shards for each chunk group
	for chunkHash, chunks := range chunkGroups {
		// Remove one shard (should still be able to reconstruct)
		incompleteChunks := chunks[:len(chunks)-1]

		reconstructed, err := kv.reedSolomonReconstructor(incompleteChunks)
		if err != nil {
			t.Fatalf("Failed to reconstruct chunk group %v with missing shard: %v", chunkHash, err)
		}

		// Verify the reconstructed chunk is valid
		if len(reconstructed.Ciphertext) == 0 {
			t.Error("Reconstructed ciphertext is empty with missing shard")
		}

		// Test that we can decrypt the reconstructed data
		_, err = kv.crypt.Decrypt(reconstructed)
		if err != nil {
			t.Errorf("Failed to decrypt reconstructed data with missing shard: %v", err)
		}
	}
}

func TestReedSolomonReconstructorErrors(t *testing.T) {
	kv, cleanup := setupTestKVForDecryption(t)
	defer cleanup()

	// Test with no chunks
	_, err := kv.reedSolomonReconstructor([]kvDataShard{})
	if err == nil {
		t.Error("Expected error when reconstructing with no chunks")
	}

	// Test with invalid Reed-Solomon index
	invalidChunk := kvDataShard{
		ChunkHash:               hash.HashString("test"),
		ReedSolomonShards:       2,
		ReedSolomonParityShards: 1,
		ReedSolomonIndex:        5, // Invalid index (should be 0-2)
		Size:                    10,
		OriginalSize:            30,
		ChunkContent:            []byte("test"),
		EncapsulatedKey:         []byte("key"),
		Nonce:                   []byte("nonce"),
	}

	_, err = kv.reedSolomonReconstructor([]kvDataShard{invalidChunk})
	if err == nil {
		t.Error("Expected error when reconstructing with invalid Reed-Solomon index")
	}
}

func TestDecompressWithZstd(t *testing.T) {
	// Test data
	originalData := []byte("This is test data for Zstd decompression testing. " +
		"It should be compressed and then decompressed successfully.")

	// Compress the data first
	compressed, err := compressWithZstd(originalData)
	if err != nil {
		t.Fatalf("Failed to compress test data: %v", err)
	}

	// Decompress the data
	decompressed, err := decompressWithZstd(compressed)
	if err != nil {
		t.Fatalf("Failed to decompress data: %v", err)
	}

	// Verify the decompressed data matches the original
	if !bytes.Equal(decompressed, originalData) {
		t.Errorf("Decompressed data doesn't match original. Expected: %s, Got: %s", originalData, decompressed)
	}
}

func TestDecompressWithZstdEmptyData(t *testing.T) {
	// Test with empty data
	originalData := []byte{}

	// Compress the empty data first
	compressed, err := compressWithZstd(originalData)
	if err != nil {
		t.Fatalf("Failed to compress empty data: %v", err)
	}

	// Decompress the data
	decompressed, err := decompressWithZstd(compressed)
	if err != nil {
		t.Fatalf("Failed to decompress empty data: %v", err)
	}

	// Verify the decompressed data is empty
	if len(decompressed) != 0 {
		t.Errorf("Expected empty decompressed data, got %d bytes", len(decompressed))
	}
}

func TestDecompressWithZstdInvalidData(t *testing.T) {
	// Test with invalid compressed data
	invalidData := []byte("this is not valid Zstd compressed data")

	_, err := decompressWithZstd(invalidData)
	if err == nil {
		t.Error("Expected error when decompressing invalid Zstd data")
	}
}

func TestEncryptDecryptRoundTrip(t *testing.T) {
	kv, cleanup := setupTestKVForDecryption(t)
	defer cleanup()

	// Test cases with different content sizes and configurations
	testCases := []struct {
		name        string
		contentSize int
		shards      uint8
		parity      uint8
	}{
		{"tiny", 5, 2, 1},
		{"small", 50, 3, 2},
		{"medium", 500, 4, 2},
		{"large", 5000, 5, 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create test content
			content := make([]byte, tc.contentSize)
			for i := range content {
				content[i] = byte(i % 256)
			}

			originalData := Data{
				Key:                     hash.HashString("roundtrip-" + tc.name),
				Content:                 content,
				Parent:                  hash.HashString("parent"),
				Children:                []hash.Hash{hash.HashString("child1")},
				ReedSolomonShards:       tc.shards,
				ReedSolomonParityShards: tc.parity,
			}

			// Encode
			encoded, err := kv.encodeDataPipeline(originalData)
			if err != nil {
				t.Fatalf("Failed to encode %s: %v", tc.name, err)
			}

			// Decode
			decoded, err := kv.decodeDataPipeline(encoded)
			if err != nil {
				t.Fatalf("Failed to decode %s: %v", tc.name, err)
			}

			// Verify round-trip integrity
			if !bytes.Equal(decoded.Content, originalData.Content) {
				t.Errorf("Round-trip failed for %s: content mismatch", tc.name)
			}
			if decoded.Key != originalData.Key {
				t.Errorf("Round-trip failed for %s: key mismatch", tc.name)
			}
		})
	}
}

func TestDecodeDataPipelineWithCorruptedChunk(t *testing.T) {
	kv, cleanup := setupTestKVForDecryption(t)
	defer cleanup()

	// Create and encode test data with enough parity to handle corruption
	originalData := Data{
		Key:                     hash.HashString("test-corrupt"),
		Content:                 []byte("This content will be corrupted during testing."),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 3, // More parity shards for better error correction
	}

	encoded, err := kv.encodeDataPipeline(originalData)
	if err != nil {
		t.Fatalf("Failed to encode test data: %v", err)
	}

	// Corrupt a parity shard (not a data shard) to test error correction
	corruptedParityShard := false
	for i, chunk := range encoded.Shards {
		if chunk.ReedSolomonIndex >= originalData.ReedSolomonShards { // This is a parity shard
			encoded.Shards[i].ChunkContent[0] ^= 0xFF // Flip bits
			corruptedParityShard = true
			break
		}
	}

	if !corruptedParityShard {
		t.Skip("No parity shard found to corrupt")
	}

	// Try to decode - should still work due to Reed-Solomon error correction
	decoded, err := kv.decodeDataPipeline(encoded)
	if err != nil {
		t.Fatalf("Failed to decode data with corrupted parity shard: %v", err)
	}

	// Verify the content is still correct (Reed-Solomon should have corrected the error)
	if !bytes.Equal(decoded.Content, originalData.Content) {
		t.Error("Reed-Solomon failed to correct corrupted parity shard")
	}
}
