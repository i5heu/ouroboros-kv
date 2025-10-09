package ouroboroskv

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"log/slog"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLargeFileRoundTrip tests the complete pipeline with very large files
func TestLargeFileRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large file round trip in short mode")
	}

	sizes := []struct {
		name string
		size int64
	}{
		{"1MB", 1 * 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
		{"50MB", 50 * 1024 * 1024},
		{"100MB", 100 * 1024 * 1024},
		{"500MB", 500 * 1024 * 1024},
	}

	for _, tc := range sizes {
		t.Run(tc.name, func(t *testing.T) {
			// Create temporary directory for this test
			tempDir, err := os.MkdirTemp("", "ouroboros-kv-large-test-")
			require.NoError(t, err)
			defer os.RemoveAll(tempDir)

			// Initialize crypto
			cryptInstance := crypt.New()

			// Create KV config
			config := &Config{
				Paths:            []string{tempDir},
				MinimumFreeSpace: 1,
				Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
			}

			// Initialize KV store
			kv, err := Init(cryptInstance, config)
			require.NoError(t, err)
			defer kv.Close()

			// Generate large random data
			t.Logf("Generating %s of random data...", tc.name)
			originalData := make([]byte, tc.size)
			_, err = rand.Read(originalData)
			require.NoError(t, err)

			// Create Data structure
			metadata := []byte(fmt.Sprintf("metadata-%s", tc.name))

			data := applyTestDefaults(Data{
				Content:                 originalData,
				MetaData:                metadata,
				Parent:                  hash.Hash{},   // Empty parent
				Children:                []hash.Hash{}, // No children
				ReedSolomonShards:       8,
				ReedSolomonParityShards: 4,
			})

			expectedKey := expectedKeyForData(data)

			// Test WriteData
			t.Logf("Writing %s of data...", tc.name)
			key, err := kv.WriteData(data)
			require.NoError(t, err, "Failed to write large data")
			require.Equal(t, expectedKey, key, "Generated key should match canonical hash")

			// Test ReadData
			t.Logf("Reading %s of data back...", tc.name)
			retrievedData, err := kv.ReadData(key)
			require.NoError(t, err, "Failed to read large data")

			// Verify data integrity
			t.Logf("Verifying %s of data integrity...", tc.name)
			assert.Equal(t, key, retrievedData.Key, "Keys should match")
			assert.Equal(t, data.Parent, retrievedData.Parent, "Parents should match")
			assert.Equal(t, data.MetaData, retrievedData.MetaData, "Metadata should match")
			assert.Equal(t, data.Created, retrievedData.Created, "Created timestamp should match")
			assert.Empty(t, retrievedData.Aliases, "Aliases should be empty by default")
			// Handle empty slice vs nil slice comparison
			if len(data.Children) == 0 && len(retrievedData.Children) == 0 {
				// Both are empty, this is fine
			} else {
				assert.Equal(t, data.Children, retrievedData.Children, "Children should match")
			}
			assert.Equal(t, data.ReedSolomonShards, retrievedData.ReedSolomonShards, "ReedSolomonShards should match")
			assert.Equal(t, data.ReedSolomonParityShards, retrievedData.ReedSolomonParityShards, "ReedSolomonParityShards should match")

			// Most importantly - verify content integrity
			if !bytes.Equal(originalData, retrievedData.Content) {
				t.Errorf("Content mismatch for %s file:", tc.name)
				t.Errorf("Original size: %d, Retrieved size: %d", len(originalData), len(retrievedData.Content))

				// Find first difference
				minLen := len(originalData)
				if len(retrievedData.Content) < minLen {
					minLen = len(retrievedData.Content)
				}

				for i := 0; i < minLen; i++ {
					if originalData[i] != retrievedData.Content[i] {
						t.Errorf("First difference at byte %d: original=0x%02x, retrieved=0x%02x", i, originalData[i], retrievedData.Content[i])
						break
					}
				}

				if len(originalData) != len(retrievedData.Content) {
					t.Errorf("Size difference: original=%d, retrieved=%d", len(originalData), len(retrievedData.Content))
				}

				t.FailNow()
			}

			t.Logf("✅ %s file test passed successfully", tc.name)
		})
	}
}

// TestEncodingDecodingPipelineWithLargeFiles tests the encoding and decoding pipelines specifically
func TestEncodingDecodingPipelineWithLargeFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large file pipeline test in short mode")
	}

	sizes := []struct {
		name string
		size int64
	}{
		{"10MB", 10 * 1024 * 1024},
		{"50MB", 50 * 1024 * 1024},
		{"100MB", 100 * 1024 * 1024},
	}

	for _, tc := range sizes {
		t.Run(tc.name, func(t *testing.T) {
			// Create temporary directory for this test
			tempDir, err := os.MkdirTemp("", "ouroboros-kv-pipeline-test-")
			require.NoError(t, err)
			defer os.RemoveAll(tempDir)

			// Initialize crypto
			cryptInstance := crypt.New()

			// Create KV config
			config := &Config{
				Paths:            []string{tempDir},
				MinimumFreeSpace: 1,
				Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
			}

			// Initialize KV store
			kv, err := Init(cryptInstance, config)
			require.NoError(t, err)
			defer kv.Close()

			// Generate large random data
			t.Logf("Testing encoding/decoding pipeline with %s of random data...", tc.name)
			originalData := make([]byte, tc.size)
			_, err = rand.Read(originalData)
			require.NoError(t, err)

			// Create Data structure
			data := applyTestDefaults(Data{
				MetaData:                []byte(fmt.Sprintf("metadata-%s", tc.name)),
				Content:                 originalData,
				Parent:                  hash.Hash{},
				Children:                []hash.Hash{},
				ReedSolomonShards:       8,
				ReedSolomonParityShards: 4,
			})
			expectedKey := expectedKeyForData(data)
			data.Key = expectedKey

			// Test encoding pipeline
			t.Logf("Testing encoding pipeline...")
			encoded, err := kv.encodeDataPipeline(data)
			require.NoError(t, err, "Encoding pipeline failed")

			// Verify encoded data structure
			assert.Equal(t, data.Key, encoded.Key, "Key should be preserved in encoding")
			assert.Equal(t, data.Parent, encoded.Parent, "Parent should be preserved in encoding")
			assert.Equal(t, data.Children, encoded.Children, "Children should be preserved in encoding")
			assert.Equal(t, data.Created, encoded.Created, "Created should be preserved in encoding")
			assert.Equal(t, data.Aliases, encoded.Aliases, "Aliases should be preserved in encoding")
			assert.NotEmpty(t, encoded.Shards, "Encoded data should have chunks")

			t.Logf("Encoded into %d chunks", len(encoded.Shards))

			// Test decoding pipeline
			t.Logf("Testing decoding pipeline...")
			decoded, err := kv.decodeDataPipeline(encoded)
			require.NoError(t, err, "Decoding pipeline failed")

			// Verify decoded data matches original
			assert.Equal(t, data.Key, decoded.Key, "Key should match after decode")
			assert.Equal(t, data.Parent, decoded.Parent, "Parent should match after decode")
			assert.Equal(t, data.Children, decoded.Children, "Children should match after decode")
			assert.Equal(t, data.Created, decoded.Created, "Created should match after decode")
			assert.Equal(t, data.Aliases, decoded.Aliases, "Aliases should match after decode")
			assert.Equal(t, data.MetaData, decoded.MetaData, "Metadata should match after decode")
			assert.Equal(t, data.ReedSolomonShards, decoded.ReedSolomonShards, "ReedSolomonShards should match after decode")
			assert.Equal(t, data.ReedSolomonParityShards, decoded.ReedSolomonParityShards, "ReedSolomonParityShards should match after decode")

			// Most importantly - verify content integrity
			if !bytes.Equal(originalData, decoded.Content) {
				t.Errorf("Content mismatch in pipeline test for %s file:", tc.name)
				t.Errorf("Original size: %d, Decoded size: %d", len(originalData), len(decoded.Content))

				// Find first difference
				minLen := len(originalData)
				if len(decoded.Content) < minLen {
					minLen = len(decoded.Content)
				}

				for i := 0; i < minLen; i++ {
					if originalData[i] != decoded.Content[i] {
						t.Errorf("First difference at byte %d: original=0x%02x, decoded=0x%02x", i, originalData[i], decoded.Content[i])
						break
					}
				}

				t.FailNow()
			}

			t.Logf("✅ %s pipeline test passed successfully", tc.name)
		})
	}
}

// TestVirtualFileStorageWithCLI tests the CLI functionality with virtual large files
func TestVirtualFileStorageWithCLI(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping CLI virtual file test in short mode")
	}

	sizes := []struct {
		name string
		size int64
	}{
		{"1MB", 1 * 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
		{"50MB", 50 * 1024 * 1024},
	}

	for _, tc := range sizes {
		t.Run(tc.name, func(t *testing.T) {
			// Create temporary directory for this test
			tempDir, err := os.MkdirTemp("", "ouroboros-kv-cli-test-")
			require.NoError(t, err)
			defer os.RemoveAll(tempDir)

			// Create a virtual file with random data
			virtualFile := filepath.Join(tempDir, fmt.Sprintf("virtual_%s.bin", tc.name))
			originalData := make([]byte, tc.size)
			_, err = rand.Read(originalData)
			require.NoError(t, err)

			err = os.WriteFile(virtualFile, originalData, 0644)
			require.NoError(t, err)

			// Initialize crypto
			cryptInstance := crypt.New()

			// Create KV config
			kvDir := filepath.Join(tempDir, "kv-data")
			config := &Config{
				Paths:            []string{kvDir},
				MinimumFreeSpace: 1,
				Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
			}

			// Initialize KV store
			kv, err := Init(cryptInstance, config)
			require.NoError(t, err)
			defer kv.Close()

			// Simulate CLI store operation
			t.Logf("Simulating CLI store for %s file...", tc.name)
			content, err := os.ReadFile(virtualFile)
			require.NoError(t, err)

			data := applyTestDefaults(Data{
				Content:                 content,
				Parent:                  hash.Hash{},
				Children:                []hash.Hash{},
				ReedSolomonShards:       8,
				ReedSolomonParityShards: 4,
			})
			expectedKey := expectedKeyForData(data)

			// Store the data
			key, err := kv.WriteData(data)
			require.NoError(t, err, "Failed to store virtual file")
			require.Equal(t, expectedKey, key, "Generated key should match canonical hash")

			// Simulate CLI restore operation
			t.Logf("Simulating CLI restore for %s file...", tc.name)
			retrievedData, err := kv.ReadData(key)
			require.NoError(t, err, "Failed to retrieve virtual file")
			require.Equal(t, key, retrievedData.Key, "Retrieved key should match")
			require.Equal(t, data.Created, retrievedData.Created, "Created timestamp should match")
			assert.Empty(t, retrievedData.Aliases, "Aliases should be empty by default")

			// Verify content integrity
			if !bytes.Equal(originalData, retrievedData.Content) {
				t.Errorf("CLI simulation failed for %s file:", tc.name)
				t.Errorf("Original size: %d, Retrieved size: %d", len(originalData), len(retrievedData.Content))
				t.FailNow()
			}

			t.Logf("✅ CLI simulation for %s file passed successfully", tc.name)
		})
	}
}
