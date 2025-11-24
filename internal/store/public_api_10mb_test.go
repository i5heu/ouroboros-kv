package store

import (
	"bytes"
	"crypto/rand"
	"os"
	"testing"

	"log/slog"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	"github.com/i5heu/ouroboros-kv/internal/types"
	"github.com/i5heu/ouroboros-kv/pkg/config"
)

func TestPublicAPI10MB(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "ouroboros-kv-test-")
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

	kv, err := Init(cryptInstance, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()

	t.Log("Generating 10MB data...")
	originalData := make([]byte, 10*1024*1024)
	_, err = rand.Read(originalData)
	if err != nil {
		t.Fatal(err)
	}

	data := types.Data{
		Content:        originalData,
		Meta:           []byte("test-metadata"),
		Parent:         hash.Hash{},
		Children:       []hash.Hash{},
		RSDataSlices:   8,
		RSParitySlices: 4,
	}

	t.Log("Writing 10MB data...")
	key, err := kv.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	t.Log("Reading 10MB data back...")
	retrievedData, err := kv.ReadData(key)
	if err != nil {
		t.Fatalf("ReadData failed: %v", err)
	}

	t.Logf("Retrieved content size: %d bytes (expected: %d bytes)", len(retrievedData.Content), len(originalData))

	if !bytes.Equal(originalData, retrievedData.Content) {
		t.Fatalf("Content mismatch: got %d bytes instead of %d", len(retrievedData.Content), len(originalData))
	}

	t.Log("✅ 10MB public API test passed!")
}
