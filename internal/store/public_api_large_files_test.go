package store

import (
	"bytes"
	"crypto/rand"
	"os"
	"testing"

	"github.com/i5heu/ouroboros-kv/internal/testutil"
	_ "github.com/i5heu/ouroboros-kv/internal/testutil"

	"log/slog"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	"github.com/i5heu/ouroboros-kv/internal/types"
	"github.com/i5heu/ouroboros-kv/pkg/config"
)

func TestPublicAPILargeFiles(t *testing.T) {
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

	sizes := []struct {
		name string
		size int64
	}{
		{"1MB", 1 * 1024 * 1024},
		{"5MB", 5 * 1024 * 1024},
		{"10MB", 10 * 1024 * 1024},
		{"50MB", 50 * 1024 * 1024},
		{"100MB", 100 * 1024 * 1024},
		{"500MB", 500 * 1024 * 1024},
	}

	for _, tc := range sizes {
		t.Run(tc.name, func(t *testing.T) {
			if tc.size > 5*1024*1024 {
				testutil.RequireLong(t)
			}

			t.Logf("Generating %s data...", tc.name)
			originalData := make([]byte, tc.size)
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

			t.Logf("Writing %s data...", tc.name)
			key, err := kv.WriteData(data)
			if err != nil {
				t.Fatalf("WriteData failed: %v", err)
			}

			t.Logf("Reading %s data back...", tc.name)
			retrievedData, err := kv.ReadData(key)
			if err != nil {
				t.Fatalf("ReadData failed: %v", err)
			}

			t.Logf("Retrieved content size: %d bytes (expected: %d bytes)", len(retrievedData.Content), len(originalData))

			if !bytes.Equal(originalData, retrievedData.Content) {
				t.Fatalf("Content mismatch: got %d bytes instead of %d", len(retrievedData.Content), len(originalData))
			}

			t.Logf("✅ %s public API round-trip test passed!", tc.name)
		})
	}
}
