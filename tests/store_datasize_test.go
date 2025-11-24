package ouroboroskv__test

import (
	"crypto/rand"
	"io"
	"log/slog"
	"os"
	"testing"

	_ "github.com/i5heu/ouroboros-kv/internal/testutil"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	ouroboroskv "github.com/i5heu/ouroboros-kv"
	"github.com/i5heu/ouroboros-kv/pkg/config"
)

func TestDataSizeSmall(t *testing.T) {
	store, cleanup := newSizeTestStore(t)
	defer cleanup()

	data := ouroboroskv.Data{
		Content:        []byte("small"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}

	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	read, err := store.ReadData(key)
	if err != nil {
		t.Fatalf("ReadData failed: %v", err)
	}

	if string(read.Content) != string(data.Content) {
		t.Fatalf("Content mismatch")
	}
}

func TestDataSizeLarge(t *testing.T) {
	store, cleanup := newSizeTestStore(t)
	defer cleanup()

	// Use 500KB - within system limits
	content := make([]byte, 500*1024)
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
		t.Fatalf("WriteData failed: %v", err)
	}

	read, err := store.ReadData(key)
	if err != nil {
		t.Fatalf("ReadData failed: %v", err)
	}

	if len(read.Content) != 500*1024 {
		t.Fatalf("Content size mismatch: expected %d, got %d", 500*1024, len(read.Content))
	}
}

func TestDataSize10MB(t *testing.T) {
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

	kv, err := ouroboroskv.Init(cryptInstance, cfg)
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

	data := ouroboroskv.Data{
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

	if len(retrievedData.Content) != 10*1024*1024 {
		t.Fatalf("Content size mismatch: expected %d, got %d", 10*1024*1024, len(retrievedData.Content))
	}

	t.Log("✅ 10MB test passed!")
}

func newSizeTestStore(t *testing.T) (ouroboroskv.Store, func()) {
	t.Helper()
	tempDir := t.TempDir()

	cfg := &ouroboroskv.Config{
		Paths:            []string{tempDir},
		MinimumFreeSpace: 0,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	kv, err := ouroboroskv.Init(crypt.New(), cfg)
	if err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	cleanup := func() {
		if err := kv.Close(); err != nil {
			t.Fatalf("Close failed: %v", err)
		}
	}

	return kv, cleanup
}
