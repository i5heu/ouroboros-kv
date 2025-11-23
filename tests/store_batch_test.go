package ouroboroskv__test

import (
	"fmt"
	"io"
	"log/slog"
	"testing"

	crypt "github.com/i5heu/ouroboros-crypt"
	ouroboroskv "github.com/i5heu/ouroboros-kv"
)

func TestBatchWriteEmpty(t *testing.T) {
	store, cleanup := newBatchTestStore(t)
	defer cleanup()

	keys, err := store.BatchWriteData([]ouroboroskv.Data{})
	if err != nil {
		t.Fatalf("BatchWriteData with empty batch failed: %v", err)
	}

	if len(keys) != 0 {
		t.Fatalf("Expected 0 keys for empty batch, got %d", len(keys))
	}
}

func TestBatchWriteLarge(t *testing.T) {
	store, cleanup := newBatchTestStore(t)
	defer cleanup()

	batch := make([]ouroboroskv.Data, 100)
	for i := range batch {
		batch[i] = ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("batch-item-%d", i)),
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
	}

	keys, err := store.BatchWriteData(batch)
	if err != nil {
		t.Fatalf("BatchWriteData with 100 items failed: %v", err)
	}

	if len(keys) != 100 {
		t.Fatalf("Expected 100 keys, got %d", len(keys))
	}

	for i, key := range keys {
		data, err := store.ReadData(key)
		if err != nil {
			t.Fatalf("ReadData(%d) failed: %v", i, err)
		}
		expected := fmt.Sprintf("batch-item-%d", i)
		if string(data.Content) != expected {
			t.Fatalf("Content mismatch at %d", i)
		}
	}
}

func TestBatchWriteIdenticalData(t *testing.T) {
	store, cleanup := newBatchTestStore(t)
	defer cleanup()

	batch := make([]ouroboroskv.Data, 10)
	for i := range batch {
		batch[i] = ouroboroskv.Data{
			Content:        []byte("identical"),
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
	}

	keys, err := store.BatchWriteData(batch)
	if err != nil {
		t.Fatalf("BatchWriteData failed: %v", err)
	}

	if len(keys) != 10 {
		t.Fatalf("Expected 10 keys, got %d", len(keys))
	}

	keySet := make(map[ouroboroskv.Hash]bool)
	for _, key := range keys {
		keySet[key] = true
	}
	if len(keySet) != 10 {
		t.Fatalf("Expected 10 unique keys (aliases), got %d", len(keySet))
	}
}

func TestBatchReadData(t *testing.T) {
	store, cleanup := newBatchTestStore(t)
	defer cleanup()

	batch := []ouroboroskv.Data{
		{Content: []byte("data1"), RSDataSlices: 3, RSParitySlices: 2},
		{Content: []byte("data2"), RSDataSlices: 3, RSParitySlices: 2},
		{Content: []byte("data3"), RSDataSlices: 3, RSParitySlices: 2},
	}

	keys, err := store.BatchWriteData(batch)
	if err != nil {
		t.Fatalf("BatchWriteData failed: %v", err)
	}

	results, err := store.BatchReadData(keys)
	if err != nil {
		t.Fatalf("BatchReadData failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	for i, result := range results {
		expected := fmt.Sprintf("data%d", i+1)
		if string(result.Content) != expected {
			t.Fatalf("Result %d content mismatch", i)
		}
	}
}

func TestBatchReadEmpty(t *testing.T) {
	store, cleanup := newBatchTestStore(t)
	defer cleanup()

	results, err := store.BatchReadData([]ouroboroskv.Hash{})
	if err != nil {
		t.Fatalf("BatchReadData with empty keys failed: %v", err)
	}

	if len(results) != 0 {
		t.Fatalf("Expected 0 results for empty batch, got %d", len(results))
	}
}

func TestBatchReadNonexistentKeys(t *testing.T) {
	store, cleanup := newBatchTestStore(t)
	defer cleanup()

	fakeKeys := []ouroboroskv.Hash{
		[64]byte{1, 2, 3},
		[64]byte{4, 5, 6},
	}

	_, err := store.BatchReadData(fakeKeys)
	if err == nil {
		t.Fatalf("BatchReadData should fail for nonexistent keys")
	}
}

func newBatchTestStore(t *testing.T) (ouroboroskv.Store, func()) {
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
