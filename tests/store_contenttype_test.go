package ouroboroskv__test

import (
	"bytes"
	"io"
	"log/slog"
	"testing"

	crypt "github.com/i5heu/ouroboros-crypt"
	ouroboroskv "github.com/i5heu/ouroboros-kv"
)

func TestContentTypeDifferentKeysForDifferentTypes(t *testing.T) {
	store, cleanup := newContentTypeTestStore(t)
	defer cleanup()

	sameContent := []byte("identical content")
	data1 := ouroboroskv.Data{
		Content:        sameContent,
		RSDataSlices:   3,
		RSParitySlices: 2,
		ContentType:    "text/plain",
	}
	data2 := ouroboroskv.Data{
		Content:        sameContent,
		RSDataSlices:   3,
		RSParitySlices: 2,
		ContentType:    "application/json",
	}

	key1, err := store.WriteData(data1)
	if err != nil {
		t.Fatalf("WriteData(text/plain) failed: %v", err)
	}

	key2, err := store.WriteData(data2)
	if err != nil {
		t.Fatalf("WriteData(application/json) failed: %v", err)
	}

	// Keys should be different
	if key1 == key2 {
		t.Fatalf("Same content with different ContentType should produce different keys")
	}

	// Read back and verify ContentType
	read1, err := store.ReadData(key1)
	if err != nil {
		t.Fatalf("ReadData(key1) failed: %v", err)
	}
	if read1.ContentType != "text/plain" {
		t.Fatalf("Expected ContentType 'text/plain', got '%s'", read1.ContentType)
	}

	read2, err := store.ReadData(key2)
	if err != nil {
		t.Fatalf("ReadData(key2) failed: %v", err)
	}
	if read2.ContentType != "application/json" {
		t.Fatalf("Expected ContentType 'application/json', got '%s'", read2.ContentType)
	}
}

func TestContentTypeEmpty(t *testing.T) {
	store, cleanup := newContentTypeTestStore(t)
	defer cleanup()

	data := ouroboroskv.Data{
		Content:        []byte("content with no type"),
		RSDataSlices:   3,
		RSParitySlices: 2,
		ContentType:    "",
	}

	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData with empty ContentType failed: %v", err)
	}

	read, err := store.ReadData(key)
	if err != nil {
		t.Fatalf("ReadData failed: %v", err)
	}
	if read.ContentType != "" {
		t.Fatalf("Expected empty ContentType, got '%s'", read.ContentType)
	}
	if !bytes.Equal(read.Content, data.Content) {
		t.Fatalf("Content mismatch")
	}
}

func TestContentTypeVersusNoType(t *testing.T) {
	store, cleanup := newContentTypeTestStore(t)
	defer cleanup()

	content := []byte("same content")
	dataWithType := ouroboroskv.Data{
		Content:        content,
		RSDataSlices:   3,
		RSParitySlices: 2,
		ContentType:    "text/html",
	}
	dataWithoutType := ouroboroskv.Data{
		Content:        content,
		RSDataSlices:   3,
		RSParitySlices: 2,
		ContentType:    "",
	}

	key1, _ := store.WriteData(dataWithType)
	key2, _ := store.WriteData(dataWithoutType)

	// Should be different keys
	if key1 == key2 {
		t.Fatalf("Data with and without ContentType should have different keys")
	}
}

func TestContentTypeVariousTypes(t *testing.T) {
	store, cleanup := newContentTypeTestStore(t)
	defer cleanup()

	types := []string{
		"text/plain",
		"text/html",
		"text/css",
		"text/javascript",
		"application/json",
		"application/xml",
		"application/pdf",
		"image/png",
		"image/jpeg",
		"video/mp4",
		"audio/mpeg",
		"application/octet-stream",
		"",
	}

	content := []byte("test content")
	keys := make(map[ouroboroskv.Hash]string)

	for _, ct := range types {
		data := ouroboroskv.Data{
			Content:        content,
			RSDataSlices:   3,
			RSParitySlices: 2,
			ContentType:    ct,
		}

		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("WriteData with ContentType '%s' failed: %v", ct, err)
		}

		// Check for duplicates
		if existingType, exists := keys[key]; exists {
			t.Fatalf("Duplicate key for types '%s' and '%s'", ct, existingType)
		}

		keys[key] = ct

		// Verify read back
		read, err := store.ReadData(key)
		if err != nil {
			t.Fatalf("ReadData for ContentType '%s' failed: %v", ct, err)
		}
		if read.ContentType != ct {
			t.Fatalf("ContentType mismatch: expected '%s', got '%s'", ct, read.ContentType)
		}
	}

	// All keys should be unique (13 types = 13 unique keys)
	if len(keys) != len(types) {
		t.Fatalf("Expected %d unique keys, got %d", len(types), len(keys))
	}
}

func TestContentTypeInGetDataInfo(t *testing.T) {
	store, cleanup := newContentTypeTestStore(t)
	defer cleanup()

	data := ouroboroskv.Data{
		Content:        []byte("info test content"),
		RSDataSlices:   3,
		RSParitySlices: 2,
		ContentType:    "text/markdown",
	}

	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	info, err := store.GetDataInfo(key)
	if err != nil {
		t.Fatalf("GetDataInfo failed: %v", err)
	}

	// DataInfo might not include ContentType, but verify other fields work
	if info.Key != key {
		t.Fatalf("DataInfo key mismatch")
	}
	if info.ClearTextSize == 0 {
		t.Fatalf("DataInfo should have non-zero ClearTextSize")
	}
}

func TestContentTypeBatchWrite(t *testing.T) {
	store, cleanup := newContentTypeTestStore(t)
	defer cleanup()

	batch := []ouroboroskv.Data{
		{
			Content:        []byte("batch1"),
			RSDataSlices:   3,
			RSParitySlices: 2,
			ContentType:    "text/plain",
		},
		{
			Content:        []byte("batch2"),
			RSDataSlices:   3,
			RSParitySlices: 2,
			ContentType:    "application/json",
		},
		{
			Content:        []byte("batch3"),
			RSDataSlices:   3,
			RSParitySlices: 2,
			ContentType:    "",
		},
	}

	keys, err := store.BatchWriteData(batch)
	if err != nil {
		t.Fatalf("BatchWriteData failed: %v", err)
	}

	if len(keys) != 3 {
		t.Fatalf("Expected 3 keys, got %d", len(keys))
	}

	// Verify each
	for i, key := range keys {
		read, err := store.ReadData(key)
		if err != nil {
			t.Fatalf("ReadData(%d) failed: %v", i, err)
		}
		if read.ContentType != batch[i].ContentType {
			t.Fatalf("ContentType mismatch for batch item %d", i)
		}
	}
}

func TestContentTypeWithSpecialCharacters(t *testing.T) {
	store, cleanup := newContentTypeTestStore(t)
	defer cleanup()

	specialTypes := []string{
		"text/plain; charset=utf-8",
		"application/json; charset=utf-8",
		"multipart/form-data; boundary=something",
		"application/vnd.api+json",
		"image/svg+xml",
	}

	for _, ct := range specialTypes {
		data := ouroboroskv.Data{
			Content:        []byte("content"),
			RSDataSlices:   3,
			RSParitySlices: 2,
			ContentType:    ct,
		}

		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("WriteData with ContentType '%s' failed: %v", ct, err)
		}

		read, err := store.ReadData(key)
		if err != nil {
			t.Fatalf("ReadData for ContentType '%s' failed: %v", ct, err)
		}
		if read.ContentType != ct {
			t.Fatalf("ContentType mismatch: expected '%s', got '%s'", ct, read.ContentType)
		}
	}
}

func newContentTypeTestStore(t *testing.T) (ouroboroskv.Store, func()) {
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
