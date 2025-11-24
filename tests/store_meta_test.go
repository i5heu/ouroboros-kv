package ouroboroskv__test

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"testing"

	_ "github.com/i5heu/ouroboros-kv/internal/testutil"

	crypt "github.com/i5heu/ouroboros-crypt"
	ouroboroskv "github.com/i5heu/ouroboros-kv"
)

func TestMetaFieldBasic(t *testing.T) {
	store, cleanup := newMetaTestStore(t)
	defer cleanup()

	data := ouroboroskv.Data{
		Content:        []byte("content"),
		Meta:           []byte("metadata-value"),
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

	if !bytes.Equal(read.Meta, data.Meta) {
		t.Fatalf("Meta mismatch: expected %q, got %q", data.Meta, read.Meta)
	}
}

func TestMetaFieldEmpty(t *testing.T) {
	store, cleanup := newMetaTestStore(t)
	defer cleanup()

	data := ouroboroskv.Data{
		Content:        []byte("content-no-meta"),
		Meta:           []byte{},
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

	if len(read.Meta) != 0 {
		t.Fatalf("Expected empty Meta, got %d bytes", len(read.Meta))
	}
}

func TestMetaFieldNil(t *testing.T) {
	store, cleanup := newMetaTestStore(t)
	defer cleanup()

	data := ouroboroskv.Data{
		Content:        []byte("content-nil-meta"),
		Meta:           nil,
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

	// nil and empty slice may be treated the same
	if len(read.Meta) != 0 {
		t.Fatalf("Expected nil/empty Meta, got %d bytes", len(read.Meta))
	}
}

func TestMetaFieldLarge(t *testing.T) {
	store, cleanup := newMetaTestStore(t)
	defer cleanup()

	// Create large meta (100KB)
	largeMeta := make([]byte, 100*1024)
	for i := range largeMeta {
		largeMeta[i] = byte(i % 256)
	}

	data := ouroboroskv.Data{
		Content:        []byte("small-content"),
		Meta:           largeMeta,
		RSDataSlices:   3,
		RSParitySlices: 2,
	}

	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData with large meta failed: %v", err)
	}

	read, err := store.ReadData(key)
	if err != nil {
		t.Fatalf("ReadData failed: %v", err)
	}

	if !bytes.Equal(read.Meta, largeMeta) {
		t.Fatalf("Large meta mismatch: got %d bytes, expected %d", len(read.Meta), len(largeMeta))
	}
}

func TestMetaDifferentKeyGeneration(t *testing.T) {
	store, cleanup := newMetaTestStore(t)
	defer cleanup()

	sameContent := []byte("identical content")
	data1 := ouroboroskv.Data{
		Content:        sameContent,
		Meta:           []byte("meta-one"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	data2 := ouroboroskv.Data{
		Content:        sameContent,
		Meta:           []byte("meta-two"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}

	key1, err := store.WriteData(data1)
	if err != nil {
		t.Fatalf("WriteData(data1) failed: %v", err)
	}

	key2, err := store.WriteData(data2)
	if err != nil {
		t.Fatalf("WriteData(data2) failed: %v", err)
	}

	// Keys should be different because meta is different
	if key1 == key2 {
		t.Fatalf("Same content with different Meta should produce different keys")
	}

	// Verify metas
	read1, _ := store.ReadData(key1)
	read2, _ := store.ReadData(key2)

	if !bytes.Equal(read1.Meta, data1.Meta) {
		t.Fatalf("Meta1 mismatch")
	}
	if !bytes.Equal(read2.Meta, data2.Meta) {
		t.Fatalf("Meta2 mismatch")
	}
}

func TestMetaBinaryData(t *testing.T) {
	store, cleanup := newMetaTestStore(t)
	defer cleanup()

	// Binary meta with null bytes and special characters
	binaryMeta := []byte{0x00, 0x01, 0xFF, 0x7F, 0x80, 0xDE, 0xAD, 0xBE, 0xEF}

	data := ouroboroskv.Data{
		Content:        []byte("binary-meta-content"),
		Meta:           binaryMeta,
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

	if !bytes.Equal(read.Meta, binaryMeta) {
		t.Fatalf("Binary meta mismatch: expected %v, got %v", binaryMeta, read.Meta)
	}
}

func TestMetaJSONData(t *testing.T) {
	store, cleanup := newMetaTestStore(t)
	defer cleanup()

	jsonMeta := []byte(`{"type":"document","version":2,"tags":["test","example"],"timestamp":"2024-01-01T00:00:00Z"}`)

	data := ouroboroskv.Data{
		Content:        []byte("json-meta-content"),
		Meta:           jsonMeta,
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

	if !bytes.Equal(read.Meta, jsonMeta) {
		t.Fatalf("JSON meta mismatch: expected %s, got %s", jsonMeta, read.Meta)
	}
}

func TestMetaBatchWrite(t *testing.T) {
	store, cleanup := newMetaTestStore(t)
	defer cleanup()

	batch := []ouroboroskv.Data{
		{
			Content:        []byte("batch1"),
			Meta:           []byte("meta1"),
			RSDataSlices:   3,
			RSParitySlices: 2,
		},
		{
			Content:        []byte("batch2"),
			Meta:           []byte("meta2"),
			RSDataSlices:   3,
			RSParitySlices: 2,
		},
		{
			Content:        []byte("batch3"),
			Meta:           nil,
			RSDataSlices:   3,
			RSParitySlices: 2,
		},
	}

	keys, err := store.BatchWriteData(batch)
	if err != nil {
		t.Fatalf("BatchWriteData failed: %v", err)
	}

	if len(keys) != 3 {
		t.Fatalf("Expected 3 keys, got %d", len(keys))
	}

	// Verify each meta
	for i, key := range keys {
		read, err := store.ReadData(key)
		if err != nil {
			t.Fatalf("ReadData(%d) failed: %v", i, err)
		}
		if !bytes.Equal(read.Meta, batch[i].Meta) {
			t.Fatalf("Meta mismatch for batch item %d", i)
		}
	}
}

func TestMetaWithRelationships(t *testing.T) {
	store, cleanup := newMetaTestStore(t)
	defer cleanup()

	// Parent with meta
	parentData := ouroboroskv.Data{
		Content:        []byte("parent-content"),
		Meta:           []byte("parent-metadata"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	parentKey, err := store.WriteData(parentData)
	if err != nil {
		t.Fatalf("WriteData(parent) failed: %v", err)
	}

	// Child with different meta
	childData := ouroboroskv.Data{
		Content:        []byte("child-content"),
		Meta:           []byte("child-metadata"),
		Parent:         parentKey,
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	childKey, err := store.WriteData(childData)
	if err != nil {
		t.Fatalf("WriteData(child) failed: %v", err)
	}

	// Verify both have correct meta
	readParent, _ := store.ReadData(parentKey)
	readChild, _ := store.ReadData(childKey)

	if !bytes.Equal(readParent.Meta, parentData.Meta) {
		t.Fatalf("Parent meta mismatch")
	}
	if !bytes.Equal(readChild.Meta, childData.Meta) {
		t.Fatalf("Child meta mismatch")
	}
}

func TestMetaVersusNoMeta(t *testing.T) {
	store, cleanup := newMetaTestStore(t)
	defer cleanup()

	content := []byte("same content")
	dataWithMeta := ouroboroskv.Data{
		Content:        content,
		Meta:           []byte("some-meta"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	dataWithoutMeta := ouroboroskv.Data{
		Content:        content,
		Meta:           nil,
		RSDataSlices:   3,
		RSParitySlices: 2,
	}

	key1, _ := store.WriteData(dataWithMeta)
	key2, _ := store.WriteData(dataWithoutMeta)

	// Should be different keys
	if key1 == key2 {
		t.Fatalf("Data with and without Meta should have different keys")
	}
}

func TestMetaUTF8Characters(t *testing.T) {
	store, cleanup := newMetaTestStore(t)
	defer cleanup()

	utf8Meta := []byte("Hello 世界 🌍 Здравствуй мир")

	data := ouroboroskv.Data{
		Content:        []byte("utf8-test"),
		Meta:           utf8Meta,
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

	if !bytes.Equal(read.Meta, utf8Meta) {
		t.Fatalf("UTF-8 meta mismatch: expected %s, got %s", utf8Meta, read.Meta)
	}
}

func TestMetaWithGetDataInfo(t *testing.T) {
	store, cleanup := newMetaTestStore(t)
	defer cleanup()

	meta := []byte("test-metadata")
	data := ouroboroskv.Data{
		Content:        []byte("info-test-content"),
		Meta:           meta,
		RSDataSlices:   3,
		RSParitySlices: 2,
	}

	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	info, err := store.GetDataInfo(key)
	if err != nil {
		t.Fatalf("GetDataInfo failed: %v", err)
	}

	// Verify basic info fields
	if info.Key != key {
		t.Fatalf("Key mismatch in DataInfo")
	}
	if info.ClearTextSize == 0 {
		t.Fatalf("ClearTextSize should be non-zero")
	}
}

func newMetaTestStore(t *testing.T) (ouroboroskv.Store, func()) {
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

func newMetaTestData(label string) ouroboroskv.Data {
	return ouroboroskv.Data{
		Content:        []byte(fmt.Sprintf("content-%s", label)),
		Meta:           []byte(fmt.Sprintf("meta-%s", label)),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
}
