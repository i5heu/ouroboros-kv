package store

import (
	"testing"
	"time"

	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	"github.com/i5heu/ouroboros-kv/internal/types"
)

func TestWriteDataRejectsNonZeroKey(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	data := types.Data{
		Key:            hash.HashString("preset"),
		Meta:           []byte("m"),
		Content:        []byte("c"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}

	if _, err := kv.WriteData(data); err == nil {
		t.Fatalf("expected error when data.Key is preset")
	}
}

func TestWriteDataPopulatesCreatedAndKey(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	startUnix := time.Now().Unix()
	original := types.Data{
		Meta:           []byte("meta"),
		Content:        []byte("content"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}

	key, err := kv.WriteData(original)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	stored, err := kv.ReadData(key)
	if err != nil {
		t.Fatalf("ReadData failed: %v", err)
	}

	if stored.Created == 0 {
		t.Fatalf("expected Created to be populated")
	}

	if stored.Created < startUnix {
		t.Fatalf("Created timestamp %d should be set during WriteData", stored.Created)
	}

	nowUnix := time.Now().Unix()
	if stored.Created > nowUnix {
		t.Fatalf("Created timestamp %d is in the future (now=%d)", stored.Created, nowUnix)
	}

	if key == computeDataKey(original) {
		t.Fatalf("expected computed key to incorporate Created timestamp")
	}

	if expected := computeDataKey(stored); key != expected {
		t.Fatalf("computed key mismatch: expected %x got %x", expected, key)
	}
}
