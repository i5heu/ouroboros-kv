package store

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	pb "github.com/i5heu/ouroboros-kv/proto"
	"google.golang.org/protobuf/proto"
)

func TestValidateSuccess(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	data := createTestStorageData()
	key, err := kv.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	if err := kv.Validate(key); err != nil {
		t.Fatalf("Validate reported unexpected error: %v", err)
	}
}

func TestValidateDetectsCorruption(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	data := createTestStorageData()
	key, err := kv.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	if err := kv.badgerDB.Update(func(txn *badger.Txn) error {
		metadata, err := kv.loadMetadata(txn, key)
		if err != nil {
			return err
		}
		if len(metadata.ChunkHashes) == 0 {
			return fmt.Errorf("no chunk hashes found for key %x", key)
		}
		sliceKey := fmt.Sprintf("%s%x_%d", SLICE_PREFIX, metadata.ChunkHashes[0], 0)
		item, err := txn.Get([]byte(sliceKey))
		if err != nil {
			return err
		}
		var protoData []byte
		if err := item.Value(func(val []byte) error {
			protoData = append([]byte(nil), val...)
			return nil
		}); err != nil {
			return err
		}
		sliceProto := &pb.SliceRecordProto{}
		if err := proto.Unmarshal(protoData, sliceProto); err != nil {
			return err
		}
		if len(sliceProto.Payload) == 0 {
			sliceProto.Payload = []byte{0xFF}
		} else {
			sliceProto.Payload[0] ^= 0xFF
		}
		updated, err := proto.Marshal(sliceProto)
		if err != nil {
			return err
		}
		return txn.Set([]byte(sliceKey), updated)
	}); err != nil {
		t.Fatalf("failed to corrupt stored chunk: %v", err)
	}

	if err := kv.Validate(key); err == nil {
		t.Fatalf("Validate should have reported corruption")
	}

	results, err := kv.ValidateAll()
	if err != nil {
		t.Fatalf("ValidateAll failed: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 validation result, got %d", len(results))
	}
	if results[0].Err == nil {
		t.Fatalf("ValidateAll did not surface corruption")
	}
}
