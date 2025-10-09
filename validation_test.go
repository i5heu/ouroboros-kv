package ouroboroskv

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	pb "github.com/i5heu/ouroboros-kv/proto"
	"google.golang.org/protobuf/proto"
)

func TestValidateKeySuccess(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	data := createTestStorageData()
	key, err := kv.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	if err := kv.ValidateKey(key); err != nil {
		t.Fatalf("ValidateKey reported unexpected error: %v", err)
	}
}

func TestValidateKeyDetectsCorruption(t *testing.T) {
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
		if len(metadata.ShardHashes) == 0 {
			return fmt.Errorf("no shard hashes found for key %x", key)
		}
		chunkKey := fmt.Sprintf("%s%x_%d", CHUNK_PREFIX, metadata.ShardHashes[0][:], 0)
		item, err := txn.Get([]byte(chunkKey))
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
		chunkProto := &pb.KvDataShardProto{}
		if err := proto.Unmarshal(protoData, chunkProto); err != nil {
			return err
		}
		if len(chunkProto.ChunkContent) == 0 {
			chunkProto.ChunkContent = []byte{0xFF}
		} else {
			chunkProto.ChunkContent[0] ^= 0xFF
		}
		updated, err := proto.Marshal(chunkProto)
		if err != nil {
			return err
		}
		return txn.Set([]byte(chunkKey), updated)
	}); err != nil {
		t.Fatalf("failed to corrupt stored chunk: %v", err)
	}

	if err := kv.ValidateKey(key); err == nil {
		t.Fatalf("ValidateKey should have reported corruption")
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
