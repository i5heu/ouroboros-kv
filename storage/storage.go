package storage

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/i5heu/ouroboros-crypt/hash"
	pb "github.com/i5heu/ouroboros-kv/proto"
	"google.golang.org/protobuf/proto"
)

const (
	// Key prefixes for different data types in BadgerDB
	MetadataPrefix = "meta:"
	ChunkPrefix    = "chunk:"
	ParentPrefix   = "parent:"
	ChildPrefix    = "child:"
)

type Metadata struct {
	Key         hash.Hash
	ShardHashes []hash.Hash
	Parent      hash.Hash
	Children    []hash.Hash
}

type Shard struct {
	ChunkHash               hash.Hash
	EncodedHash             hash.Hash
	ReedSolomonShards       uint8
	ReedSolomonParityShards uint8
	ReedSolomonIndex        uint8
	Size                    uint64
	OriginalSize            uint64
	EncapsulatedKey         []byte
	Nonce                   []byte
	ChunkContent            []byte
}

type KVWriter interface {
	Set(key, val []byte) error
}

func IsEmptyHash(h hash.Hash) bool {
	var empty hash.Hash
	return h == empty
}

func StoreMetadata(w KVWriter, metadata Metadata) error {
	protoMetadata := &pb.KvDataHashProto{
		Key:    metadata.Key[:],
		Parent: metadata.Parent[:],
	}
	for _, chunkHash := range metadata.ShardHashes {
		protoMetadata.ChunkHashes = append(protoMetadata.ChunkHashes, chunkHash[:])
	}
	for _, child := range metadata.Children {
		protoMetadata.Children = append(protoMetadata.Children, child[:])
	}
	data, err := proto.Marshal(protoMetadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	key := fmt.Sprintf("%s%x", MetadataPrefix, metadata.Key)
	return w.Set([]byte(key), data)
}

func StoreShard(w KVWriter, shard Shard) error {
	protoChunk := &pb.KvDataShardProto{
		ChunkHash:               shard.ChunkHash[:],
		EncodedHash:             shard.EncodedHash[:],
		ReedSolomonShards:       uint32(shard.ReedSolomonShards),
		ReedSolomonParityShards: uint32(shard.ReedSolomonParityShards),
		ReedSolomonIndex:        uint32(shard.ReedSolomonIndex),
		Size:                    shard.Size,
		OriginalSize:            shard.OriginalSize,
		EncapsulatedKey:         shard.EncapsulatedKey,
		Nonce:                   shard.Nonce,
		ChunkContent:            shard.ChunkContent,
	}
	data, err := proto.Marshal(protoChunk)
	if err != nil {
		return fmt.Errorf("failed to marshal shard: %w", err)
	}
	key := fmt.Sprintf("%s%x_%d", ChunkPrefix, shard.ChunkHash, shard.ReedSolomonIndex)
	return w.Set([]byte(key), data)
}

func StoreParentChildRelationships(w KVWriter, dataKey, parent hash.Hash, children []hash.Hash) error {
	if !IsEmptyHash(parent) {
		parentToChildKey := fmt.Sprintf("%s%s:%s", ParentPrefix, parent, dataKey)
		if err := w.Set([]byte(parentToChildKey), []byte{}); err != nil {
			return fmt.Errorf("failed to store parent->child relationship: %w", err)
		}
		childToParentKey := fmt.Sprintf("%s%s:%s", ChildPrefix, dataKey, parent)
		if err := w.Set([]byte(childToParentKey), []byte{}); err != nil {
			return fmt.Errorf("failed to store child->parent relationship: %w", err)
		}
	}
	for _, child := range children {
		if IsEmptyHash(child) {
			continue
		}
		parentToChildKey := fmt.Sprintf("%s%s:%s", ParentPrefix, dataKey, child)
		if err := w.Set([]byte(parentToChildKey), []byte{}); err != nil {
			return fmt.Errorf("failed to store parent->child relationship: %w", err)
		}
		childToParentKey := fmt.Sprintf("%s%s:%s", ChildPrefix, child, dataKey)
		if err := w.Set([]byte(childToParentKey), []byte{}); err != nil {
			return fmt.Errorf("failed to store child->parent relationship: %w", err)
		}
	}
	return nil
}

func LoadMetadata(txn *badger.Txn, key hash.Hash) (Metadata, error) {
	metadataKey := fmt.Sprintf("%s%x", MetadataPrefix, key)
	item, err := txn.Get([]byte(metadataKey))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return Metadata{}, fmt.Errorf("metadata not found for key %x", key)
		}
		return Metadata{}, fmt.Errorf("failed to get metadata: %w", err)
	}
	var protoData []byte
	if err = item.Value(func(val []byte) error {
		protoData = append([]byte(nil), val...)
		return nil
	}); err != nil {
		return Metadata{}, fmt.Errorf("failed to read metadata value: %w", err)
	}
	protoMetadata := &pb.KvDataHashProto{}
	if err = proto.Unmarshal(protoData, protoMetadata); err != nil {
		return Metadata{}, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}
	metadata := Metadata{Key: key}
	if len(protoMetadata.Parent) == 64 {
		copy(metadata.Parent[:], protoMetadata.Parent)
	}
	for _, chunkHashBytes := range protoMetadata.ChunkHashes {
		if len(chunkHashBytes) == 64 {
			var chunkHash hash.Hash
			copy(chunkHash[:], chunkHashBytes)
			metadata.ShardHashes = append(metadata.ShardHashes, chunkHash)
		}
	}
	for _, childBytes := range protoMetadata.Children {
		if len(childBytes) == 64 {
			var child hash.Hash
			copy(child[:], childBytes)
			metadata.Children = append(metadata.Children, child)
		}
	}
	return metadata, nil
}

func LoadShardsByHash(txn *badger.Txn, chunkHash hash.Hash) ([]Shard, error) {
	var shards []Shard
	prefix := fmt.Sprintf("%s%x_", ChunkPrefix, chunkHash)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()
	for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
		item := it.Item()
		var protoData []byte
		if err := item.Value(func(val []byte) error {
			protoData = append([]byte(nil), val...)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("failed to read shard value: %w", err)
		}
		protoChunk := &pb.KvDataShardProto{}
		if err := proto.Unmarshal(protoData, protoChunk); err != nil {
			return nil, fmt.Errorf("failed to unmarshal shard: %w", err)
		}
		shard := Shard{
			ReedSolomonShards:       uint8(protoChunk.ReedSolomonShards),
			ReedSolomonParityShards: uint8(protoChunk.ReedSolomonParityShards),
			ReedSolomonIndex:        uint8(protoChunk.ReedSolomonIndex),
			Size:                    protoChunk.Size,
			OriginalSize:            protoChunk.OriginalSize,
			EncapsulatedKey:         protoChunk.EncapsulatedKey,
			Nonce:                   protoChunk.Nonce,
			ChunkContent:            protoChunk.ChunkContent,
		}
		if len(protoChunk.ChunkHash) == 64 {
			copy(shard.ChunkHash[:], protoChunk.ChunkHash)
		}
		if len(protoChunk.EncodedHash) == 64 {
			copy(shard.EncodedHash[:], protoChunk.EncodedHash)
		}
		shards = append(shards, shard)
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("no chunks found for hash %x", chunkHash)
	}
	return shards, nil
}

func DeleteMetadata(txn *badger.Txn, key hash.Hash) error {
	metadataKey := fmt.Sprintf("%s%x", MetadataPrefix, key)
	return txn.Delete([]byte(metadataKey))
}

func DeleteShard(txn *badger.Txn, shard Shard) error {
	chunkKey := fmt.Sprintf("%s%x_%d", ChunkPrefix, shard.ChunkHash, shard.ReedSolomonIndex)
	return txn.Delete([]byte(chunkKey))
}
