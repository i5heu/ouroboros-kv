package ouroboroskv

import (
	"fmt"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/i5heu/ouroboros-crypt/hash"
	pb "github.com/i5heu/ouroboros-kv/proto"
	"google.golang.org/protobuf/proto"
)

const (
	// Key prefixes for different data types in BadgerDB
	METADATA_PREFIX = "meta:"  // For KvDataHash metadata
	CHUNK_PREFIX    = "chunk:" // For KvContentChunk data
)

// WriteData encodes and stores the given Data in the key-value store
// It uses the encoding pipeline to create encrypted, compressed, and erasure-coded chunks
func (k *KV) WriteData(data Data) error {
	atomic.AddUint64(&k.writeCounter, 1)

	// Use the encoding pipeline to process the data
	encoded, err := k.encodeDataPipeline(data)
	if err != nil {
		return fmt.Errorf("failed to encode data: %w", err)
	}

	// Create metadata structure with chunk hashes
	var chunkHashes []hash.Hash
	chunkMap := make(map[hash.Hash][]KvContentChunk)

	// Group chunks by their chunk hash to get unique chunk identifiers
	for _, chunk := range encoded.Chunks {
		if _, exists := chunkMap[chunk.ChunkHash]; !exists {
			chunkHashes = append(chunkHashes, chunk.ChunkHash)
		}
		chunkMap[chunk.ChunkHash] = append(chunkMap[chunk.ChunkHash], chunk)
	}

	metadata := KvDataHash{
		Key:         encoded.Key,
		ChunkHashes: chunkHashes,
		Parent:      encoded.Parent,
		Children:    encoded.Children,
	}

	// Use a batch write for atomic operations
	err = k.badgerDB.Update(func(txn *badger.Txn) error {
		// Store metadata
		err := k.storeMetadata(txn, metadata)
		if err != nil {
			return fmt.Errorf("failed to store metadata: %w", err)
		}

		// Store all chunks
		for _, chunks := range chunkMap {
			for _, chunk := range chunks {
				err := k.storeChunk(txn, chunk)
				if err != nil {
					return fmt.Errorf("failed to store chunk: %w", err)
				}
			}
		}

		return nil
	})

	if err != nil {
		log.Errorf("Failed to write data: %v", err)
		return fmt.Errorf("failed to write data to store: %w", err)
	}

	log.Debugf("Successfully wrote data with key %x", data.Key)
	return nil
}

// storeMetadata serializes and stores KvDataHash metadata
func (k *KV) storeMetadata(txn *badger.Txn, metadata KvDataHash) error {
	// Convert to protobuf
	protoMetadata := &pb.KvDataHashProto{
		Key:    metadata.Key[:],
		Parent: metadata.Parent[:],
	}

	// Convert chunk hashes
	for _, chunkHash := range metadata.ChunkHashes {
		protoMetadata.ChunkHashes = append(protoMetadata.ChunkHashes, chunkHash[:])
	}

	// Convert children hashes
	for _, child := range metadata.Children {
		protoMetadata.Children = append(protoMetadata.Children, child[:])
	}

	// Serialize to protobuf
	data, err := proto.Marshal(protoMetadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Create key with metadata prefix
	key := fmt.Sprintf("%s%x", METADATA_PREFIX, metadata.Key)

	return txn.Set([]byte(key), data)
}

// storeChunk serializes and stores a KvContentChunk
func (k *KV) storeChunk(txn *badger.Txn, chunk KvContentChunk) error {
	// Convert to protobuf
	protoChunk := &pb.KvContentChunkProto{
		ChunkHash:               chunk.ChunkHash[:],
		EncodedHash:             chunk.EncodedHash[:],
		ReedSolomonShards:       uint32(chunk.ReedSolomonShards),
		ReedSolomonParityShards: uint32(chunk.ReedSolomonParityShards),
		ReedSolomonIndex:        uint32(chunk.ReedSolomonIndex),
		Size:                    chunk.Size,
		OriginalSize:            chunk.OriginalSize,
		EncapsulatedKey:         chunk.EncapsulatedKey,
		Nonce:                   chunk.Nonce,
		ChunkContent:            chunk.ChunkContent,
	}

	// Serialize to protobuf
	data, err := proto.Marshal(protoChunk)
	if err != nil {
		return fmt.Errorf("failed to marshal chunk: %w", err)
	}

	// Create key with chunk prefix and unique identifier
	// Use chunk hash + Reed-Solomon index to create unique keys for each shard
	key := fmt.Sprintf("%s%x_%d", CHUNK_PREFIX, chunk.ChunkHash, chunk.ReedSolomonIndex)

	return txn.Set([]byte(key), data)
}

// BatchWriteData writes multiple Data objects in a single batch operation
func (k *KV) BatchWriteData(dataList []Data) error {
	if len(dataList) == 0 {
		return nil
	}

	atomic.AddUint64(&k.writeCounter, uint64(len(dataList)))

	// Process all data through encoding pipeline first
	var encodedData []KvDataLinked
	var allMetadata []KvDataHash
	var allChunks []KvContentChunk

	for _, data := range dataList {
		encoded, err := k.encodeDataPipeline(data)
		if err != nil {
			return fmt.Errorf("failed to encode data with key %x: %w", data.Key, err)
		}
		encodedData = append(encodedData, encoded)

		// Create metadata
		var chunkHashes []hash.Hash
		chunkMap := make(map[hash.Hash]bool)

		for _, chunk := range encoded.Chunks {
			if !chunkMap[chunk.ChunkHash] {
				chunkHashes = append(chunkHashes, chunk.ChunkHash)
				chunkMap[chunk.ChunkHash] = true
			}
			allChunks = append(allChunks, chunk)
		}

		metadata := KvDataHash{
			Key:         encoded.Key,
			ChunkHashes: chunkHashes,
			Parent:      encoded.Parent,
			Children:    encoded.Children,
		}
		allMetadata = append(allMetadata, metadata)
	}

	// Perform batch write
	err := k.badgerDB.Update(func(txn *badger.Txn) error {
		// Store all metadata
		for _, metadata := range allMetadata {
			err := k.storeMetadata(txn, metadata)
			if err != nil {
				return fmt.Errorf("failed to store metadata for key %x: %w", metadata.Key, err)
			}
		}

		// Store all chunks
		for _, chunk := range allChunks {
			err := k.storeChunk(txn, chunk)
			if err != nil {
				return fmt.Errorf("failed to store chunk %x: %w", chunk.ChunkHash, err)
			}
		}

		return nil
	})

	if err != nil {
		log.Errorf("Failed to batch write data: %v", err)
		return fmt.Errorf("failed to batch write data: %w", err)
	}

	log.Debugf("Successfully batch wrote %d data objects", len(dataList))
	return nil
}
