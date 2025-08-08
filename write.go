package ouroboroskv

import (
	"fmt"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/i5heu/ouroboros-kv/storage"
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
	chunkMap := make(map[hash.Hash][]storage.Shard)

	// Group chunks by their chunk hash to get unique chunk identifiers
	for _, chunk := range encoded.Shards {
		if _, exists := chunkMap[chunk.ChunkHash]; !exists {
			chunkHashes = append(chunkHashes, chunk.ChunkHash)
		}
		chunkMap[chunk.ChunkHash] = append(chunkMap[chunk.ChunkHash], chunk)
	}

	metadata := storage.Metadata{
		Key:         encoded.Key,
		ShardHashes: chunkHashes,
		Parent:      encoded.Parent,
		Children:    encoded.Children,
	}

	// Use WriteBatch for better handling of large transactions
	wb := k.badgerDB.NewWriteBatch()
	defer wb.Cancel()

	// Store metadata
	err = storage.StoreMetadata(wb, metadata)
	if err != nil {
		return fmt.Errorf("failed to store metadata: %w", err)
	}

	// Store parent-child relationships
	err = storage.StoreParentChildRelationships(wb, metadata.Key, metadata.Parent, metadata.Children)
	if err != nil {
		return fmt.Errorf("failed to store parent-child relationships: %w", err)
	}

	// Store all chunks
	for _, chunks := range chunkMap {
		for _, chunk := range chunks {
			err := storage.StoreShard(wb, chunk)
			if err != nil {
				return fmt.Errorf("failed to store chunk: %w", err)
			}
		}
	}

	// Commit the batch
	err = wb.Flush()
	if err != nil {
		log.Errorf("Failed to write data: %v", err)
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	log.Debugf("Successfully wrote data with key %x", data.Key)
	return nil
}

// BatchWriteData writes multiple Data objects in a single batch operation
func (k *KV) BatchWriteData(dataList []Data) error {
	if len(dataList) == 0 {
		return nil
	}

	atomic.AddUint64(&k.writeCounter, uint64(len(dataList)))

	// Process all data through encoding pipeline first
	var encodedData []kvDataLinked
	var allMetadata []storage.Metadata
	var allChunks []storage.Shard

	for _, data := range dataList {
		encoded, err := k.encodeDataPipeline(data)
		if err != nil {
			return fmt.Errorf("failed to encode data with key %x: %w", data.Key, err)
		}
		encodedData = append(encodedData, encoded)

		// Create metadata
		var chunkHashes []hash.Hash
		chunkMap := make(map[hash.Hash]bool)

		for _, chunk := range encoded.Shards {
			if !chunkMap[chunk.ChunkHash] {
				chunkHashes = append(chunkHashes, chunk.ChunkHash)
				chunkMap[chunk.ChunkHash] = true
			}
			allChunks = append(allChunks, chunk)
		}

		metadata := storage.Metadata{
			Key:         encoded.Key,
			ShardHashes: chunkHashes,
			Parent:      encoded.Parent,
			Children:    encoded.Children,
		}
		allMetadata = append(allMetadata, metadata)
	}

	// Perform batch write
	err := k.badgerDB.Update(func(txn *badger.Txn) error {
		// Store all metadata
		for _, metadata := range allMetadata {
			err := storage.StoreMetadata(txn, metadata)
			if err != nil {
				return fmt.Errorf("failed to store metadata for key %x: %w", metadata.Key, err)
			}

			// Store parent-child relationships
			err = storage.StoreParentChildRelationships(txn, metadata.Key, metadata.Parent, metadata.Children)
			if err != nil {
				return fmt.Errorf("failed to store parent-child relationships for key %x: %w", metadata.Key, err)
			}
		}

		// Store all chunks
		for _, chunk := range allChunks {
			err := storage.StoreShard(txn, chunk)
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
