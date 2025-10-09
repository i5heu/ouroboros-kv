package ouroboroskv

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/i5heu/ouroboros-crypt/hash"
	pb "github.com/i5heu/ouroboros-kv/proto"
	"google.golang.org/protobuf/proto"
)

const (
	// Key prefixes for different data types in BadgerDB
	METADATA_PREFIX       = "meta:"        // For KvDataHash metadata
	METADATA_CHUNK_PREFIX = "meta_chunks:" // For metadata shard hashes
	CHUNK_PREFIX          = "chunk:"       // For KvDataShard data
	PARENT_PREFIX         = "parent:"      // For parent relationships: parent_key -> child_key
	CHILD_PREFIX          = "child:"       // For child relationships: child_key -> parent_key
)

// WriteData encodes and stores the given Data in the key-value store
// It uses the encoding pipeline to create encrypted, compressed, and erasure-coded chunks
func (k *KV) WriteData(data Data) (hash.Hash, error) {
	atomic.AddUint64(&k.writeCounter, 1)

	if !isEmptyHash(data.Key) {
		return hash.Hash{}, fmt.Errorf("data key must be zero value; it will be generated from content")
	}

	if data.CreationUnixTime == 0 {
		data.CreationUnixTime = time.Now().Unix()
	}

	generatedKey, aliases := generateDataKey(data)
	data.Key = generatedKey
	data.Alias = aliases

	// Use the encoding pipeline to process the data
	encoded, err := k.encodeDataPipeline(data)
	if err != nil {
		return hash.Hash{}, fmt.Errorf("failed to encode data: %w", err)
	}

	// Group content shards by chunk hash
	contentHashes := make([]hash.Hash, 0, len(encoded.Shards))
	contentShardMap := make(map[hash.Hash][]kvDataShard)
	for _, shard := range encoded.Shards {
		if _, exists := contentShardMap[shard.ChunkHash]; !exists {
			contentHashes = append(contentHashes, shard.ChunkHash)
		}
		contentShardMap[shard.ChunkHash] = append(contentShardMap[shard.ChunkHash], shard)
	}

	// Group metadata shards by chunk hash
	metaHashes := make([]hash.Hash, 0, len(encoded.MetaShards))
	metaShardMap := make(map[hash.Hash][]kvDataShard)
	for _, shard := range encoded.MetaShards {
		if _, exists := metaShardMap[shard.ChunkHash]; !exists {
			metaHashes = append(metaHashes, shard.ChunkHash)
		}
		metaShardMap[shard.ChunkHash] = append(metaShardMap[shard.ChunkHash], shard)
	}

	metadata := kvDataHash{
		Key:              encoded.Key,
		ShardHashes:      contentHashes,
		MetaShardHashes:  metaHashes,
		Parent:           encoded.Parent,
		CreationUnixTime: encoded.CreationUnixTime,
		Alias:            encoded.Alias,
	}
	fmt.Printf("DEBUG store: key bytes len=%d hex=%x string=%s\n", len(metadata.Key), metadata.Key[:], metadata.Key.String())

	// Use WriteBatch for better handling of large transactions
	wb := k.badgerDB.NewWriteBatch()
	defer wb.Cancel()

	// Store metadata
	err = k.storeMetadataWithBatch(wb, metadata)
	if err != nil {
		return hash.Hash{}, fmt.Errorf("failed to store metadata: %w", err)
	}

	if err := k.storeMetadataChunkHashesWithBatch(wb, metadata.Key, metadata.MetaShardHashes); err != nil {
		return hash.Hash{}, fmt.Errorf("failed to store metadata shard hashes: %w", err)
	}

	// Store parent-child relationships
	err = k.storeParentChildRelationships(wb, metadata.Key, metadata.Parent)
	if err != nil {
		return hash.Hash{}, fmt.Errorf("failed to store parent-child relationships: %w", err)
	}

	// Store all content chunks
	for _, chunks := range contentShardMap {
		for _, chunk := range chunks {
			if err := k.storeChunkWithBatch(wb, chunk); err != nil {
				return hash.Hash{}, fmt.Errorf("failed to store chunk: %w", err)
			}
		}
	}

	// Store all metadata chunks
	for _, chunks := range metaShardMap {
		for _, chunk := range chunks {
			if err := k.storeChunkWithBatch(wb, chunk); err != nil {
				return hash.Hash{}, fmt.Errorf("failed to store metadata chunk: %w", err)
			}
		}
	}

	// Commit the batch
	err = wb.Flush()
	if err != nil {
		log.Error("Failed to write data", "error", err)
		return hash.Hash{}, fmt.Errorf("failed to commit batch: %w", err)
	}

	log.Debug("Successfully wrote data", "key", fmt.Sprintf("%x", data.Key))
	return data.Key, nil
}

// storeMetadata serializes and stores KvDataHash metadata
func (k *KV) storeMetadata(txn *badger.Txn, metadata kvDataHash) error {
	metadata.Alias = canonicalizeAliases(metadata.Alias)
	// Convert to protobuf
	protoMetadata := &pb.KvDataHashProto{
		Key:              metadata.Key[:],
		Parent:           metadata.Parent[:],
		CreationUnixTime: metadata.CreationUnixTime,
	}

	// Convert chunk hashes
	for _, chunkHash := range metadata.ShardHashes {
		protoMetadata.ChunkHashes = append(protoMetadata.ChunkHashes, append([]byte(nil), chunkHash[:]...))
	}

	for _, alias := range metadata.Alias {
		protoMetadata.Aliases = append(protoMetadata.Aliases, append([]byte(nil), alias[:]...))
	}
	fmt.Printf("DEBUG protoMetadata before marshal chunk hashes=%d alias=%d keyLen=%d parentLen=%d firstBytes=%x\n", len(protoMetadata.ChunkHashes), len(protoMetadata.Aliases), len(protoMetadata.Key), len(protoMetadata.Parent), protoMetadata.Key[:5])
	fmt.Printf("DEBUG protoMetadata before marshal chunk hashes=%d alias=%d\n", len(protoMetadata.ChunkHashes), len(protoMetadata.Aliases))

	// Serialize to protobuf
	data, err := proto.Marshal(protoMetadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	fmt.Printf("DEBUG marshaled metadata len=%d size=%d\n", len(data), proto.Size(protoMetadata))

	// Create key with metadata prefix
	key := fmt.Sprintf("%s%x", METADATA_PREFIX, metadata.Key[:])
	fmt.Printf("DEBUG storeMetadataWithBatch key=%s\n", key)
	fmt.Printf("DEBUG storeMetadata key=%s\n", key)

	return txn.Set([]byte(key), data)
}

// storeChunk serializes and stores a KvDataShard
func (k *KV) storeChunk(txn *badger.Txn, chunk kvDataShard) error {
	// Convert to protobuf
	protoChunk := &pb.KvDataShardProto{
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
	key := fmt.Sprintf("%s%x_%d", CHUNK_PREFIX, chunk.ChunkHash[:], chunk.ReedSolomonIndex)

	return txn.Set([]byte(key), data)
}

// storeMetadataWithBatch serializes and stores KvDataHash metadata using WriteBatch
func (k *KV) storeMetadataWithBatch(wb *badger.WriteBatch, metadata kvDataHash) error {
	metadata.Alias = canonicalizeAliases(metadata.Alias)
	// Convert to protobuf
	protoMetadata := &pb.KvDataHashProto{
		Key:              append([]byte(nil), metadata.Key[:]...),
		Parent:           append([]byte(nil), metadata.Parent[:]...),
		CreationUnixTime: metadata.CreationUnixTime,
	}
	fmt.Printf("DEBUG storeMetadataWithBatch shards=%d meta=%d alias=%d creation=%d\n", len(metadata.ShardHashes), len(metadata.MetaShardHashes), len(metadata.Alias), metadata.CreationUnixTime)

	// Convert chunk hashes
	for _, chunkHash := range metadata.ShardHashes {
		protoMetadata.ChunkHashes = append(protoMetadata.ChunkHashes, append([]byte(nil), chunkHash[:]...))
	}

	for _, alias := range metadata.Alias {
		protoMetadata.Aliases = append(protoMetadata.Aliases, append([]byte(nil), alias[:]...))
	}
	firstChunkLen := 0
	firstAliasLen := 0
	if len(protoMetadata.ChunkHashes) > 0 {
		firstChunkLen = len(protoMetadata.ChunkHashes[0])
	}
	if len(protoMetadata.Aliases) > 0 {
		firstAliasLen = len(protoMetadata.Aliases[0])
	}
	fmt.Printf("DEBUG protoMetadata before marshal chunk hashes=%d alias=%d keyLen=%d parentLen=%d firstChunkLen=%d firstAliasLen=%d\n", len(protoMetadata.ChunkHashes), len(protoMetadata.Aliases), len(protoMetadata.Key), len(protoMetadata.Parent), firstChunkLen, firstAliasLen)

	// Serialize to protobuf
	data, err := proto.Marshal(protoMetadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	fmt.Printf("DEBUG marshaled metadata len=%d size=%d\n", len(data), proto.Size(protoMetadata))

	// Create key with metadata prefix
	key := fmt.Sprintf("%s%x", METADATA_PREFIX, metadata.Key[:])

	return wb.Set([]byte(key), data)
}

func (k *KV) storeMetadataChunkHashesWithBatch(wb *badger.WriteBatch, key hash.Hash, hashes []hash.Hash) error {
	metaKey := fmt.Sprintf("%s%x", METADATA_CHUNK_PREFIX, key[:])
	if len(hashes) == 0 {
		return nil
	}

	payload := serializeHashesToBytes(hashes)
	return wb.Set([]byte(metaKey), payload)
}

func (k *KV) storeMetadataChunkHashesTxn(txn *badger.Txn, key hash.Hash, hashes []hash.Hash) error {
	metaKey := fmt.Sprintf("%s%x", METADATA_CHUNK_PREFIX, key[:])
	if len(hashes) == 0 {
		return nil
	}

	payload := serializeHashesToBytes(hashes)
	return txn.Set([]byte(metaKey), payload)
}

// storeChunkWithBatch serializes and stores a KvDataShard using WriteBatch
func (k *KV) storeChunkWithBatch(wb *badger.WriteBatch, chunk kvDataShard) error {
	// Convert to protobuf
	protoChunk := &pb.KvDataShardProto{
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
	key := fmt.Sprintf("%s%x_%d", CHUNK_PREFIX, chunk.ChunkHash[:], chunk.ReedSolomonIndex)

	return wb.Set([]byte(key), data)
}

// storeParentChildRelationships stores bidirectional parent-child relationships in BadgerDB
func (k *KV) storeParentChildRelationships(wb *badger.WriteBatch, dataKey, parent hash.Hash) error {
	// Store parent -> child relationship (if this data has a parent)
	if !isEmptyHash(parent) {
		parentStr := parent.String()
		childStr := dataKey.String()

		// Store: parent:PARENT_HASH -> child:DATA_KEY
		parentToChildKey := fmt.Sprintf("%s%s:%s", PARENT_PREFIX, parentStr, childStr)
		err := wb.Set([]byte(parentToChildKey), []byte(""))
		if err != nil {
			return fmt.Errorf("failed to store parent->child relationship: %w", err)
		}

		// Store: child:DATA_KEY -> parent:PARENT_HASH
		childToParentKey := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, childStr, parentStr)
		err = wb.Set([]byte(childToParentKey), []byte(""))
		if err != nil {
			return fmt.Errorf("failed to store child->parent relationship: %w", err)
		}
	}

	return nil
}

// isEmptyHash checks if a hash is the zero value
func isEmptyHash(h hash.Hash) bool {
	var empty hash.Hash
	return h == empty
}

// BatchWriteData writes multiple Data objects in a single batch operation
func (k *KV) BatchWriteData(dataList []Data) ([]hash.Hash, error) {
	if len(dataList) == 0 {
		return []hash.Hash{}, nil
	}

	atomic.AddUint64(&k.writeCounter, uint64(len(dataList)))

	// Process all data through encoding pipeline first
	var (
		allMetadata []kvDataHash
		allChunks   []kvDataShard
		keys        []hash.Hash
	)

	for _, data := range dataList {

		if !isEmptyHash(data.Key) {
			return nil, fmt.Errorf("data key must be zero value; it will be generated from content")
		}

		if data.CreationUnixTime == 0 {
			data.CreationUnixTime = time.Now().Unix()
		}

		generatedKey, aliases := generateDataKey(data)
		data.Key = generatedKey
		data.Alias = aliases

		encoded, err := k.encodeDataPipeline(data)
		if err != nil {
			return nil, fmt.Errorf("failed to encode data with key %x: %w", data.Key, err)
		}
		keys = append(keys, data.Key)

		// Create metadata
		contentHashes := make([]hash.Hash, 0, len(encoded.Shards))
		contentSeen := make(map[hash.Hash]bool)
		for _, chunk := range encoded.Shards {
			if !contentSeen[chunk.ChunkHash] {
				contentHashes = append(contentHashes, chunk.ChunkHash)
				contentSeen[chunk.ChunkHash] = true
			}
			allChunks = append(allChunks, chunk)
		}

		metaHashes := make([]hash.Hash, 0, len(encoded.MetaShards))
		metaSeen := make(map[hash.Hash]bool)
		for _, chunk := range encoded.MetaShards {
			if !metaSeen[chunk.ChunkHash] {
				metaHashes = append(metaHashes, chunk.ChunkHash)
				metaSeen[chunk.ChunkHash] = true
			}
			allChunks = append(allChunks, chunk)
		}

		metadata := kvDataHash{
			Key:              encoded.Key,
			ShardHashes:      contentHashes,
			MetaShardHashes:  metaHashes,
			Parent:           encoded.Parent,
			CreationUnixTime: encoded.CreationUnixTime,
			Alias:            encoded.Alias,
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

			if err := k.storeMetadataChunkHashesTxn(txn, metadata.Key, metadata.MetaShardHashes); err != nil {
				return fmt.Errorf("failed to store metadata shard hashes for key %x: %w", metadata.Key, err)
			}

			// Store parent-child relationships
			err = k.storeParentChildRelationshipsTxn(txn, metadata.Key, metadata.Parent)
			if err != nil {
				return fmt.Errorf("failed to store parent-child relationships for key %x: %w", metadata.Key, err)
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
		log.Error("Failed to batch write data", "error", err)
		return nil, fmt.Errorf("failed to batch write data: %w", err)
	}

	log.Debug("Successfully batch wrote data", "count", len(dataList))
	return keys, nil
}

// storeParentChildRelationshipsTxn stores parent-child relationships using a transaction
func (k *KV) storeParentChildRelationshipsTxn(txn *badger.Txn, dataKey, parent hash.Hash) error {
	// Store parent -> child relationship (if this data has a parent)
	if !isEmptyHash(parent) {
		parentStr := parent.String()
		childStr := dataKey.String()

		// Store: parent:PARENT_HASH -> child:DATA_KEY
		parentToChildKey := fmt.Sprintf("%s%s:%s", PARENT_PREFIX, parentStr, childStr)
		err := txn.Set([]byte(parentToChildKey), []byte{})
		if err != nil {
			return fmt.Errorf("failed to store parent->child relationship: %w", err)
		}

		// Store: child:DATA_KEY -> parent:PARENT_HASH
		childToParentKey := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, childStr, parentStr)
		err = txn.Set([]byte(childToParentKey), []byte{})
		if err != nil {
			return fmt.Errorf("failed to store child->parent relationship: %w", err)
		}
	}

	return nil
}
