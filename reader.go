package ouroboroskv

import (
	"encoding/hex"
	"fmt"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/i5heu/ouroboros-crypt/hash"
	pb "github.com/i5heu/ouroboros-kv/proto"
	"google.golang.org/protobuf/proto"
)

// ReadData retrieves and decodes data from the key-value store by its hash key
func (k *KV) ReadData(key hash.Hash) (Data, error) {
	atomic.AddUint64(&k.readCounter, 1)

	var data Data
	err := k.badgerDB.View(func(txn *badger.Txn) error {
		// Load metadata first
		metadata, err := k.loadMetadata(txn, key)
		if err != nil {
			return fmt.Errorf("failed to load metadata for key %x: %w", key, err)
		}

		// Load all chunks for this data
		var allChunks []KvContentChunk
		for _, chunkHash := range metadata.ChunkHashes {
			chunks, err := k.loadChunksByHash(txn, chunkHash)
			if err != nil {
				return fmt.Errorf("failed to load chunks for hash %x: %w", chunkHash, err)
			}
			allChunks = append(allChunks, chunks...)
		}

		// Create KvDataLinked structure for decoding
		kvDataLinked := KvDataLinked{
			Key:      metadata.Key,
			Chunks:   allChunks,
			Parent:   metadata.Parent,
			Children: metadata.Children,
		}

		// Use decoding pipeline to reconstruct original data
		decodedData, err := k.decodeDataPipeline(kvDataLinked)
		if err != nil {
			return fmt.Errorf("failed to decode data: %w", err)
		}

		data = decodedData
		return nil
	})

	if err != nil {
		log.Errorf("Failed to read data for key %x: %v", key, err)
		return Data{}, fmt.Errorf("failed to read data: %w", err)
	}

	log.Debugf("Successfully read data with key %x", key)
	return data, nil
}

// GetChildren returns all direct children of a given data key
func (k *KV) GetChildren(parentKey hash.Hash) ([]hash.Hash, error) {
	atomic.AddUint64(&k.readCounter, 1)

	var children []hash.Hash

	err := k.badgerDB.View(func(txn *badger.Txn) error {
		prefix := []byte(fmt.Sprintf("%s%s:", PARENT_PREFIX, parentKey))

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			// Extract child hash from key: parent:PARENT_HASH:CHILD_HASH
			keyStr := string(key)
			prefixLen := len(fmt.Sprintf("%s%s:", PARENT_PREFIX, parentKey))
			if len(keyStr) > prefixLen {
				childHashHex := keyStr[prefixLen:]
				log.Debugf("Parsing child hash hex: '%s' (length: %d)", childHashHex, len(childHashHex))
				if len(childHashHex) == 128 { // 64 bytes = 128 hex chars
					childHash, err := hash.HashHexadecimal(childHashHex)
					if err == nil {
						children = append(children, childHash)
						log.Debugf("Successfully parsed child hash: %x", childHash)
					} else {
						log.Debugf("Failed to parse child hash: %v", err)
					}
				} else {
					log.Debugf("Child hash hex wrong length: expected 128, got %d", len(childHashHex))
				}
			}
		}
		return nil
	})

	return children, err
}

// GetParent returns the parent of a given data key
func (k *KV) GetParent(childKey hash.Hash) (hash.Hash, error) {
	atomic.AddUint64(&k.readCounter, 1)

	var parent hash.Hash

	err := k.badgerDB.View(func(txn *badger.Txn) error {
		prefix := []byte(fmt.Sprintf("%s%s:", CHILD_PREFIX, childKey))

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			// Extract parent hash from key: child:CHILD_HASH:PARENT_HASH
			keyStr := string(key)
			parts := len(fmt.Sprintf("%s%s:", CHILD_PREFIX, childKey))
			if len(keyStr) > parts {
				parentHashHex := keyStr[parts:]
				if len(parentHashHex) == 128 { // 64 bytes = 128 hex chars
					parentHash, err := hash.HashHexadecimal(parentHashHex)
					if err == nil {
						parent = parentHash
						break // Should only be one parent
					}
				}
			}
		}
		return nil
	})

	return parent, err
}

// GetDescendants returns all descendants (children, grandchildren, etc.) of a given data key
func (k *KV) GetDescendants(rootKey hash.Hash) ([]hash.Hash, error) {
	var descendants []hash.Hash
	visited := make(map[hash.Hash]bool)

	var traverse func(hash.Hash) error
	traverse = func(key hash.Hash) error {
		if visited[key] {
			return nil // Avoid cycles
		}
		visited[key] = true

		children, err := k.GetChildren(key)
		if err != nil {
			return err
		}

		for _, child := range children {
			descendants = append(descendants, child)
			err = traverse(child)
			if err != nil {
				return err
			}
		}
		return nil
	}

	err := traverse(rootKey)
	return descendants, err
}

// GetAncestors returns all ancestors (parent, grandparent, etc.) of a given data key
func (k *KV) GetAncestors(leafKey hash.Hash) ([]hash.Hash, error) {
	var ancestors []hash.Hash
	visited := make(map[hash.Hash]bool)

	current := leafKey
	for {
		if visited[current] {
			break // Avoid cycles
		}
		visited[current] = true

		parent, err := k.GetParent(current)
		if err != nil {
			return nil, err
		}

		if isEmptyHash(parent) {
			break // No more parents
		}

		ancestors = append(ancestors, parent)
		current = parent
	}

	return ancestors, nil
}

// GetRoots returns all data entries that have no parent (root nodes)
func (k *KV) GetRoots() ([]hash.Hash, error) {
	allKeys, err := k.ListKeys()
	if err != nil {
		return nil, err
	}

	var roots []hash.Hash
	for _, key := range allKeys {
		parent, err := k.GetParent(key)
		if err != nil {
			return nil, err
		}

		if isEmptyHash(parent) {
			roots = append(roots, key)
		}
	}

	return roots, nil
}

// loadMetadata loads and deserializes KvDataHash metadata from storage
func (k *KV) loadMetadata(txn *badger.Txn, key hash.Hash) (KvDataHash, error) {
	// Create key with metadata prefix
	metadataKey := fmt.Sprintf("%s%x", METADATA_PREFIX, key)

	item, err := txn.Get([]byte(metadataKey))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return KvDataHash{}, fmt.Errorf("metadata not found for key %x", key)
		}
		return KvDataHash{}, fmt.Errorf("failed to get metadata: %w", err)
	}

	var protoData []byte
	err = item.Value(func(val []byte) error {
		protoData = append([]byte(nil), val...)
		return nil
	})
	if err != nil {
		return KvDataHash{}, fmt.Errorf("failed to read metadata value: %w", err)
	}

	// Deserialize protobuf
	protoMetadata := &pb.KvDataHashProto{}
	err = proto.Unmarshal(protoData, protoMetadata)
	if err != nil {
		return KvDataHash{}, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	// Convert back to Go struct
	metadata := KvDataHash{
		Key: key, // We already know the key
	}

	// Convert parent
	if len(protoMetadata.Parent) == 64 { // hash.Hash is 64 bytes
		copy(metadata.Parent[:], protoMetadata.Parent)
	}

	// Convert chunk hashes
	for _, chunkHashBytes := range protoMetadata.ChunkHashes {
		if len(chunkHashBytes) == 64 {
			var chunkHash hash.Hash
			copy(chunkHash[:], chunkHashBytes)
			metadata.ChunkHashes = append(metadata.ChunkHashes, chunkHash)
		}
	}

	// Convert children
	for _, childBytes := range protoMetadata.Children {
		if len(childBytes) == 64 {
			var child hash.Hash
			copy(child[:], childBytes)
			metadata.Children = append(metadata.Children, child)
		}
	}

	return metadata, nil
}

// loadChunksByHash loads all chunks (shards) for a given chunk hash
func (k *KV) loadChunksByHash(txn *badger.Txn, chunkHash hash.Hash) ([]KvContentChunk, error) {
	var chunks []KvContentChunk

	// Create iterator to find all chunks with this hash
	prefix := fmt.Sprintf("%s%x_", CHUNK_PREFIX, chunkHash)
	it := txn.NewIterator(badger.DefaultIteratorOptions)
	defer it.Close()

	for it.Seek([]byte(prefix)); it.ValidForPrefix([]byte(prefix)); it.Next() {
		item := it.Item()

		var protoData []byte
		err := item.Value(func(val []byte) error {
			protoData = append([]byte(nil), val...)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk value: %w", err)
		}

		// Deserialize protobuf
		protoChunk := &pb.KvContentChunkProto{}
		err = proto.Unmarshal(protoData, protoChunk)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal chunk: %w", err)
		}

		// Convert back to Go struct
		chunk := KvContentChunk{
			ReedSolomonShards:       uint8(protoChunk.ReedSolomonShards),
			ReedSolomonParityShards: uint8(protoChunk.ReedSolomonParityShards),
			ReedSolomonIndex:        uint8(protoChunk.ReedSolomonIndex),
			Size:                    protoChunk.Size,
			OriginalSize:            protoChunk.OriginalSize,
			EncapsulatedKey:         protoChunk.EncapsulatedKey,
			Nonce:                   protoChunk.Nonce,
			ChunkContent:            protoChunk.ChunkContent,
		}

		// Convert hashes
		if len(protoChunk.ChunkHash) == 64 {
			copy(chunk.ChunkHash[:], protoChunk.ChunkHash)
		}
		if len(protoChunk.EncodedHash) == 64 {
			copy(chunk.EncodedHash[:], protoChunk.EncodedHash)
		}

		chunks = append(chunks, chunk)
	}

	if len(chunks) == 0 {
		return nil, fmt.Errorf("no chunks found for hash %x", chunkHash)
	}

	return chunks, nil
}

// BatchReadData reads multiple data objects by their keys
func (k *KV) BatchReadData(keys []hash.Hash) ([]Data, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	atomic.AddUint64(&k.readCounter, uint64(len(keys)))

	var results []Data
	err := k.badgerDB.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			// Load metadata
			metadata, err := k.loadMetadata(txn, key)
			if err != nil {
				return fmt.Errorf("failed to load metadata for key %x: %w", key, err)
			}

			// Load all chunks
			var allChunks []KvContentChunk
			for _, chunkHash := range metadata.ChunkHashes {
				chunks, err := k.loadChunksByHash(txn, chunkHash)
				if err != nil {
					return fmt.Errorf("failed to load chunks for hash %x: %w", chunkHash, err)
				}
				allChunks = append(allChunks, chunks...)
			}

			// Create KvDataLinked structure for decoding
			kvDataLinked := KvDataLinked{
				Key:      metadata.Key,
				Chunks:   allChunks,
				Parent:   metadata.Parent,
				Children: metadata.Children,
			}

			// Decode data
			decodedData, err := k.decodeDataPipeline(kvDataLinked)
			if err != nil {
				return fmt.Errorf("failed to decode data for key %x: %w", key, err)
			}

			results = append(results, decodedData)
		}
		return nil
	})

	if err != nil {
		log.Errorf("Failed to batch read data: %v", err)
		return nil, fmt.Errorf("failed to batch read data: %w", err)
	}

	log.Debugf("Successfully batch read %d data objects", len(keys))
	return results, nil
}

// DataExists checks if data exists for the given key
func (k *KV) DataExists(key hash.Hash) (bool, error) {
	atomic.AddUint64(&k.readCounter, 1)

	var exists bool
	err := k.badgerDB.View(func(txn *badger.Txn) error {
		metadataKey := fmt.Sprintf("%s%x", METADATA_PREFIX, key)
		_, err := txn.Get([]byte(metadataKey))
		if err != nil {
			if err == badger.ErrKeyNotFound {
				exists = false
				return nil
			}
			return err
		}
		exists = true
		return nil
	})

	return exists, err
}

// ListKeys returns all data keys stored in the database
func (k *KV) ListKeys() ([]hash.Hash, error) {
	var keys []hash.Hash
	atomic.AddUint64(&k.readCounter, 1)

	err := k.badgerDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(METADATA_PREFIX)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			// Extract hash from key (remove prefix)
			if len(key) > len(METADATA_PREFIX) {
				hashHex := string(key[len(METADATA_PREFIX):])
				// The hash is double-encoded: it's hex of hex
				// First decode to get the "inner" hex string
				if len(hashHex) == 256 {
					// Decode the first 128 chars to get ASCII hex
					innerHexBytes, err := hex.DecodeString(hashHex[:128])
					if err == nil && len(innerHexBytes) == 64 {
						// Now we have the original hex string as ASCII
						innerHex := string(innerHexBytes)
						if len(innerHex) == 64 {
							// This should be 64 ASCII hex chars representing 32 bytes
							// But HashHexadecimal expects 128 hex chars for 64 bytes
							// Let's try the second half too
							secondHalfBytes, err := hex.DecodeString(hashHex[128:])
							if err == nil && len(secondHalfBytes) == 64 {
								fullInnerHex := string(innerHexBytes) + string(secondHalfBytes)
								if len(fullInnerHex) == 128 {
									hashValue, err := hash.HashHexadecimal(fullInnerHex)
									if err == nil {
										keys = append(keys, hashValue)
									}
								}
							}
						}
					}
				}
			}
		}
		return nil
	})

	return keys, err
}
