package ouroboroskv

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/i5heu/ouroboros-crypt/hash"
	pb "github.com/i5heu/ouroboros-kv/proto"
	"google.golang.org/protobuf/proto"
)

var hashStringLen = len(hash.Hash{}.String())

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
		fmt.Printf("DEBUG metadata shard hashes=%d meta shard hashes=%d\n", len(metadata.ShardHashes), len(metadata.MetaShardHashes))

		// Load all content chunks for this data
		var contentChunks []kvDataShard
		for _, chunkHash := range metadata.ShardHashes {
			chunks, err := k.loadChunksByHash(txn, chunkHash)
			if err != nil {
				return fmt.Errorf("failed to load chunks for hash %x: %w", chunkHash, err)
			}
			fmt.Printf("DEBUG loaded %d chunks for hash %s\n", len(chunks), chunkHash.String())
			contentChunks = append(contentChunks, chunks...)
		}

		// Load metadata chunks if present
		var metadataChunks []kvDataShard
		for _, chunkHash := range metadata.MetaShardHashes {
			chunks, err := k.loadChunksByHash(txn, chunkHash)
			if err != nil {
				return fmt.Errorf("failed to load metadata chunks for hash %x: %w", chunkHash, err)
			}
			metadataChunks = append(metadataChunks, chunks...)
		}

		// Create KvDataLinked structure for decoding
		kvDataLinked := kvDataLinked{
			Key:              metadata.Key,
			Shards:           contentChunks,
			ChunkHashes:      metadata.ShardHashes,
			MetaShards:       metadataChunks,
			MetaChunkHashes:  metadata.MetaShardHashes,
			Parent:           metadata.Parent,
			CreationUnixTime: metadata.CreationUnixTime,
			Alias:            metadata.Alias,
			Children:         metadata.Children,
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
		log.Error("Failed to read data", "key", fmt.Sprintf("%x", key), "error", err)
		return Data{}, fmt.Errorf("failed to read data: %w", err)
	}

	log.Debug("Successfully read data", "key", fmt.Sprintf("%x", key))
	return data, nil
}

// GetChildren returns all direct children of a given data key
func (k *KV) GetChildren(parentKey hash.Hash) ([]hash.Hash, error) {
	atomic.AddUint64(&k.readCounter, 1)

	var children []hash.Hash

	err := k.badgerDB.View(func(txn *badger.Txn) error {
		var err error
		children, err = k.getChildrenFromTxn(txn, parentKey)
		return err
	})

	return children, err
}

func (k *KV) getChildrenFromTxn(txn *badger.Txn, parentKey hash.Hash) ([]hash.Hash, error) {
	prefixKey := fmt.Sprintf("%s%s:", PARENT_PREFIX, parentKey.String())
	prefix := []byte(prefixKey)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	var children []hash.Hash

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		key := item.Key()

		keyStr := string(key)
		if len(keyStr) <= len(prefixKey) {
			continue
		}

		childHashHex := keyStr[len(prefixKey):]
		var childHash hash.Hash
		var err error

		switch len(childHashHex) {
		case hashStringLen:
			childHash, err = hash.HashHexadecimal(childHashHex)
		case hashStringLen * 2:
			if decoded, decodeErr := hex.DecodeString(childHashHex); decodeErr == nil {
				childHash, err = hash.HashHexadecimal(string(decoded))
			}
		}

		if err != nil {
			continue
		}

		children = append(children, childHash)
	}

	sort.Slice(children, func(i, j int) bool {
		return bytes.Compare(children[i][:], children[j][:]) < 0
	})

	return children, nil
}

// GetParent returns the parent of a given data key
func (k *KV) GetParent(childKey hash.Hash) (hash.Hash, error) {
	atomic.AddUint64(&k.readCounter, 1)

	var parent hash.Hash

	err := k.badgerDB.View(func(txn *badger.Txn) error {
		prefix := []byte(fmt.Sprintf("%s%s:", CHILD_PREFIX, childKey.String()))

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // We only need keys
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()

			// Extract parent hash from key: child:CHILD_HASH:PARENT_HASH
			keyStr := string(key)
			parts := len(fmt.Sprintf("%s%s:", CHILD_PREFIX, childKey.String()))
			if len(keyStr) > parts {
				parentHashHex := keyStr[parts:]
				var candidate hash.Hash
				var err error
				switch len(parentHashHex) {
				case hashStringLen:
					candidate, err = hash.HashHexadecimal(parentHashHex)
				case hashStringLen * 2:
					if decoded, decodeErr := hex.DecodeString(parentHashHex); decodeErr == nil {
						candidate, err = hash.HashHexadecimal(string(decoded))
					}
				}
				if err == nil {
					parent = candidate
					break // Should only be one parent
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
	return k.ListRootKeys()
}

// loadMetadata loads and deserializes KvDataHash metadata from storage
func (k *KV) loadMetadata(txn *badger.Txn, key hash.Hash) (kvDataHash, error) {
	// Create key with metadata prefix
	metadataKey := fmt.Sprintf("%s%x", METADATA_PREFIX, key[:])
	fmt.Printf("DEBUG loadMetadata key=%s\n", metadataKey)

	item, err := txn.Get([]byte(metadataKey))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return kvDataHash{}, fmt.Errorf("metadata not found for key %x", key)
		}
		return kvDataHash{}, fmt.Errorf("failed to get metadata: %w", err)
	}

	var protoData []byte
	err = item.Value(func(val []byte) error {
		protoData = append([]byte(nil), val...)
		return nil
	})
	if err != nil {
		return kvDataHash{}, fmt.Errorf("failed to read metadata value: %w", err)
	}
	fmt.Printf("DEBUG raw metadata bytes len=%d\n", len(protoData))

	// Deserialize protobuf
	protoMetadata := &pb.KvDataHashProto{}
	err = proto.Unmarshal(protoData, protoMetadata)
	if err != nil {
		return kvDataHash{}, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}
	fmt.Printf("DEBUG protoMetadata chunk hashes=%d aliases=%d\n", len(protoMetadata.ChunkHashes), len(protoMetadata.Aliases))

	// Convert back to Go struct
	metadata := kvDataHash{
		Key: key, // We already know the key
	}

	// Convert parent
	if len(protoMetadata.Parent) == 64 { // hash.Hash is 64 bytes
		copy(metadata.Parent[:], protoMetadata.Parent)
	}

	// Convert chunk hashes
	for _, chunkHashBytes := range protoMetadata.ChunkHashes {
		fmt.Printf("DEBUG chunkHashBytes len=%d\n", len(chunkHashBytes))
		if len(chunkHashBytes) == 64 {
			var chunkHash hash.Hash
			copy(chunkHash[:], chunkHashBytes)
			metadata.ShardHashes = append(metadata.ShardHashes, chunkHash)
		}
	}

	metadata.CreationUnixTime = protoMetadata.CreationUnixTime

	for _, aliasBytes := range protoMetadata.Aliases {
		if len(aliasBytes) == 64 {
			var alias hash.Hash
			copy(alias[:], aliasBytes)
			metadata.Alias = append(metadata.Alias, alias)
		}
	}
	metadata.Alias = canonicalizeAliases(metadata.Alias)

	children, err := k.getChildrenFromTxn(txn, key)
	if err != nil {
		return kvDataHash{}, fmt.Errorf("failed to load children: %w", err)
	}
	metadata.Children = children

	metaChunksKey := fmt.Sprintf("%s%x", METADATA_CHUNK_PREFIX, key[:])
	metaItem, err := txn.Get([]byte(metaChunksKey))
	if err == nil {
		var raw []byte
		err = metaItem.Value(func(val []byte) error {
			raw = append([]byte(nil), val...)
			return nil
		})
		if err != nil {
			return kvDataHash{}, fmt.Errorf("failed to read metadata chunk hashes: %w", err)
		}

		hashes, err := deserializeHashesFromBytes(raw)
		if err != nil {
			return kvDataHash{}, fmt.Errorf("failed to parse metadata chunk hashes: %w", err)
		}
		metadata.MetaShardHashes = hashes
	} else if err != nil && err != badger.ErrKeyNotFound {
		return kvDataHash{}, fmt.Errorf("failed to load metadata chunk hashes: %w", err)
	}

	return metadata, nil
}

// loadChunksByHash loads all chunks (shards) for a given chunk hash
func (k *KV) loadChunksByHash(txn *badger.Txn, chunkHash hash.Hash) ([]kvDataShard, error) {
	var chunks []kvDataShard

	// Create iterator to find all chunks with this hash
	prefix := fmt.Sprintf("%s%x_", CHUNK_PREFIX, chunkHash[:])
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
		protoChunk := &pb.KvDataShardProto{}
		err = proto.Unmarshal(protoData, protoChunk)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal chunk: %w", err)
		}

		// Convert back to Go struct
		chunk := kvDataShard{
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
			var contentChunks []kvDataShard
			for _, chunkHash := range metadata.ShardHashes {
				chunks, err := k.loadChunksByHash(txn, chunkHash)
				if err != nil {
					return fmt.Errorf("failed to load chunks for hash %x: %w", chunkHash, err)
				}
				contentChunks = append(contentChunks, chunks...)
			}

			var metadataChunks []kvDataShard
			for _, chunkHash := range metadata.MetaShardHashes {
				chunks, err := k.loadChunksByHash(txn, chunkHash)
				if err != nil {
					return fmt.Errorf("failed to load metadata chunks for hash %x: %w", chunkHash, err)
				}
				metadataChunks = append(metadataChunks, chunks...)
			}

			// Create KvDataLinked structure for decoding
			kvDataLinked := kvDataLinked{
				Key:              metadata.Key,
				Shards:           contentChunks,
				ChunkHashes:      metadata.ShardHashes,
				MetaShards:       metadataChunks,
				MetaChunkHashes:  metadata.MetaShardHashes,
				Parent:           metadata.Parent,
				CreationUnixTime: metadata.CreationUnixTime,
				Alias:            metadata.Alias,
				Children:         metadata.Children,
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
		log.Error("Failed to batch read data", "error", err)
		return nil, fmt.Errorf("failed to batch read data: %w", err)
	}

	log.Debug("Successfully batch read data", "count", len(keys))
	return results, nil
}

// DataExists checks if data exists for the given key
func (k *KV) DataExists(key hash.Hash) (bool, error) {
	atomic.AddUint64(&k.readCounter, 1)

	var exists bool
	err := k.badgerDB.View(func(txn *badger.Txn) error {
		metadataKey := fmt.Sprintf("%s%x", METADATA_PREFIX, key[:])
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

			if len(key) <= len(METADATA_PREFIX) {
				continue
			}

			hashHex := string(key[len(METADATA_PREFIX):])
			var hashValue hash.Hash
			var err error

			switch len(hashHex) {
			case 128:
				hashValue, err = hash.HashHexadecimal(hashHex)
			case 256:
				if decoded, decodeErr := hex.DecodeString(hashHex); decodeErr == nil {
					hashValue, err = hash.HashHexadecimal(string(decoded))
				}
			default:
				continue
			}

			if err != nil {
				continue
			}

			keys = append(keys, hashValue)
		}
		return nil
	})

	return keys, err
}

// ListRootKeys returns all metadata hashes that do not have a parent relationship
func (k *KV) ListRootKeys() ([]hash.Hash, error) {
	var roots []hash.Hash
	atomic.AddUint64(&k.readCounter, 1)

	err := k.badgerDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte(METADATA_PREFIX)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			var valueCopy []byte
			if err := item.Value(func(val []byte) error {
				valueCopy = append([]byte(nil), val...)
				return nil
			}); err != nil {
				return fmt.Errorf("failed to read metadata value: %w", err)
			}

			protoMetadata := &pb.KvDataHashProto{}
			if err := proto.Unmarshal(valueCopy, protoMetadata); err != nil {
				return fmt.Errorf("failed to unmarshal metadata: %w", err)
			}

			var parent hash.Hash
			if len(protoMetadata.Parent) == len(parent) {
				copy(parent[:], protoMetadata.Parent)
			}

			if !isEmptyHash(parent) {
				continue
			}

			var keyHash hash.Hash
			if len(protoMetadata.Key) == len(keyHash) {
				copy(keyHash[:], protoMetadata.Key)
				roots = append(roots, keyHash)
				continue
			}

			// Fallback: attempt to parse the hash from the metadata key prefix
			keyBytes := item.Key()
			if len(keyBytes) > len(METADATA_PREFIX) {
				hashHex := string(keyBytes[len(METADATA_PREFIX):])
				hashValue, err := hash.HashHexadecimal(hashHex)
				if err != nil && len(hashHex) == 256 {
					if decoded, decodeErr := hex.DecodeString(hashHex); decodeErr == nil {
						hashValue, err = hash.HashHexadecimal(string(decoded))
					}
				}
				if err == nil {
					roots = append(roots, hashValue)
				}
			}
		}
		return nil
	})

	return roots, err
}
