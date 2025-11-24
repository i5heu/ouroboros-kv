package store

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	"github.com/i5heu/ouroboros-kv/internal/pipeline"
	"github.com/i5heu/ouroboros-kv/internal/types"
	pb "github.com/i5heu/ouroboros-kv/proto"
	"google.golang.org/protobuf/proto"
)

// ReadData retrieves and decodes data from the key-value store by its hash key
func (k *KV) ReadData(key hash.Hash) (types.Data, error) {
	atomic.AddUint64(&k.readCounter, 1)

	var data types.Data
	err := k.badgerDB.View(func(txn *badger.Txn) error {
		canonicalKey := key
		if resolved, found, err := k.resolveAliasTxn(txn, key); err != nil {
			return fmt.Errorf("failed to resolve alias for key %x: %w", key, err)
		} else if found {
			canonicalKey = resolved
		}

		// Load metadata first
		metadata, err := k.loadMetadata(txn, canonicalKey)
		if err != nil {
			return fmt.Errorf("failed to load metadata for key %x: %w", canonicalKey, err)
		}

		children, err := collectChildrenForTxn(txn, metadata.Key)
		if err != nil {
			return fmt.Errorf("failed to load children for key %x: %w", key, err)
		}

		// Load all content slices for this data
		var contentSlices []types.SealedSlice
		for _, chunkHash := range metadata.ChunkHashes {
			slices, err := k.loadSlicesByHash(txn, chunkHash)
			if err != nil {
				return fmt.Errorf("failed to load slices for hash %x: %w", chunkHash, err)
			}
			contentSlices = append(contentSlices, slices...)
		}

		// Load metadata slices if present
		var metadataSlices []types.SealedSlice
		for _, chunkHash := range metadata.MetaChunkHashes {
			slices, err := k.loadSlicesByHash(txn, chunkHash)
			if err != nil {
				return fmt.Errorf("failed to load metadata slices for hash %x: %w", chunkHash, err)
			}
			metadataSlices = append(metadataSlices, slices...)
		}

		// Create KvDataLinked structure for decoding
		kvDataLinked := types.KvData{
			Key:             metadata.Key,
			Slices:          contentSlices,
			ChunkHashes:     metadata.ChunkHashes,
			MetaSlices:      metadataSlices,
			MetaChunkHashes: metadata.MetaChunkHashes,
			Parent:          metadata.Parent,
			Children:        children,
			Created:         metadata.Created,
			Aliases:         metadata.Aliases,
			ContentType:     metadata.ContentType,
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
		k.log.Error("Failed to read data", "key", fmt.Sprintf("%x", key), "error", err)
		return types.Data{}, fmt.Errorf("failed to read data: %w", err)
	}

	k.log.Debug("Successfully read data", "key", fmt.Sprintf("%x", key))
	return data, nil
}

// GetChildren returns all direct children of a given data key
func (k *KV) GetChildren(parentKey hash.Hash) ([]hash.Hash, error) {
	atomic.AddUint64(&k.readCounter, 1)

	var children []hash.Hash

	err := k.badgerDB.View(func(txn *badger.Txn) error {
		var err error
		children, err = collectChildrenForTxn(txn, parentKey)
		return err
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
			if bytes.HasPrefix(it.Item().Key(), []byte(META_CHUNK_HASH_PREFIX)) {
				continue
			}
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
	return k.ListRootKeys()
}

// loadMetadata loads and deserializes KvDataHash metadata from storage
func (k *KV) loadMetadata(txn *badger.Txn, key hash.Hash) (types.KvRef, error) {
	// Create key with metadata prefix
	metadataKey := fmt.Sprintf("%s%x", METADATA_PREFIX, key)

	item, err := txn.Get([]byte(metadataKey))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return types.KvRef{}, fmt.Errorf("metadata not found for key %x", key)
		}
		return types.KvRef{}, fmt.Errorf("failed to get metadata: %w", err)
	}

	var protoData []byte
	err = item.Value(func(val []byte) error {
		protoData = append([]byte(nil), val...)
		return nil
	})
	if err != nil {
		return types.KvRef{}, fmt.Errorf("failed to read metadata value: %w", err)
	}

	// Deserialize protobuf
	protoMetadata := &pb.KvDataHashProto{}
	err = proto.Unmarshal(protoData, protoMetadata)
	if err != nil {
		return types.KvRef{}, fmt.Errorf("failed to unmarshal metadata for %x (len %d): %w", key, len(protoData), err)
	}

	// Convert back to Go struct
	metadata := types.KvRef{
		Key:     key, // We already know the key
		Created: protoMetadata.Created,
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

	for _, aliasBytes := range protoMetadata.Aliases {
		if len(aliasBytes) == 64 {
			var alias hash.Hash
			copy(alias[:], aliasBytes)
			metadata.Aliases = append(metadata.Aliases, alias)
		}
	}

	metaChunksKey := fmt.Sprintf("%s%x", META_CHUNK_HASH_PREFIX, key)
	metaItem, err := txn.Get([]byte(metaChunksKey))
	if err == nil {
		var raw []byte
		err = metaItem.Value(func(val []byte) error {
			raw = append([]byte(nil), val...)
			return nil
		})
		if err != nil {
			return types.KvRef{}, fmt.Errorf("failed to read metadata chunk hashes: %w", err)
		}

		hashes, err := deserializeHashesFromBytes(raw)
		if err != nil {
			return types.KvRef{}, fmt.Errorf("failed to parse metadata chunk hashes: %w", err)
		}
		metadata.MetaChunkHashes = hashes
	} else if err != badger.ErrKeyNotFound {
		return types.KvRef{}, fmt.Errorf("failed to load metadata chunk hashes: %w", err)
	}

	// Load content type if present (stored with suffix :ct)
	ctKey := fmt.Sprintf("%s%x:ct", METADATA_PREFIX, key)
	ctItem, err := txn.Get([]byte(ctKey))
	if err == nil {
		var val []byte
		err = ctItem.Value(func(v []byte) error {
			val = append([]byte(nil), v...)
			return nil
		})
		if err != nil {
			return types.KvRef{}, fmt.Errorf("failed to read content type for key %x: %w", key, err)
		}
		metadata.ContentType = string(val)
	} else if err != badger.ErrKeyNotFound {
		return types.KvRef{}, fmt.Errorf("failed to load content type: %w", err)
	}

	return metadata, nil
}

// loadSlicesByHash loads all RS slices for a given chunk hash
func (k *KV) loadSlicesByHash(txn *badger.Txn, chunkHash hash.Hash) ([]types.SealedSlice, error) {
	var records []types.SealedSlice

	// Create iterator to find all slices with this hash
	prefix := fmt.Sprintf("%s%x_", SLICE_PREFIX, chunkHash)
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
			return nil, fmt.Errorf("failed to read slice value: %w", err)
		}

		// Deserialize protobuf
		protoSlice := &pb.SliceRecordProto{}
		err = proto.Unmarshal(protoData, protoSlice)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal slice: %w", err)
		}

		// Convert back to Go struct
		record := types.SealedSlice{
			RSDataSlices:    uint8(protoSlice.RsDataSlices),
			RSParitySlices:  uint8(protoSlice.RsParitySlices),
			RSSliceIndex:    uint8(protoSlice.RsSliceIndex),
			Size:            protoSlice.Size,
			OriginalSize:    protoSlice.OriginalSize,
			EncapsulatedKey: protoSlice.EncapsulatedKey,
			Nonce:           protoSlice.Nonce,
			Payload:         protoSlice.Payload,
		}

		// Convert hashes
		if len(protoSlice.ChunkHash) == 64 {
			copy(record.ChunkHash[:], protoSlice.ChunkHash)
		}
		if len(protoSlice.SealedHash) == 64 {
			copy(record.SealedHash[:], protoSlice.SealedHash)
		}

		records = append(records, record)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("no slices found for hash %x", chunkHash)
	}

	return records, nil
}

func collectChildrenForTxn(txn *badger.Txn, parentKey hash.Hash) ([]hash.Hash, error) {
	prefixStr := fmt.Sprintf("%s%s:", PARENT_PREFIX, parentKey)
	prefix := []byte(prefixStr)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	var children []hash.Hash

	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().Key()
		keyStr := string(key)
		if len(keyStr) <= len(prefixStr) {
			continue
		}

		childHashHex := keyStr[len(prefixStr):]
		if len(childHashHex) != 128 {
			continue
		}

		childHash, err := hash.HashHexadecimal(childHashHex)
		if err != nil {
			continue
		}

		children = append(children, childHash)
	}

	return children, nil
}

// BatchReadData reads multiple data objects by their keys
func (k *KV) BatchReadData(keys []hash.Hash) ([]types.Data, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	atomic.AddUint64(&k.readCounter, uint64(len(keys)))

	var results []types.Data
	err := k.badgerDB.View(func(txn *badger.Txn) error {
		for _, key := range keys {
			canonicalKey := key
			if resolved, found, err := k.resolveAliasTxn(txn, key); err != nil {
				return fmt.Errorf("failed to resolve alias for key %x: %w", key, err)
			} else if found {
				canonicalKey = resolved
			}

			// Load metadata
			metadata, err := k.loadMetadata(txn, canonicalKey)
			if err != nil {
				return fmt.Errorf("failed to load metadata for key %x: %w", canonicalKey, err)
			}

			children, err := collectChildrenForTxn(txn, metadata.Key)
			if err != nil {
				return fmt.Errorf("failed to load children for key %x: %w", canonicalKey, err)
			}

			// Load all slices
			var contentSlices []types.SealedSlice
			for _, chunkHash := range metadata.ChunkHashes {
				slices, err := k.loadSlicesByHash(txn, chunkHash)
				if err != nil {
					return fmt.Errorf("failed to load slices for hash %x: %w", chunkHash, err)
				}
				contentSlices = append(contentSlices, slices...)
			}

			var metadataSlices []types.SealedSlice
			for _, chunkHash := range metadata.MetaChunkHashes {
				slices, err := k.loadSlicesByHash(txn, chunkHash)
				if err != nil {
					return fmt.Errorf("failed to load metadata slices for hash %x: %w", chunkHash, err)
				}
				metadataSlices = append(metadataSlices, slices...)
			}

			// Create KvDataLinked structure for decoding
			kvDataLinked := types.KvData{
				Key:             metadata.Key,
				Slices:          contentSlices,
				ChunkHashes:     metadata.ChunkHashes,
				MetaSlices:      metadataSlices,
				MetaChunkHashes: metadata.MetaChunkHashes,
				Parent:          metadata.Parent,
				Children:        children,
				Created:         metadata.Created,
				Aliases:         metadata.Aliases,
			}

			// Decode data
			decodedData, err := k.decodeDataPipeline(kvDataLinked)
			if err != nil {
				return fmt.Errorf("failed to decode data for key %x: %w", canonicalKey, err)
			}

			results = append(results, decodedData)
		}
		return nil
	})

	if err != nil {
		k.log.Error("Failed to batch read data", "error", err)
		return nil, fmt.Errorf("failed to batch read data: %w", err)
	}

	k.log.Debug("Successfully batch read data", "count", len(keys))
	return results, nil
}

// DataExists checks if data exists for the given key
func (k *KV) DataExists(key hash.Hash) (bool, error) {
	atomic.AddUint64(&k.readCounter, 1)

	var exists bool
	err := k.badgerDB.View(func(txn *badger.Txn) error {
		if resolved, found, err := k.resolveAliasTxn(txn, key); err != nil {
			return err
		} else if found {
			metadataKey := fmt.Sprintf("%s%x", METADATA_PREFIX, resolved)
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
		}

		count, err := k.getRefCountTxn(txn, key)
		if err != nil {
			return err
		}
		if count > 0 {
			exists = false
			return nil
		}

		metadataKey := fmt.Sprintf("%s%x", METADATA_PREFIX, key)
		_, err = txn.Get([]byte(metadataKey))
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

// GetDataInfo returns detailed information about a specific data entry
func (k *KV) GetDataInfo(key hash.Hash) (types.DataInfo, error) {
	atomic.AddUint64(&k.readCounter, 1)

	var info types.DataInfo

	err := k.badgerDB.View(func(txn *badger.Txn) error {
		// Load metadata
		metadata, err := k.loadMetadata(txn, key)
		if err != nil {
			return fmt.Errorf("failed to load metadata: %w", err)
		}

		// Initialize basic info
		info.Key = key
		info.KeyBase64 = base64.StdEncoding.EncodeToString(key[:])
		info.ChunkHashes = metadata.ChunkHashes
		info.MetaChunkHashes = metadata.MetaChunkHashes
		info.NumChunks = len(metadata.ChunkHashes)
		info.MetaNumChunks = len(metadata.MetaChunkHashes)

		// Load slices to get detailed information
		var allSlices []types.SealedSlice
		for _, chunkHash := range metadata.ChunkHashes {
			slices, err := k.loadSlicesByHash(txn, chunkHash)
			if err != nil {
				return fmt.Errorf("failed to load slices for hash %x: %w", chunkHash, err)
			}
			allSlices = append(allSlices, slices...)
		}

		var allMetaSlices []types.SealedSlice
		for _, chunkHash := range metadata.MetaChunkHashes {
			slices, err := k.loadSlicesByHash(txn, chunkHash)
			if err != nil {
				return fmt.Errorf("failed to load metadata slices for hash %x: %w", chunkHash, err)
			}
			allMetaSlices = append(allMetaSlices, slices...)
		}

		// Calculate sizes and analyze slices
		var totalStorageSize uint64
		var totalMetaStorageSize uint64
		chunkMap := make(map[hash.Hash][]types.SealedSlice)
		metaChunkMap := make(map[hash.Hash][]types.SealedSlice)

		// Group slices by hash
		for _, slice := range allSlices {
			chunkMap[slice.ChunkHash] = append(chunkMap[slice.ChunkHash], slice)
		}
		for _, slice := range allMetaSlices {
			metaChunkMap[slice.ChunkHash] = append(metaChunkMap[slice.ChunkHash], slice)
		}

		// Process each chunk group
		for _, chunkHash := range metadata.ChunkHashes {
			slices := chunkMap[chunkHash]
			if len(slices) == 0 {
				continue
			}

			// Get Reed-Solomon configuration from first slice
			firstSlice := slices[0]
			if info.RSDataSlices == 0 {
				info.RSDataSlices = firstSlice.RSDataSlices
				info.RSParitySlices = firstSlice.RSParitySlices
			}

			// Create chunk info
			chunkInfo := types.ChunkInfo{
				ChunkHash:       chunkHash,
				ChunkHashBase64: base64.StdEncoding.EncodeToString(chunkHash[:]),
				OriginalSize:    firstSlice.OriginalSize,
				SliceCount:      len(slices),
			}

			// Calculate compressed size (size before Reed-Solomon splitting)
			chunkInfo.CompressedSize = firstSlice.OriginalSize

			// Process each slice
			for _, slice := range slices {
				sliceInfo := types.SliceInfo{
					Index:       slice.RSSliceIndex,
					Size:        slice.Size,
					IsDataSlice: slice.RSSliceIndex < firstSlice.RSDataSlices,
				}
				chunkInfo.SliceDetails = append(chunkInfo.SliceDetails, sliceInfo)
				totalStorageSize += slice.Size
			}

			info.ChunkDetails = append(info.ChunkDetails, chunkInfo)
			info.ClearTextSize += chunkInfo.OriginalSize
		}

		info.StorageSize = totalStorageSize
		info.NumSlices = len(allSlices)

		// Process metadata slices
		for _, metaHash := range metadata.MetaChunkHashes {
			metaSlices := metaChunkMap[metaHash]
			if len(metaSlices) == 0 {
				continue
			}

			firstMeta := metaSlices[0]
			info.MetaClearTextSize += firstMeta.OriginalSize

			for _, slice := range metaSlices {
				totalMetaStorageSize += slice.Size
			}
		}

		info.MetaStorageSize = totalMetaStorageSize
		info.MetaNumSlices = len(allMetaSlices)

		if len(allMetaSlices) > 0 && len(metadata.MetaChunkHashes) > 0 {
			metaPayload, _, _, err := pipeline.ReconstructPayload(allMetaSlices, metadata.MetaChunkHashes, k.crypt)
			if err != nil {
				return fmt.Errorf("failed to reconstruct metadata payload: %w", err)
			}
			info.MetaData = metaPayload
			info.MetaClearTextSize = uint64(len(metaPayload))
		}

		return nil
	})

	if err != nil {
		return types.DataInfo{}, err
	}

	return info, nil
}

func (k *KV) decodeDataPipeline(kvDataLinked types.KvData) (types.Data, error) {
	content, rsData, rsParity, err := pipeline.ReconstructPayload(kvDataLinked.Slices, kvDataLinked.ChunkHashes, k.crypt)
	if err != nil {
		return types.Data{}, err
	}

	metadata, _, _, err := pipeline.ReconstructPayload(kvDataLinked.MetaSlices, kvDataLinked.MetaChunkHashes, k.crypt)
	if err != nil {
		return types.Data{}, err
	}

	return types.Data{
		Key:            kvDataLinked.Key,
		Meta:           metadata,
		Content:        content,
		Parent:         kvDataLinked.Parent,
		Children:       kvDataLinked.Children,
		RSDataSlices:   rsData,
		RSParitySlices: rsParity,
		Created:        kvDataLinked.Created,
		Aliases:        kvDataLinked.Aliases,
		ContentType:    kvDataLinked.ContentType,
	}, nil
}
