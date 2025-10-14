package ouroboroskv

import (
	"bytes"
	"encoding/binary"
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
	METADATA_PREFIX        = "meta:"             // For KvDataHash metadata
	META_CHUNK_HASH_PREFIX = "meta:chunkhashes:" // For metadata chunk hash ordering
	SLICE_PREFIX           = "slice:"            // For individual SliceRecord payloads
	PARENT_PREFIX          = "parent:"           // For parent relationships: parent_key -> child_key
	CHILD_PREFIX           = "child:"            // For child relationships: child_key -> parent_key
)

// WriteData encodes and stores the given Data in the key-value store
// It uses the encoding pipeline to create encrypted, compressed, and erasure-coded chunks
func (k *KV) WriteData(data Data) (hash.Hash, error) {
	atomic.AddUint64(&k.writeCounter, 1)

	if !isEmptyHash(data.Key) {
		return hash.Hash{}, fmt.Errorf("data key must be zero value; it will be generated from content")
	}

	if data.Created == 0 {
		data.Created = time.Now().Unix()
	}

	data.Key = computeDataKey(data) // Calculate the key hash from all relevant fields

	// Use the encoding pipeline to process the data
	encoded, err := k.encodeDataPipeline(data)
	if err != nil {
		return hash.Hash{}, fmt.Errorf("failed to encode data: %w", err)
	}

	// Group content slices by chunk hash
	contentHashes := make([]hash.Hash, 0, len(encoded.Slices))
	contentSliceMap := make(map[hash.Hash][]SliceRecord)
	for _, slice := range encoded.Slices {
		if _, exists := contentSliceMap[slice.ChunkHash]; !exists {
			contentHashes = append(contentHashes, slice.ChunkHash)
		}
		contentSliceMap[slice.ChunkHash] = append(contentSliceMap[slice.ChunkHash], slice)
	}

	// Group metadata slices by chunk hash
	metaHashes := make([]hash.Hash, 0, len(encoded.MetaSlices))
	metaSliceMap := make(map[hash.Hash][]SliceRecord)
	for _, slice := range encoded.MetaSlices {
		if _, exists := metaSliceMap[slice.ChunkHash]; !exists {
			metaHashes = append(metaHashes, slice.ChunkHash)
		}
		metaSliceMap[slice.ChunkHash] = append(metaSliceMap[slice.ChunkHash], slice)
	}

	metadata := kvDataHash{
		Key:             encoded.Key,
		ChunkHashes:     contentHashes,
		MetaChunkHashes: metaHashes,
		Parent:          encoded.Parent,
		Created:         encoded.Created,
		Aliases:         encoded.Aliases,
	}

	// Use WriteBatch for better handling of large transactions
	wb := k.badgerDB.NewWriteBatch()
	defer wb.Cancel()

	// Store metadata
	err = k.storeMetadataWithBatch(wb, metadata)
	if err != nil {
		return hash.Hash{}, fmt.Errorf("failed to store metadata: %w", err)
	}

	if err := k.storeMetadataChunkHashesWithBatch(wb, metadata.Key, metadata.MetaChunkHashes); err != nil {
		return hash.Hash{}, fmt.Errorf("failed to store metadata chunk hashes: %w", err)
	}

	// Store parent-child relationships
	err = k.storeParentChildRelationships(wb, metadata.Key, metadata.Parent, encoded.Children)
	if err != nil {
		return hash.Hash{}, fmt.Errorf("failed to store parent-child relationships: %w", err)
	}

	// Store all content slices
	for _, slices := range contentSliceMap {
		for _, slice := range slices {
			if err := k.storeSliceWithBatch(wb, slice); err != nil {
				return hash.Hash{}, fmt.Errorf("failed to store slice: %w", err)
			}
		}
	}

	// Store all metadata slices
	for _, slices := range metaSliceMap {
		for _, slice := range slices {
			if err := k.storeSliceWithBatch(wb, slice); err != nil {
				return hash.Hash{}, fmt.Errorf("failed to store metadata slice: %w", err)
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
	// Convert to protobuf
	protoMetadata := &pb.KvDataHashProto{
		Key:     metadata.Key[:],
		Parent:  metadata.Parent[:],
		Created: metadata.Created,
	}

	// Convert chunk hashes
	for _, chunkHash := range metadata.ChunkHashes {
		protoMetadata.ChunkHashes = append(protoMetadata.ChunkHashes, chunkHash[:])
	}

	for _, alias := range metadata.Aliases {
		protoMetadata.Aliases = append(protoMetadata.Aliases, alias[:])
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

// storeSlice serializes and stores a SliceRecord
func (k *KV) storeSlice(txn *badger.Txn, slice SliceRecord) error {
	// Convert to protobuf
	protoSlice := &pb.SliceRecordProto{
		ChunkHash:       slice.ChunkHash[:],
		SealedHash:      slice.SealedHash[:],
		RsDataSlices:    uint32(slice.RSDataSlices),
		RsParitySlices:  uint32(slice.RSParitySlices),
		RsSliceIndex:    uint32(slice.RSSliceIndex),
		Size:            slice.Size,
		OriginalSize:    slice.OriginalSize,
		EncapsulatedKey: slice.EncapsulatedKey,
		Nonce:           slice.Nonce,
		Payload:         slice.Payload,
	}

	// Serialize to protobuf
	data, err := proto.Marshal(protoSlice)
	if err != nil {
		return fmt.Errorf("failed to marshal chunk: %w", err)
	}

	// Create key with chunk prefix and unique identifier
	// Use chunk hash + Reed-Solomon index to create unique keys for each slice
	key := fmt.Sprintf("%s%x_%d", SLICE_PREFIX, slice.ChunkHash, slice.RSSliceIndex)

	return txn.Set([]byte(key), data)
}

// storeMetadataWithBatch serializes and stores KvDataHash metadata using WriteBatch
func (k *KV) storeMetadataWithBatch(wb *badger.WriteBatch, metadata kvDataHash) error {
	// Convert to protobuf
	protoMetadata := &pb.KvDataHashProto{
		Key:     metadata.Key[:],
		Parent:  metadata.Parent[:],
		Created: metadata.Created,
	}

	// Convert chunk hashes
	for _, chunkHash := range metadata.ChunkHashes {
		protoMetadata.ChunkHashes = append(protoMetadata.ChunkHashes, chunkHash[:])
	}

	for _, alias := range metadata.Aliases {
		protoMetadata.Aliases = append(protoMetadata.Aliases, alias[:])
	}

	// Serialize to protobuf
	data, err := proto.Marshal(protoMetadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Create key with metadata prefix
	key := fmt.Sprintf("%s%x", METADATA_PREFIX, metadata.Key)

	return wb.Set([]byte(key), data)
}

func (k *KV) storeMetadataChunkHashesWithBatch(wb *badger.WriteBatch, key hash.Hash, hashes []hash.Hash) error {
	metaKey := fmt.Sprintf("%s%x", META_CHUNK_HASH_PREFIX, key)
	if len(hashes) == 0 {
		return nil
	}

	payload := serializeHashesToBytes(hashes)
	return wb.Set([]byte(metaKey), payload)
}

func (k *KV) storeMetadataChunkHashesTxn(txn *badger.Txn, key hash.Hash, hashes []hash.Hash) error {
	metaKey := fmt.Sprintf("%s%x", META_CHUNK_HASH_PREFIX, key)
	if len(hashes) == 0 {
		return nil
	}

	payload := serializeHashesToBytes(hashes)
	return txn.Set([]byte(metaKey), payload)
}

// storeSliceWithBatch serializes and stores a SliceRecord using WriteBatch
func (k *KV) storeSliceWithBatch(wb *badger.WriteBatch, slice SliceRecord) error {
	// Convert to protobuf
	protoSlice := &pb.SliceRecordProto{
		ChunkHash:       slice.ChunkHash[:],
		SealedHash:      slice.SealedHash[:],
		RsDataSlices:    uint32(slice.RSDataSlices),
		RsParitySlices:  uint32(slice.RSParitySlices),
		RsSliceIndex:    uint32(slice.RSSliceIndex),
		Size:            slice.Size,
		OriginalSize:    slice.OriginalSize,
		EncapsulatedKey: slice.EncapsulatedKey,
		Nonce:           slice.Nonce,
		Payload:         slice.Payload,
	}

	// Serialize to protobuf
	data, err := proto.Marshal(protoSlice)
	if err != nil {
		return fmt.Errorf("failed to marshal chunk: %w", err)
	}

	// Create key with chunk prefix and unique identifier
	// Use chunk hash + Reed-Solomon index to create unique keys for each slice
	key := fmt.Sprintf("%s%x_%d", SLICE_PREFIX, slice.ChunkHash, slice.RSSliceIndex)

	return wb.Set([]byte(key), data)
}

// storeParentChildRelationships stores bidirectional parent-child relationships in BadgerDB
func (k *KV) storeParentChildRelationships(wb *badger.WriteBatch, dataKey, parent hash.Hash, children []hash.Hash) error {
	// Store parent -> child relationship (if this data has a parent)
	if !isEmptyHash(parent) {
		// Store: parent:PARENT_HASH -> child:DATA_KEY
		parentToChildKey := fmt.Sprintf("%s%s:%s", PARENT_PREFIX, parent, dataKey)
		err := wb.Set([]byte(parentToChildKey), []byte(""))
		if err != nil {
			return fmt.Errorf("failed to store parent->child relationship: %w", err)
		}

		// Store: child:DATA_KEY -> parent:PARENT_HASH
		childToParentKey := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, dataKey, parent)
		err = wb.Set([]byte(childToParentKey), []byte(""))
		if err != nil {
			return fmt.Errorf("failed to store child->parent relationship: %w", err)
		}
	}

	// Store child -> parent relationships (for each child this data has)
	for _, child := range children {
		if !isEmptyHash(child) {
			// Store: parent:DATA_KEY -> child:CHILD_HASH
			parentToChildKey := fmt.Sprintf("%s%s:%s", PARENT_PREFIX, dataKey, child)
			err := wb.Set([]byte(parentToChildKey), []byte(""))
			if err != nil {
				return fmt.Errorf("failed to store parent->child relationship: %w", err)
			}

			// Store: child:CHILD_HASH -> parent:DATA_KEY
			childToParentKey := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, child, dataKey)
			err = wb.Set([]byte(childToParentKey), []byte(""))
			if err != nil {
				return fmt.Errorf("failed to store child->parent relationship: %w", err)
			}
		}
	}

	return nil
}

// isEmptyHash checks if a hash is the zero value
func isEmptyHash(h hash.Hash) bool {
	var empty hash.Hash
	return h == empty
}

func canonicalDataKeyPayload(data Data) []byte {
	var buf bytes.Buffer
	writeBytesWithLength(&buf, data.MetaData)
	writeBytesWithLength(&buf, data.Content)
	buf.Write(data.Parent[:])
	buf.WriteByte(data.RSDataSlices)
	buf.WriteByte(data.RSParitySlices)

	var createdBytes [8]byte
	binary.BigEndian.PutUint64(createdBytes[:], uint64(data.Created))
	buf.Write(createdBytes[:])

	var aliasCount [4]byte
	binary.BigEndian.PutUint32(aliasCount[:], uint32(len(data.Aliases)))
	buf.Write(aliasCount[:])
	for _, alias := range data.Aliases {
		buf.Write(alias[:])
	}

	return buf.Bytes()
}

func writeBytesWithLength(buf *bytes.Buffer, payload []byte) {
	var lengthBytes [8]byte
	binary.BigEndian.PutUint64(lengthBytes[:], uint64(len(payload)))
	buf.Write(lengthBytes[:])
	if len(payload) > 0 {
		buf.Write(payload)
	}
}

// BatchWriteData writes multiple Data objects in a single batch operation
func (k *KV) BatchWriteData(dataList []Data) ([]hash.Hash, error) {
	if len(dataList) == 0 {
		return []hash.Hash{}, nil
	}

	atomic.AddUint64(&k.writeCounter, uint64(len(dataList)))

	// Process all data through encoding pipeline first
	var (
		allMetadata      []kvDataHash
		allSlices        []SliceRecord
		metadataChildren [][]hash.Hash
		keys             []hash.Hash
	)

	for _, data := range dataList {

		if !isEmptyHash(data.Key) {
			return nil, fmt.Errorf("data key must be zero value; it will be generated from content")
		}

		if data.Created == 0 {
			data.Created = time.Now().Unix()
		}

		data.Key = computeDataKey(data)

		encoded, err := k.encodeDataPipeline(data)
		if err != nil {
			return nil, fmt.Errorf("failed to encode data with key %x: %w", data.Key, err)
		}
		keys = append(keys, data.Key)

		// Create metadata
		contentHashes := make([]hash.Hash, 0, len(encoded.Slices))
		contentSeen := make(map[hash.Hash]bool)
		for _, slice := range encoded.Slices {
			if !contentSeen[slice.ChunkHash] {
				contentHashes = append(contentHashes, slice.ChunkHash)
				contentSeen[slice.ChunkHash] = true
			}
			allSlices = append(allSlices, slice)
		}

		metaHashes := make([]hash.Hash, 0, len(encoded.MetaSlices))
		metaSeen := make(map[hash.Hash]bool)
		for _, slice := range encoded.MetaSlices {
			if !metaSeen[slice.ChunkHash] {
				metaHashes = append(metaHashes, slice.ChunkHash)
				metaSeen[slice.ChunkHash] = true
			}
			allSlices = append(allSlices, slice)
		}

		metadata := kvDataHash{
			Key:             encoded.Key,
			ChunkHashes:     contentHashes,
			MetaChunkHashes: metaHashes,
			Parent:          encoded.Parent,
			Created:         encoded.Created,
			Aliases:         encoded.Aliases,
		}
		allMetadata = append(allMetadata, metadata)
		metadataChildren = append(metadataChildren, encoded.Children)
	}

	// Perform batch write
	err := k.badgerDB.Update(func(txn *badger.Txn) error {
		// Store all metadata
		for idx, metadata := range allMetadata {
			err := k.storeMetadata(txn, metadata)
			if err != nil {
				return fmt.Errorf("failed to store metadata for key %x: %w", metadata.Key, err)
			}

			if err := k.storeMetadataChunkHashesTxn(txn, metadata.Key, metadata.MetaChunkHashes); err != nil {
				return fmt.Errorf("failed to store metadata chunk hashes for key %x: %w", metadata.Key, err)
			}

			// Store parent-child relationships
			err = k.storeParentChildRelationshipsTxn(txn, metadata.Key, metadata.Parent, metadataChildren[idx])
			if err != nil {
				return fmt.Errorf("failed to store parent-child relationships for key %x: %w", metadata.Key, err)
			}
		}

		// Store all slices
		for _, slice := range allSlices {
			err := k.storeSlice(txn, slice)
			if err != nil {
				return fmt.Errorf("failed to store slice for hash %x: %w", slice.ChunkHash, err)
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
func (k *KV) storeParentChildRelationshipsTxn(txn *badger.Txn, dataKey, parent hash.Hash, children []hash.Hash) error {
	// Store parent -> child relationship (if this data has a parent)
	if !isEmptyHash(parent) {
		// Store: parent:PARENT_HASH -> child:DATA_KEY
		parentToChildKey := fmt.Sprintf("%s%s:%s", PARENT_PREFIX, parent, dataKey)
		err := txn.Set([]byte(parentToChildKey), []byte{})
		if err != nil {
			return fmt.Errorf("failed to store parent->child relationship: %w", err)
		}

		// Store: child:DATA_KEY -> parent:PARENT_HASH
		childToParentKey := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, dataKey, parent)
		err = txn.Set([]byte(childToParentKey), []byte{})
		if err != nil {
			return fmt.Errorf("failed to store child->parent relationship: %w", err)
		}
	}

	// Store child -> parent relationships (for each child this data has)
	for _, child := range children {
		if !isEmptyHash(child) {
			// Store: parent:DATA_KEY -> child:CHILD_HASH
			parentToChildKey := fmt.Sprintf("%s%s:%s", PARENT_PREFIX, dataKey, child)
			err := txn.Set([]byte(parentToChildKey), []byte{})
			if err != nil {
				return fmt.Errorf("failed to store parent->child relationship: %w", err)
			}

			// Store: child:CHILD_HASH -> parent:DATA_KEY
			childToParentKey := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, child, dataKey)
			err = txn.Set([]byte(childToParentKey), []byte{})
			if err != nil {
				return fmt.Errorf("failed to store child->parent relationship: %w", err)
			}
		}
	}

	return nil
}
