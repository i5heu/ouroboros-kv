package ouroboroskv

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	pb "github.com/i5heu/ouroboros-kv/proto"
	"google.golang.org/protobuf/proto"
)

// ChunkInfo represents information about a single chunk and its slices
type ChunkInfo struct {
	ChunkHash       hash.Hash   // Hash of the original chunk
	ChunkHashBase64 string      // Base64 encoded chunk hash
	OriginalSize    uint64      // Size before compression/encryption
	CompressedSize  uint64      // Size after compression but before Reed-Solomon
	SliceCount      int         // Number of slices for this chunk
	SliceDetails    []SliceInfo // Information about each slice
}

// SliceInfo represents information about a single Reed-Solomon slice
type SliceInfo struct {
	Index       uint8  // Reed-Solomon index
	Size        uint64 // Size of this slice on storage
	IsDataSlice bool   // True if data slice, false if parity slice
}

// ListStoredData returns detailed information about all stored data
func (k *KV) ListStoredData() ([]DataInfo, error) {
	var dataInfos []DataInfo

	// Get all keys
	keys, err := k.ListKeys()
	if err != nil {
		return nil, fmt.Errorf("failed to list keys: %w", err)
	}

	// For each key, get detailed information
	for _, key := range keys {
		info, err := k.GetDataInfo(key)
		if err != nil {
			log.Error("Failed to get info for key", "key", fmt.Sprintf("%x", key), "error", err)
			continue
		}
		dataInfos = append(dataInfos, info)
	}

	return dataInfos, nil
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
			if bytes.HasPrefix(it.Item().Key(), []byte(META_CHUNK_HASH_PREFIX)) {
				continue
			}
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
		chunkHashPrefix := []byte(META_CHUNK_HASH_PREFIX)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			keyBytes := item.Key()
			if bytes.HasPrefix(keyBytes, chunkHashPrefix) {
				continue
			}

			var valueCopy []byte
			if err := item.Value(func(val []byte) error {
				valueCopy = append([]byte(nil), val...)
				return nil
			}); err != nil {
				return fmt.Errorf("failed to read metadata value: %w", err)
			}

			protoMetadata := &pb.KvDataHashProto{}
			if err := proto.Unmarshal(valueCopy, protoMetadata); err != nil {
				return fmt.Errorf("failed to unmarshal metadata for %x: %w", keyBytes, err)
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
			keyBytesPrefix := item.Key()
			if len(keyBytesPrefix) > len(METADATA_PREFIX) {
				hashHex := string(keyBytesPrefix[len(METADATA_PREFIX):])
				hashValue, err := hash.HashHexadecimal(hashHex)
				if err == nil {
					roots = append(roots, hashValue)
				}
			}
		}
		return nil
	})

	return roots, err
}
