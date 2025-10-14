package ouroboroskv

import (
	"fmt"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/i5heu/ouroboros-crypt/hash"
)

// DeleteData removes data and all its associated slices from the key-value store
func (k *KV) DeleteData(key hash.Hash) error {
	atomic.AddUint64(&k.writeCounter, 1)

	err := k.badgerDB.Update(func(txn *badger.Txn) error {
		// First, load metadata to find all slices that need to be deleted
		metadata, err := k.loadMetadata(txn, key)
		if err != nil {
			return fmt.Errorf("failed to load metadata for key %x: %w", key, err)
		}

		// Delete all slices associated with this data
		for _, chunkHash := range metadata.ChunkHashes {
			slices, err := k.loadSlicesByHash(txn, chunkHash)
			if err != nil {
				return fmt.Errorf("failed to load slices for hash %x: %w", chunkHash, err)
			}

			// Delete each slice record
			for _, slice := range slices {
				sliceKey := fmt.Sprintf("%s%x_%d", SLICE_PREFIX, slice.ChunkHash, slice.RSSliceIndex)
				err := txn.Delete([]byte(sliceKey))
				if err != nil {
					return fmt.Errorf("failed to delete slice %s: %w", sliceKey, err)
				}
			}
		}

		// Delete metadata
		metadataKey := fmt.Sprintf("%s%x", METADATA_PREFIX, key)
		err = txn.Delete([]byte(metadataKey))
		if err != nil {
			return fmt.Errorf("failed to delete metadata for key %x: %w", key, err)
		}

		metaChunksKey := fmt.Sprintf("%s%x", META_CHUNK_HASH_PREFIX, key)
		err = txn.Delete([]byte(metaChunksKey))
		if err != nil && err != badger.ErrKeyNotFound {
			return fmt.Errorf("failed to delete metadata chunk hashes for key %x: %w", key, err)
		}

		return nil
	})

	if err != nil {
		log.Error("Failed to delete data", "key", fmt.Sprintf("%x", key), "error", err)
		return fmt.Errorf("failed to delete data: %w", err)
	}

	log.Debug("Successfully deleted data", "key", fmt.Sprintf("%x", key))
	return nil
}
