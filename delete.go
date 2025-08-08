package ouroboroskv

import (
	"fmt"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/i5heu/ouroboros-kv/storage"
)

// DeleteData removes data and all its associated chunks from the key-value store
func (k *KV) DeleteData(key hash.Hash) error {
	atomic.AddUint64(&k.writeCounter, 1)

	err := k.badgerDB.Update(func(txn *badger.Txn) error {
		// First, load metadata to find all chunks that need to be deleted
		metadata, err := storage.LoadMetadata(txn, key)
		if err != nil {
			return fmt.Errorf("failed to load metadata for key %x: %w", key, err)
		}

		// Delete all chunks associated with this data
		for _, chunkHash := range metadata.ShardHashes {
			chunks, err := storage.LoadShardsByHash(txn, chunkHash)
			if err != nil {
				return fmt.Errorf("failed to load chunks for hash %x: %w", chunkHash, err)
			}

			// Delete each chunk shard
			for _, chunk := range chunks {
				chunkKey := fmt.Sprintf("%s%x_%d", storage.ChunkPrefix, chunk.ChunkHash, chunk.ReedSolomonIndex)
				err := txn.Delete([]byte(chunkKey))
				if err != nil {
					return fmt.Errorf("failed to delete chunk %s: %w", chunkKey, err)
				}
			}
		}

		// Delete metadata
		metadataKey := fmt.Sprintf("%s%x", storage.MetadataPrefix, key)
		err = txn.Delete([]byte(metadataKey))
		if err != nil {
			return fmt.Errorf("failed to delete metadata for key %x: %w", key, err)
		}

		return nil
	})

	if err != nil {
		log.Errorf("Failed to delete data for key %x: %v", key, err)
		return fmt.Errorf("failed to delete data: %w", err)
	}

	log.Debugf("Successfully deleted data with key %x", key)
	return nil
}
