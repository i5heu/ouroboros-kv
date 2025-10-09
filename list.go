package ouroboroskv

import (
	"encoding/base64"
	"fmt"
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
	"github.com/i5heu/ouroboros-crypt/hash"
)

// DataInfo represents detailed information about stored data
type DataInfo struct {
	Key                     hash.Hash   // The data key
	KeyBase64               string      // Base64 encoded key for display
	ChunkHashes             []hash.Hash // Hashes of all chunks
	MetaChunkHashes         []hash.Hash // Hashes of all metadata chunks
	ClearTextSize           uint64      // Original uncompressed data size
	StorageSize             uint64      // Total size on storage (sum of all shards)
	MetaClearTextSize       uint64      // Metadata clear text size
	MetaStorageSize         uint64      // Total metadata storage size
	NumChunks               int         // Number of logical chunks
	NumShards               int         // Total number of Reed-Solomon shards
	MetaNumChunks           int         // Number of metadata chunks
	MetaNumShards           int         // Total number of metadata shards
	ReedSolomonShards       uint8       // Data shards per chunk
	ReedSolomonParityShards uint8       // Parity shards per chunk
	ChunkDetails            []ChunkInfo // Detailed information per chunk
	MetaData                []byte      // Decoded metadata payload
}

// ChunkInfo represents information about a single chunk and its shards
type ChunkInfo struct {
	ChunkHash       hash.Hash   // Hash of the original chunk
	ChunkHashBase64 string      // Base64 encoded chunk hash
	OriginalSize    uint64      // Size before compression/encryption
	CompressedSize  uint64      // Size after compression but before Reed-Solomon
	ShardCount      int         // Number of shards for this chunk
	ShardDetails    []ShardInfo // Information about each shard
}

// ShardInfo represents information about a single Reed-Solomon shard
type ShardInfo struct {
	Index       uint8  // Reed-Solomon index
	Size        uint64 // Size of this shard on storage
	IsDataShard bool   // True if data shard, false if parity shard
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

// GetDataInfo returns detailed information about a specific data entry
func (k *KV) GetDataInfo(key hash.Hash) (DataInfo, error) {
	atomic.AddUint64(&k.readCounter, 1)

	var info DataInfo

	err := k.badgerDB.View(func(txn *badger.Txn) error {
		// Load metadata
		metadata, err := k.loadMetadata(txn, key)
		if err != nil {
			return fmt.Errorf("failed to load metadata: %w", err)
		}

		// Initialize basic info
		info.Key = key
		info.KeyBase64 = base64.StdEncoding.EncodeToString(key[:])
		info.ChunkHashes = metadata.ShardHashes
		info.MetaChunkHashes = metadata.MetaShardHashes
		info.NumChunks = len(metadata.ShardHashes)
		info.MetaNumChunks = len(metadata.MetaShardHashes)

		// Load chunks to get detailed information
		var allChunks []kvDataShard
		for _, chunkHash := range metadata.ShardHashes {
			chunks, err := k.loadChunksByHash(txn, chunkHash)
			if err != nil {
				return fmt.Errorf("failed to load chunks for hash %x: %w", chunkHash, err)
			}
			allChunks = append(allChunks, chunks...)
		}

		var allMetaChunks []kvDataShard
		for _, chunkHash := range metadata.MetaShardHashes {
			chunks, err := k.loadChunksByHash(txn, chunkHash)
			if err != nil {
				return fmt.Errorf("failed to load metadata chunks for hash %x: %w", chunkHash, err)
			}
			allMetaChunks = append(allMetaChunks, chunks...)
		}

		// Calculate sizes and analyze chunks
		var totalStorageSize uint64
		var totalMetaStorageSize uint64
		chunkMap := make(map[hash.Hash][]kvDataShard)
		metaChunkMap := make(map[hash.Hash][]kvDataShard)

		// Group chunks by hash
		for _, chunk := range allChunks {
			chunkMap[chunk.ChunkHash] = append(chunkMap[chunk.ChunkHash], chunk)
		}
		for _, chunk := range allMetaChunks {
			metaChunkMap[chunk.ChunkHash] = append(metaChunkMap[chunk.ChunkHash], chunk)
		}

		// Process each chunk group
		for _, chunkHash := range metadata.ShardHashes {
			chunks := chunkMap[chunkHash]
			if len(chunks) == 0 {
				continue
			}

			// Get Reed-Solomon configuration from first chunk
			firstChunk := chunks[0]
			if info.ReedSolomonShards == 0 {
				info.ReedSolomonShards = firstChunk.ReedSolomonShards
				info.ReedSolomonParityShards = firstChunk.ReedSolomonParityShards
			}

			// Create chunk info
			chunkInfo := ChunkInfo{
				ChunkHash:       chunkHash,
				ChunkHashBase64: base64.StdEncoding.EncodeToString(chunkHash[:]),
				OriginalSize:    firstChunk.OriginalSize,
				ShardCount:      len(chunks),
			}

			// Calculate compressed size (size before Reed-Solomon splitting)
			chunkInfo.CompressedSize = firstChunk.OriginalSize

			// Process each shard
			for _, chunk := range chunks {
				shardInfo := ShardInfo{
					Index:       chunk.ReedSolomonIndex,
					Size:        chunk.Size,
					IsDataShard: chunk.ReedSolomonIndex < firstChunk.ReedSolomonShards,
				}
				chunkInfo.ShardDetails = append(chunkInfo.ShardDetails, shardInfo)
				totalStorageSize += chunk.Size
			}

			info.ChunkDetails = append(info.ChunkDetails, chunkInfo)
			info.ClearTextSize += chunkInfo.OriginalSize
		}

		info.StorageSize = totalStorageSize
		info.NumShards = len(allChunks)

		// Process metadata chunks
		for _, metaHash := range metadata.MetaShardHashes {
			metaChunks := metaChunkMap[metaHash]
			if len(metaChunks) == 0 {
				continue
			}

			firstMeta := metaChunks[0]
			info.MetaClearTextSize += firstMeta.OriginalSize

			for _, chunk := range metaChunks {
				totalMetaStorageSize += chunk.Size
			}
		}

		info.MetaStorageSize = totalMetaStorageSize
		info.MetaNumShards = len(allMetaChunks)

		if len(allMetaChunks) > 0 && len(metadata.MetaShardHashes) > 0 {
			metaPayload, _, _, err := k.reconstructPayload(allMetaChunks, metadata.MetaShardHashes)
			if err != nil {
				return fmt.Errorf("failed to reconstruct metadata payload: %w", err)
			}
			info.MetaData = metaPayload
			info.MetaClearTextSize = uint64(len(metaPayload))
		}

		return nil
	})

	if err != nil {
		return DataInfo{}, err
	}

	return info, nil
}

// FormatDataInfo returns a human-readable string representation of DataInfo
func (info DataInfo) FormatDataInfo() string {
	output := fmt.Sprintf("Data Key: %s\n", info.KeyBase64)
	output += fmt.Sprintf("Clear Text Size: %s (%d bytes)\n", formatBytes(info.ClearTextSize), info.ClearTextSize)
	output += fmt.Sprintf("Storage Size: %s (%d bytes)\n", formatBytes(info.StorageSize), info.StorageSize)
	output += fmt.Sprintf("Compression Ratio: %.2fx\n", float64(info.StorageSize)/float64(info.ClearTextSize))
	output += fmt.Sprintf("Chunks: %d, Total Shards: %d\n", info.NumChunks, info.NumShards)
	if info.MetaNumChunks > 0 {
		output += fmt.Sprintf("Metadata Size: %s (%d bytes)\n", formatBytes(info.MetaClearTextSize), info.MetaClearTextSize)
		output += fmt.Sprintf("Metadata Storage Size: %s (%d bytes)\n", formatBytes(info.MetaStorageSize), info.MetaStorageSize)
		output += fmt.Sprintf("Metadata Chunks: %d, Metadata Shards: %d\n", info.MetaNumChunks, info.MetaNumShards)
	}
	output += fmt.Sprintf("Reed-Solomon Config: %d data + %d parity shards per chunk\n\n",
		info.ReedSolomonShards, info.ReedSolomonParityShards)

	for i, chunk := range info.ChunkDetails {
		output += fmt.Sprintf("  Chunk %d:\n", i+1)
		output += fmt.Sprintf("    Hash: %s\n", chunk.ChunkHashBase64)
		output += fmt.Sprintf("    Original Size: %s (%d bytes)\n", formatBytes(chunk.OriginalSize), chunk.OriginalSize)
		output += fmt.Sprintf("    Shards: %d\n", chunk.ShardCount)

		for _, shard := range chunk.ShardDetails {
			shardType := "data"
			if !shard.IsDataShard {
				shardType = "parity"
			}
			output += fmt.Sprintf("      Shard %d (%s): %s (%d bytes)\n",
				shard.Index, shardType, formatBytes(shard.Size), shard.Size)
		}
		output += "\n"
	}

	return output
}

// formatBytes returns a human-readable byte size
func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
