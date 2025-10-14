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
	Key               hash.Hash   // The data key
	KeyBase64         string      // Base64 encoded key for display
	ChunkHashes       []hash.Hash // Hashes of all chunks
	MetaChunkHashes   []hash.Hash // Hashes of all metadata chunks
	ClearTextSize     uint64      // Original uncompressed data size
	StorageSize       uint64      // Total size on storage (sum of all slices)
	MetaClearTextSize uint64      // Metadata clear text size
	MetaStorageSize   uint64      // Total metadata storage size
	NumChunks         int         // Number of logical chunks
	NumSlices         int         // Total number of Reed-Solomon slices
	MetaNumChunks     int         // Number of metadata chunks
	MetaNumSlices     int         // Total number of metadata slices
	RSDataSlices      uint8       // Data slices per chunk
	RSParitySlices    uint8       // Parity slices per chunk
	ChunkDetails      []ChunkInfo // Detailed information per chunk
	MetaData          []byte      // Decoded metadata payload
}

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
		info.ChunkHashes = metadata.ChunkHashes
		info.MetaChunkHashes = metadata.MetaChunkHashes
		info.NumChunks = len(metadata.ChunkHashes)
		info.MetaNumChunks = len(metadata.MetaChunkHashes)

		// Load slices to get detailed information
		var allSlices []SliceRecord
		for _, chunkHash := range metadata.ChunkHashes {
			slices, err := k.loadSlicesByHash(txn, chunkHash)
			if err != nil {
				return fmt.Errorf("failed to load slices for hash %x: %w", chunkHash, err)
			}
			allSlices = append(allSlices, slices...)
		}

		var allMetaSlices []SliceRecord
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
		chunkMap := make(map[hash.Hash][]SliceRecord)
		metaChunkMap := make(map[hash.Hash][]SliceRecord)

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
			chunkInfo := ChunkInfo{
				ChunkHash:       chunkHash,
				ChunkHashBase64: base64.StdEncoding.EncodeToString(chunkHash[:]),
				OriginalSize:    firstSlice.OriginalSize,
				SliceCount:      len(slices),
			}

			// Calculate compressed size (size before Reed-Solomon splitting)
			chunkInfo.CompressedSize = firstSlice.OriginalSize

			// Process each slice
			for _, slice := range slices {
				sliceInfo := SliceInfo{
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
			metaPayload, _, _, err := k.reconstructPayload(allMetaSlices, metadata.MetaChunkHashes)
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
	output += fmt.Sprintf("Chunks: %d, Total Slices: %d\n", info.NumChunks, info.NumSlices)
	if info.MetaNumChunks > 0 {
		output += fmt.Sprintf("Metadata Size: %s (%d bytes)\n", formatBytes(info.MetaClearTextSize), info.MetaClearTextSize)
		output += fmt.Sprintf("Metadata Storage Size: %s (%d bytes)\n", formatBytes(info.MetaStorageSize), info.MetaStorageSize)
		output += fmt.Sprintf("Metadata Chunks: %d, Metadata Slices: %d\n", info.MetaNumChunks, info.MetaNumSlices)
	}
	output += fmt.Sprintf("Reed-Solomon Config: %d data + %d parity slices per chunk\n\n",
		info.RSDataSlices, info.RSParitySlices)

	for i, chunk := range info.ChunkDetails {
		output += fmt.Sprintf("  Chunk %d:\n", i+1)
		output += fmt.Sprintf("    Hash: %s\n", chunk.ChunkHashBase64)
		output += fmt.Sprintf("    Original Size: %s (%d bytes)\n", formatBytes(chunk.OriginalSize), chunk.OriginalSize)
		output += fmt.Sprintf("    Slices: %d\n", chunk.SliceCount)

		for _, slice := range chunk.SliceDetails {
			sliceType := "data"
			if !slice.IsDataSlice {
				sliceType = "parity"
			}
			output += fmt.Sprintf("      Slice %d (%s): %s (%d bytes)\n",
				slice.Index, sliceType, formatBytes(slice.Size), slice.Size)
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
