package types

import (
	"fmt"

	"github.com/i5heu/ouroboros-crypt/pkg/hash"
)

// VersionRef captures a versioned reference with its hash and VersionID.
type VersionRef struct {
	Hash      hash.Hash // Key of the referenced version
	VersionID int64     // Version identifier in milliseconds since epoch (0 for originals)
}

// Data is the user facing "value" or Data of ouroboros-kv, which contains the content and metadata.
type Data struct {
	Key hash.Hash // Key is derived from all fields except Children; must be zero when writing new data because it is generated from the content
	// Part of the key hash:
	Content         []byte    // The actual content of the data (stored encrypted)
	Parent          hash.Hash // Key of the parent value
	PrevVersionHash hash.Hash // Hash of the immediately previous version (zero hash for originals)
	Created         int64     // Unix timestamp when the data was created
	VersionID       int64     // Unix time in milliseconds representing this version; 0 for originals
	PrevVersionID   int64     // VersionID of the previous version; 0 for originals
	RSDataSlices    uint8     // Number of Reed-Solomon data slices per stripe (RSDataSlices + RSParitySlices = total slices)
	RSParitySlices  uint8     // Number of Reed-Solomon parity slices per stripe (RSDataSlices + RSParitySlices = total slices)
	ContentType     string    // ContentType type of the content (not encrypted and is part of the key hash)
	// Not part of the key hash:
	Children []hash.Hash  // Keys of the child values (stored dynamically and not part of the key hash)
	Meta     []byte       // Dynamic additional metadata (stored encrypted but not part of the key hash) for embeddings, labels, etc.
	Aliases  []hash.Hash  // Aliases for the data (not part of the key hash)
	Branches []VersionRef // Alternative branch heads that share the same original ancestor (only set on originals)
	// UnencryptedSystemData []UnencryptedSystemData // Unencrypted system data associated (not part of the key hash and not encrypted but very fast to read and write) for stats, distribution info, etc.
}

// KvData represents the `Data` structure with full SliceRecords for decryption and reconstruction.
type KvData struct {
	Key             hash.Hash
	Slices          []SealedSlice // Content RSSlices
	ChunkHashes     []hash.Hash   // Order of content chunk hashes
	MetaSlices      []SealedSlice // Metadata RSSlices
	MetaChunkHashes []hash.Hash   // Order of metadata chunk hashes
	Parent          hash.Hash     // Key of the parent chunk
	PrevVersionHash hash.Hash     // Hash of the previous version (zero for originals)
	Children        []hash.Hash   // Keys of the child chunks
	Created         int64         // Unix timestamp when the data was created
	VersionID       int64         // Unix time in milliseconds for this version; 0 for originals
	PrevVersionID   int64         // VersionID of the previous version; 0 for originals
	Aliases         []hash.Hash   // Aliases for the data
	ContentType     string        // Content type for the data - IETF RFC 9110 §8.3 https://datatracker.ietf.org/doc/html/rfc9110#section-8.3 (unencrypted)
	Branches        []VersionRef  // Alternative branch heads that share the same original ancestor (only set on originals)
}

// SealedSlice represents a single Reed-Solomon slice (data or parity) persisted in the key-value store.
type SealedSlice struct {
	ChunkHash       hash.Hash // Hash of the clear chunk produced by Buzhash
	SealedHash      hash.Hash // Hash of the sealed chunk (compressed + encrypted) prior to Reed-Solomon encoding
	RSDataSlices    uint8     // Number of data slices in the originating stripe
	RSParitySlices  uint8     // Number of parity slices in the originating stripe
	RSSliceIndex    uint8     // Index of the slice within the stripe (data slices precede parity slices)
	Size            uint64    // Size of this slice payload in bytes
	OriginalSize    uint64    // Size of the sealed chunk before Reed-Solomon encoding
	EncapsulatedKey []byte    // ML-KEM encapsulated secret for the sealed chunk
	Nonce           []byte    // AES-GCM nonce for encryption
	Payload         []byte    // Slice payload after Reed-Solomon encoding
}

// KvRef represents the `Data` structure stored in the key-value store, with chunk hashes instead of full content.
type KvRef struct {
	Key             hash.Hash
	ChunkHashes     []hash.Hash  // Hash of deduplicated chunks
	MetaChunkHashes []hash.Hash  // Hash of metadata chunks
	Parent          hash.Hash    // Key of the parent chunk
	PrevVersionHash hash.Hash    // Previous version hash (zero for originals)
	Created         int64        // Unix timestamp when the data was created
	VersionID       int64        // Unix time in milliseconds for this version
	PrevVersionID   int64        // VersionID of the previous version
	Aliases         []hash.Hash  // Aliases for the data
	ContentType     string       // ContentType type of the content (stored with ref for quick access)
	Branches        []VersionRef // Alternative branch heads that share the same original ancestor (only set on originals)
}

// Unencrypted system data associated (not part of the key hash and not encrypted but very fast to read and write) for stats, distribution info, etc.
// type UnencryptedSystemData struct {
// 	Type string // Type of system data (e.g., "config", "stats")
// 	Data []byte // Raw system data payload
// }

// DataInfo represents detailed information about stored data
type DataInfo struct {
	Key               hash.Hash    // The data key
	KeyBase64         string       // Base64 encoded key for display
	VersionID         int64        // Version identifier (ms since epoch, 0 for originals)
	PrevVersionHash   hash.Hash    // Previous version hash if present
	PrevVersionID     int64        // Previous version identifier
	Branches          []VersionRef // Other branch heads that share the same original ancestor
	ChunkHashes       []hash.Hash  // Hashes of all chunks
	MetaChunkHashes   []hash.Hash  // Hashes of all metadata chunks
	ClearTextSize     uint64       // Original uncompressed data size
	StorageSize       uint64       // Total size on storage (sum of all slices)
	MetaClearTextSize uint64       // Metadata clear text size
	MetaStorageSize   uint64       // Total metadata storage size
	NumChunks         int          // Number of logical chunks
	NumSlices         int          // Total number of Reed-Solomon slices
	MetaNumChunks     int          // Number of metadata chunks
	MetaNumSlices     int          // Total number of metadata slices
	RSDataSlices      uint8        // Data slices per chunk
	RSParitySlices    uint8        // Parity slices per chunk
	ChunkDetails      []ChunkInfo  // Detailed information per chunk
	MetaData          []byte       // Decoded metadata payload
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

// FormatDataInfo returns a human-readable string representation of DataInfo
func (info DataInfo) FormatDataInfo() string {
	output := fmt.Sprintf("Data Key: %s\n", info.KeyBase64)
	output += fmt.Sprintf("Version: %d\n", info.VersionID)
	if info.PrevVersionID != 0 || !isZeroHash(info.PrevVersionHash) {
		output += fmt.Sprintf("Previous: %x @ %d\n", info.PrevVersionHash, info.PrevVersionID)
	}
	if len(info.Branches) > 0 {
		output += "Branches:\n"
		for _, br := range info.Branches {
			output += fmt.Sprintf("  %x @ %d\n", br.Hash, br.VersionID)
		}
	}
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

// formatBytes returns a human-readable byte size (duplicate of the one in list.go for CLI use)
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

func isZeroHash(h hash.Hash) bool {
	var zero hash.Hash
	return h == zero
}
