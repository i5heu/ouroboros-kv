package ouroboroskv

import (
	"bytes"
	"fmt"

	"github.com/i5heu/ouroboros-crypt/encrypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/i5heu/ouroboros-kv/pipeline"
	"github.com/i5heu/ouroboros-kv/storage"
)

func (k *KV) decodeDataPipeline(kvDataLinked kvDataLinked) (Data, error) {
	// Group chunks by their chunk hash to reconstruct original encrypted chunks
	chunkGroups := make(map[hash.Hash][]storage.Shard)
	for _, chunk := range kvDataLinked.Shards {
		chunkGroups[chunk.ChunkHash] = append(chunkGroups[chunk.ChunkHash], chunk)
	}

	// Get ordered chunk hashes from metadata to preserve chunk order
	// We need to determine the correct order. Since we don't have direct access to the metadata order here,
	// we'll extract it from the chunks themselves by looking at the unique chunk hashes in order they appear
	var orderedChunkHashes []hash.Hash
	seenHashes := make(map[hash.Hash]bool)
	for _, chunk := range kvDataLinked.Shards {
		if !seenHashes[chunk.ChunkHash] {
			orderedChunkHashes = append(orderedChunkHashes, chunk.ChunkHash)
			seenHashes[chunk.ChunkHash] = true
		}
	}

	// Reconstruct Reed-Solomon encoded chunks back to encrypted chunks in the correct order
	var encryptedChunks []*encrypt.EncryptResult

	for _, chunkHash := range orderedChunkHashes {
		chunks := chunkGroups[chunkHash]
		encryptedChunk, err := pipeline.ReconstructReedSolomon(chunks)
		if err != nil {
			return Data{}, fmt.Errorf("failed to reconstruct Reed-Solomon chunk: %w", err)
		}
		encryptedChunks = append(encryptedChunks, encryptedChunk)
	}

	// Decrypt the chunks
	var compressedChunks [][]byte
	for _, encryptedChunk := range encryptedChunks {
		decryptedChunk, err := k.crypt.Decrypt(encryptedChunk)
		if err != nil {
			return Data{}, fmt.Errorf("failed to decrypt chunk: %w", err)
		}
		compressedChunks = append(compressedChunks, decryptedChunk)
	}

	// Decompress the chunks
	var chunks [][]byte
	for _, compressedChunk := range compressedChunks {
		decompressedChunk, err := pipeline.DecompressWithZstd(compressedChunk)
		if err != nil {
			return Data{}, fmt.Errorf("failed to decompress chunk: %w", err)
		}
		chunks = append(chunks, decompressedChunk)
	}

	// Verify chunk hashes in order
	for i, chunk := range chunks {
		expectedHash := orderedChunkHashes[i]
		actualHash := hash.HashBytes(chunk)
		if actualHash != expectedHash {
			return Data{}, fmt.Errorf("chunk %d hash mismatch: expected %x, got %x", i, expectedHash, actualHash)
		}
	}

	// Reassemble chunks into original content
	var content bytes.Buffer
	for _, chunk := range chunks {
		content.Write(chunk)
	}

	// Get Reed-Solomon configuration from the first chunk (all chunks have the same config)
	var reedSolomonShards, reedSolomonParityShards uint8
	if len(kvDataLinked.Shards) > 0 {
		reedSolomonShards = kvDataLinked.Shards[0].ReedSolomonShards
		reedSolomonParityShards = kvDataLinked.Shards[0].ReedSolomonParityShards
	}

	return Data{
		Key:                     kvDataLinked.Key,
		Content:                 content.Bytes(),
		Parent:                  kvDataLinked.Parent,
		Children:                kvDataLinked.Children,
		ReedSolomonShards:       reedSolomonShards,
		ReedSolomonParityShards: reedSolomonParityShards,
	}, nil
}
