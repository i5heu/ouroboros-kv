package ouroboroskv

import (
	"bytes"
	"fmt"
	"io"

	"github.com/i5heu/ouroboros-crypt/encrypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/klauspost/reedsolomon"
	"github.com/klauspost/compress/zstd"
)

func (k *KV) decodeDataPipeline(kvDataLinked KvDataLinked) (Data, error) {
	// Group chunks by their chunk hash to reconstruct original encrypted chunks
	chunkGroups := make(map[hash.Hash][]KvContentChunk)
	for _, chunk := range kvDataLinked.Chunks {
		chunkGroups[chunk.ChunkHash] = append(chunkGroups[chunk.ChunkHash], chunk)
	}

	// Get ordered chunk hashes from metadata to preserve chunk order
	// We need to determine the correct order. Since we don't have direct access to the metadata order here,
	// we'll extract it from the chunks themselves by looking at the unique chunk hashes in order they appear
	var orderedChunkHashes []hash.Hash
	seenHashes := make(map[hash.Hash]bool)
	for _, chunk := range kvDataLinked.Chunks {
		if !seenHashes[chunk.ChunkHash] {
			orderedChunkHashes = append(orderedChunkHashes, chunk.ChunkHash)
			seenHashes[chunk.ChunkHash] = true
		}
	}

	// Reconstruct Reed-Solomon encoded chunks back to encrypted chunks in the correct order
	var encryptedChunks []*encrypt.EncryptResult

	for _, chunkHash := range orderedChunkHashes {
		chunks := chunkGroups[chunkHash]
		encryptedChunk, err := k.reedSolomonReconstructor(chunks)
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
			decompressedChunk, err := decompressWithZstd(compressedChunk)
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
	if len(kvDataLinked.Chunks) > 0 {
		reedSolomonShards = kvDataLinked.Chunks[0].ReedSolomonShards
		reedSolomonParityShards = kvDataLinked.Chunks[0].ReedSolomonParityShards
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

func (k *KV) reedSolomonReconstructor(chunks []KvContentChunk) (*encrypt.EncryptResult, error) {
	if len(chunks) == 0 {
		return nil, fmt.Errorf("no chunks provided for reconstruction")
	}

	// All chunks should have the same Reed-Solomon configuration
	firstChunk := chunks[0]
	dataShards := int(firstChunk.ReedSolomonShards)
	parityShards := int(firstChunk.ReedSolomonParityShards)
	totalShards := dataShards + parityShards

	if len(chunks) > totalShards {
		return nil, fmt.Errorf("too many chunks: got %d, expected at most %d", len(chunks), totalShards)
	}

	// Create Reed-Solomon decoder
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, fmt.Errorf("failed to create Reed-Solomon decoder: %w", err)
	}

	// Prepare shards array - initialize with nil slices
	shards := make([][]byte, totalShards)
	var encapsulatedKey []byte
	var nonce []byte

	// Fill shards with available chunks
	for _, chunk := range chunks {
		if int(chunk.ReedSolomonIndex) >= totalShards {
			return nil, fmt.Errorf("invalid Reed-Solomon index: %d (max %d)", chunk.ReedSolomonIndex, totalShards-1)
		}

		shards[chunk.ReedSolomonIndex] = chunk.ChunkContent

		// All chunks should have the same encryption metadata
		if encapsulatedKey == nil {
			encapsulatedKey = chunk.EncapsulatedKey
			nonce = chunk.Nonce
		}
	}

	// Try to reconstruct missing shards
	err = enc.Reconstruct(shards)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct Reed-Solomon shards: %w", err)
	}

	// Calculate the total expected data size from the original size stored in chunks
	originalSize := int(chunks[0].OriginalSize)

	// Use the Reed-Solomon Join method to properly reconstruct the data
	var reconstructed bytes.Buffer
	err = enc.Join(&reconstructed, shards, originalSize)
	if err != nil {
		return nil, fmt.Errorf("failed to join Reed-Solomon shards: %w", err)
	}

	return &encrypt.EncryptResult{
		Ciphertext:      reconstructed.Bytes(),
		EncapsulatedKey: encapsulatedKey,
		Nonce:           nonce,
	}, nil
}

func decompressWithZstd(data []byte) ([]byte, error) {
	reader := bytes.NewReader(data)
	dec, err := zstd.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create Zstd reader: %w", err)
	}
	defer dec.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, dec)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress Zstd data: %w", err)
	}

	return buf.Bytes(), nil
}
