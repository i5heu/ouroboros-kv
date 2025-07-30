package ouroboroskv

import (
	"bytes"
	"fmt"
	"io"

	"github.com/i5heu/ouroboros-crypt/encrypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/klauspost/reedsolomon"
	"github.com/ulikunitz/xz/lzma"
)

func (k *KV) decodeDataPipeline(kvDataLinked KvDataLinked) (Data, error) {
	// Group chunks by their chunk hash to reconstruct original encrypted chunks
	chunkGroups := make(map[hash.Hash][]KvContentChunk)
	for _, chunk := range kvDataLinked.Chunks {
		chunkGroups[chunk.ChunkHash] = append(chunkGroups[chunk.ChunkHash], chunk)
	}

	// Reconstruct Reed-Solomon encoded chunks back to encrypted chunks
	var encryptedChunks []*encrypt.EncryptResult
	var chunkHashes []hash.Hash

	for chunkHash, chunks := range chunkGroups {
		encryptedChunk, err := k.reedSolomonReconstructor(chunks)
		if err != nil {
			return Data{}, fmt.Errorf("failed to reconstruct Reed-Solomon chunk: %w", err)
		}
		encryptedChunks = append(encryptedChunks, encryptedChunk)
		chunkHashes = append(chunkHashes, chunkHash)
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
		decompressedChunk, err := decompressWithLzma(compressedChunk)
		if err != nil {
			return Data{}, fmt.Errorf("failed to decompress chunk: %w", err)
		}
		chunks = append(chunks, decompressedChunk)
	}

	// Verify chunk hashes
	for i, chunk := range chunks {
		expectedHash := chunkHashes[i]
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

	return Data{
		Key:      kvDataLinked.Key,
		Content:  content.Bytes(),
		Parent:   kvDataLinked.Parent,
		Children: kvDataLinked.Children,
		// Note: Reed-Solomon configuration is not preserved in the reconstruction
		// as it's not needed for the original data structure
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

func decompressWithLzma(data []byte) ([]byte, error) {
	reader := bytes.NewReader(data)
	r, err := lzma.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create LZMA reader: %w", err)
	}

	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress LZMA data: %w", err)
	}

	return buf.Bytes(), nil
}
