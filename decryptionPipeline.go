package ouroboroskv

import (
	"bytes"
	"fmt"
	"io"

	"github.com/i5heu/ouroboros-crypt/encrypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/reedsolomon"
)

func (k *KV) decodeDataPipeline(kvDataLinked kvDataLinked) (Data, error) {
	content, rsShards, rsParity, err := k.reconstructPayload(kvDataLinked.Shards, kvDataLinked.ChunkHashes)
	if err != nil {
		return Data{}, err
	}

	metadata, _, _, err := k.reconstructPayload(kvDataLinked.MetaShards, kvDataLinked.MetaChunkHashes)
	if err != nil {
		return Data{}, err
	}

	return Data{
		Key:                     kvDataLinked.Key,
		MetaData:                metadata,
		Content:                 content,
		Parent:                  kvDataLinked.Parent,
		Children:                kvDataLinked.Children,
		ReedSolomonShards:       rsShards,
		ReedSolomonParityShards: rsParity,
		Created:                 kvDataLinked.Created,
		Aliases:                 kvDataLinked.Aliases,
	}, nil
}

func (k *KV) reedSolomonReconstructor(chunks []kvDataShard) (*encrypt.EncryptResult, error) {
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

func (k *KV) reconstructPayload(shards []kvDataShard, hashOrder []hash.Hash) ([]byte, uint8, uint8, error) {
	if len(shards) == 0 {
		return nil, 0, 0, nil
	}

	if len(hashOrder) == 0 {
		return nil, 0, 0, fmt.Errorf("hash order missing for payload reconstruction")
	}

	chunkGroups := make(map[hash.Hash][]kvDataShard)
	for _, shard := range shards {
		chunkGroups[shard.ChunkHash] = append(chunkGroups[shard.ChunkHash], shard)
	}

	var payload bytes.Buffer
	var rsShards, rsParity uint8

	for idx, chunkHash := range hashOrder {
		chunks, ok := chunkGroups[chunkHash]
		if !ok {
			return nil, 0, 0, fmt.Errorf("missing shard group for hash %x", chunkHash)
		}

		encryptedChunk, err := k.reedSolomonReconstructor(chunks)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("failed to reconstruct Reed-Solomon chunk: %w", err)
		}

		decryptedChunk, err := k.crypt.Decrypt(encryptedChunk)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("failed to decrypt chunk: %w", err)
		}

		decompressedChunk, err := decompressWithZstd(decryptedChunk)
		if err != nil {
			return nil, 0, 0, fmt.Errorf("failed to decompress chunk: %w", err)
		}

		actualHash := hash.HashBytes(decompressedChunk)
		if actualHash != chunkHash {
			return nil, 0, 0, fmt.Errorf("chunk %d hash mismatch: expected %x, got %x", idx, chunkHash, actualHash)
		}

		payload.Write(decompressedChunk)

		if rsShards == 0 && len(chunks) > 0 {
			rsShards = chunks[0].ReedSolomonShards
			rsParity = chunks[0].ReedSolomonParityShards
		}
	}

	return payload.Bytes(), rsShards, rsParity, nil
}
