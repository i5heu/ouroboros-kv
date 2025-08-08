package pipeline

import (
	"bytes"
	"fmt"

	"github.com/i5heu/ouroboros-crypt/encrypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/i5heu/ouroboros-kv/storage"
	"github.com/klauspost/reedsolomon"
)

func SplitReedSolomon(encryptedChunks []*encrypt.EncryptResult, chunkHashes []hash.Hash, dataShards, parityShards uint8) ([]storage.Shard, error) {
	enc, err := reedsolomon.New(int(dataShards), int(parityShards))
	if err != nil {
		return nil, fmt.Errorf("error creating reed solomon encoder: %w", err)
	}
	var shards []storage.Shard
	for i, encChunk := range encryptedChunks {
		originalSize := uint64(len(encChunk.Ciphertext))
		split, err := enc.Split(encChunk.Ciphertext)
		if err != nil {
			return nil, fmt.Errorf("error splitting encrypted chunk: %w", err)
		}
		if len(split) != int(dataShards+parityShards) {
			return nil, fmt.Errorf("unexpected number of shards: got %d, expected %d", len(split), dataShards+parityShards)
		}
		for j, s := range split {
			shards = append(shards, storage.Shard{
				ChunkHash:               chunkHashes[i],
				EncodedHash:             hash.HashBytes(encChunk.Ciphertext),
				ReedSolomonShards:       dataShards,
				ReedSolomonParityShards: parityShards,
				ReedSolomonIndex:        uint8(j),
				Size:                    uint64(len(s)),
				OriginalSize:            originalSize,
				EncapsulatedKey:         encChunk.EncapsulatedKey,
				Nonce:                   encChunk.Nonce,
				ChunkContent:            s,
			})
		}
	}
	return shards, nil
}

func ReconstructReedSolomon(shards []storage.Shard) (*encrypt.EncryptResult, error) {
	if len(shards) == 0 {
		return nil, fmt.Errorf("no shards provided for reconstruction")
	}
	first := shards[0]
	dataShards := int(first.ReedSolomonShards)
	parityShards := int(first.ReedSolomonParityShards)
	total := dataShards + parityShards
	if len(shards) > total {
		return nil, fmt.Errorf("too many shards: got %d, expected at most %d", len(shards), total)
	}
	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return nil, fmt.Errorf("failed to create Reed-Solomon decoder: %w", err)
	}
	rsShards := make([][]byte, total)
	var encapsulatedKey []byte
	var nonce []byte
	for _, s := range shards {
		if int(s.ReedSolomonIndex) >= total {
			return nil, fmt.Errorf("invalid Reed-Solomon index: %d (max %d)", s.ReedSolomonIndex, total-1)
		}
		rsShards[s.ReedSolomonIndex] = s.ChunkContent
		if encapsulatedKey == nil {
			encapsulatedKey = s.EncapsulatedKey
			nonce = s.Nonce
		}
	}
	if err := enc.Reconstruct(rsShards); err != nil {
		return nil, fmt.Errorf("failed to reconstruct Reed-Solomon shards: %w", err)
	}
	var reconstructed bytes.Buffer
	if err := enc.Join(&reconstructed, rsShards, int(shards[0].OriginalSize)); err != nil {
		return nil, fmt.Errorf("failed to join Reed-Solomon shards: %w", err)
	}
	return &encrypt.EncryptResult{
		Ciphertext:      reconstructed.Bytes(),
		EncapsulatedKey: encapsulatedKey,
		Nonce:           nonce,
	}, nil
}
