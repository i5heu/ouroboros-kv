package ouroboroskv

import (
	"github.com/i5heu/ouroboros-crypt/encrypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/i5heu/ouroboros-kv/pipeline"
)

func (k *KV) encodeDataPipeline(data Data) (kvDataLinked, error) {
	// chunk the data content
	chunks, err := pipeline.Chunk(data.Content)
	if err != nil {
		return kvDataLinked{}, err
	}

	// hash the chunks
	var chunkHashes []hash.Hash
	for _, chunk := range chunks {
		chunkHash := hash.HashBytes(chunk) // Hash the chunk content
		chunkHashes = append(chunkHashes, chunkHash)
	}

	// compress the chunk
	var compressedChunks [][]byte
	for _, chunk := range chunks {
		compressedChunk, err := pipeline.CompressWithZstd(chunk)
		if err != nil {
			return kvDataLinked{}, err
		}
		compressedChunks = append(compressedChunks, compressedChunk)
	}

	// encrypt the chunk
	var encryptedChunks []*encrypt.EncryptResult
	for _, chunk := range compressedChunks {
		encryptedChunk, err := k.crypt.Encrypt(chunk)
		if err != nil {
			return kvDataLinked{}, err
		}
		encryptedChunks = append(encryptedChunks, encryptedChunk)
	}

	// split the chunk into reeds-solomon shards

	shards, err := pipeline.SplitReedSolomon(encryptedChunks, chunkHashes, data.ReedSolomonShards, data.ReedSolomonParityShards)
	if err != nil {
		return kvDataLinked{}, err
	}

	return kvDataLinked{
		Key:      data.Key,
		Shards:   shards,
		Parent:   data.Parent,
		Children: data.Children,
	}, nil
}
