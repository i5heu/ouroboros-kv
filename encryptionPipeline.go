package ouroboroskv

import (
	"bytes"
	"fmt"
	"io"

	"github.com/i5heu/ouroboros-crypt/encrypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	chunker "github.com/ipfs/boxo/chunker"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/reedsolomon"
)

func (k *KV) encodeDataPipeline(data Data) (KvDataLinked, error) {
	// chunk the data content
	chunks, err := k.chunker(data)
	if err != nil {
		return KvDataLinked{}, err
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
		compressedChunk, err := compressWithZstd(chunk)
		if err != nil {
			return KvDataLinked{}, err
		}
		compressedChunks = append(compressedChunks, compressedChunk)
	}

	// encrypt the chunk
	var encryptedChunks []*encrypt.EncryptResult
	for _, chunk := range compressedChunks {
		encryptedChunk, err := k.crypt.Encrypt(chunk)
		if err != nil {
			return KvDataLinked{}, err
		}
		encryptedChunks = append(encryptedChunks, encryptedChunk)
	}

	// split the chunk into reeds-solomon shards

	kvContentChunks, err := k.reedSolomonSplitter(data, encryptedChunks, chunkHashes)
	if err != nil {
		return KvDataLinked{}, err
	}

	return KvDataLinked{
		Key:      data.Key,
		Chunks:   kvContentChunks,
		Parent:   data.Parent,
		Children: data.Children,
	}, nil
}

func (k *KV) chunker(data Data) ([][]byte, error) {

	reader := bytes.NewReader(data.Content)
	bz := chunker.NewBuzhash(reader)

	var chunks [][]byte

	for chunkIndex := 0; ; chunkIndex++ {
		buzChunk, err := bz.NextBytes()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, buzChunk)

	}
	return chunks, nil
}

func compressWithZstd(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf)
	if err != nil {
		return nil, err
	}
	_, err = enc.Write(data)
	if err != nil {
		return nil, err
	}
	err = enc.Close()
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (k *KV) reedSolomonSplitter(data Data, encryptedChunks []*encrypt.EncryptResult, chunkHashes []hash.Hash) ([]KvContentChunk, error) {
	enc, err := reedsolomon.New(int(data.ReedSolomonShards), int(data.ReedSolomonParityShards))
	if err != nil {
		return nil, fmt.Errorf("error creating reed solomon encoder: %w", err)
	}

	var kvContentChunks []KvContentChunk
	for i, encryptedChunk := range encryptedChunks {
		originalSize := uint64(len(encryptedChunk.Ciphertext))
		shards, err := enc.Split(encryptedChunk.Ciphertext)
		if err != nil {
			return nil, fmt.Errorf("error splitting encrypted chunk: %w", err)
		}

		if len(shards) != int(data.ReedSolomonShards+data.ReedSolomonParityShards) {
			return nil, fmt.Errorf("unexpected number of shards: got %d, expected %d", len(shards), data.ReedSolomonShards+data.ReedSolomonParityShards)
		}

		for j, shard := range shards {
			kvContentChunk := KvContentChunk{
				ChunkHash:               chunkHashes[i],
				EncodedHash:             hash.HashBytes(encryptedChunk.Ciphertext), // TODO must also include metadata
				ReedSolomonShards:       data.ReedSolomonShards,
				ReedSolomonParityShards: data.ReedSolomonParityShards,
				ReedSolomonIndex:        uint8(j),
				Size:                    uint64(len(shard)),
				OriginalSize:            originalSize,
				EncapsulatedKey:         encryptedChunk.EncapsulatedKey,
				Nonce:                   encryptedChunk.Nonce,
				ChunkContent:            shard,
			}
			kvContentChunks = append(kvContentChunks, kvContentChunk)
		}
	}

	return kvContentChunks, nil
}
