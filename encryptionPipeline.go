package ouroboroskv

import (
	"bytes"
	"fmt"
	"io"

	"github.com/i5heu/ouroboros-crypt/pkg/encrypt"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	chunker "github.com/ipfs/boxo/chunker"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/reedsolomon"
)

func (k *KV) encodeDataPipeline(data Data) (kvData, error) {
	contentSlices, contentHashes, err := k.encodePayload(data.Content, data)
	if err != nil {
		return kvData{}, err
	}

	metaSlices, metaHashes, err := k.encodePayload(data.Meta, data)
	if err != nil {
		return kvData{}, err
	}

	return kvData{
		Key:             data.Key,
		Slices:          contentSlices,
		ChunkHashes:     contentHashes,
		MetaSlices:      metaSlices,
		MetaChunkHashes: metaHashes,
		Parent:          data.Parent,
		Children:        data.Children,
		Created:         data.Created,
		Aliases:         data.Aliases,
	}, nil
}

func (k *KV) chunker(payload []byte) ([][]byte, error) {

	reader := bytes.NewReader(payload)
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

func (k *KV) splitIntoRSSlices(data Data, sealedChunks []*encrypt.EncryptResult, chunkHashes []hash.Hash) ([]SealedSlice, error) {
	enc, err := reedsolomon.New(int(data.RSDataSlices), int(data.RSParitySlices))
	if err != nil {
		return nil, fmt.Errorf("error creating reed solomon encoder: %w", err)
	}

	var records []SealedSlice
	for i, sealedChunk := range sealedChunks {
		originalSize := uint64(len(sealedChunk.Ciphertext))
		slices, err := enc.Split(sealedChunk.Ciphertext)
		if err != nil {
			return nil, fmt.Errorf("error splitting encrypted chunk: %w", err)
		}

		if len(slices) != int(data.RSDataSlices+data.RSParitySlices) {
			return nil, fmt.Errorf("unexpected number of slices: got %d, expected %d", len(slices), data.RSDataSlices+data.RSParitySlices)
		}

		for j, slice := range slices {
			record := SealedSlice{
				ChunkHash:       chunkHashes[i],
				SealedHash:      hash.HashBytes(sealedChunk.Ciphertext),
				RSDataSlices:    data.RSDataSlices,
				RSParitySlices:  data.RSParitySlices,
				RSSliceIndex:    uint8(j),
				Size:            uint64(len(slice)),
				OriginalSize:    originalSize,
				EncapsulatedKey: sealedChunk.EncapsulatedKey,
				Nonce:           sealedChunk.Nonce,
				Payload:         slice,
			}
			records = append(records, record)
		}
	}

	return records, nil
}

func (k *KV) encodePayload(payload []byte, data Data) ([]SealedSlice, []hash.Hash, error) {
	if len(payload) == 0 {
		return nil, nil, nil
	}

	chunks, err := k.chunker(payload)
	if err != nil {
		return nil, nil, err
	}

	var chunkHashes []hash.Hash
	for _, chunk := range chunks {
		chunkHashes = append(chunkHashes, hash.HashBytes(chunk))
	}

	var compressedChunks [][]byte
	for _, chunk := range chunks {
		compressedChunk, err := compressWithZstd(chunk)
		if err != nil {
			return nil, nil, err
		}
		compressedChunks = append(compressedChunks, compressedChunk)
	}

	var sealedChunks []*encrypt.EncryptResult
	for _, chunk := range compressedChunks {
		sealedChunk, err := k.crypt.Encrypt(chunk)
		if err != nil {
			return nil, nil, err
		}
		sealedChunks = append(sealedChunks, sealedChunk)
	}

	slices, err := k.splitIntoRSSlices(data, sealedChunks, chunkHashes)
	if err != nil {
		return nil, nil, err
	}

	return slices, chunkHashes, nil
}
