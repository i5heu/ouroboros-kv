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
	content, rsData, rsParity, err := k.reconstructPayload(kvDataLinked.Slices, kvDataLinked.ChunkHashes)
	if err != nil {
		return Data{}, err
	}

	metadata, _, _, err := k.reconstructPayload(kvDataLinked.MetaSlices, kvDataLinked.MetaChunkHashes)
	if err != nil {
		return Data{}, err
	}

	return Data{
		Key:            kvDataLinked.Key,
		MetaData:       metadata,
		Content:        content,
		Parent:         kvDataLinked.Parent,
		Children:       kvDataLinked.Children,
		RSDataSlices:   rsData,
		RSParitySlices: rsParity,
		Created:        kvDataLinked.Created,
		Aliases:        kvDataLinked.Aliases,
	}, nil
}

func (k *KV) reedSolomonReconstructor(slices []SliceRecord) (*encrypt.EncryptResult, error) {
	if len(slices) == 0 {
		return nil, fmt.Errorf("no slices provided for reconstruction")
	}

	// All slices should have the same Reed-Solomon configuration
	firstSlice := slices[0]
	dataSlices := int(firstSlice.RSDataSlices)
	paritySlices := int(firstSlice.RSParitySlices)
	totalSlices := dataSlices + paritySlices

	if len(slices) > totalSlices {
		return nil, fmt.Errorf("too many slices: got %d, expected at most %d", len(slices), totalSlices)
	}

	// Create Reed-Solomon decoder
	enc, err := reedsolomon.New(dataSlices, paritySlices)
	if err != nil {
		return nil, fmt.Errorf("failed to create Reed-Solomon decoder: %w", err)
	}

	// Prepare slice matrix - initialize with nil payloads
	stripeSlices := make([][]byte, totalSlices)
	var encapsulatedKey []byte
	var nonce []byte

	// Fill slice matrix with available slices
	for _, slice := range slices {
		if int(slice.RSSliceIndex) >= totalSlices {
			return nil, fmt.Errorf("invalid Reed-Solomon index: %d (max %d)", slice.RSSliceIndex, totalSlices-1)
		}

		stripeSlices[slice.RSSliceIndex] = slice.Payload

		// All slices should have the same encryption metadata
		if encapsulatedKey == nil {
			encapsulatedKey = slice.EncapsulatedKey
			nonce = slice.Nonce
		}
	}

	// Try to reconstruct missing slices
	err = enc.Reconstruct(stripeSlices)
	if err != nil {
		return nil, fmt.Errorf("failed to reconstruct Reed-Solomon slices: %w", err)
	}

	// Calculate the total expected data size from the original size stored in chunks
	originalSize := int(slices[0].OriginalSize)

	// Use the Reed-Solomon Join method to properly reconstruct the data
	var reconstructed bytes.Buffer
	err = enc.Join(&reconstructed, stripeSlices, originalSize)
	if err != nil {
		return nil, fmt.Errorf("failed to join Reed-Solomon slices: %w", err)
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

func (k *KV) reconstructPayload(slices []SliceRecord, hashOrder []hash.Hash) ([]byte, uint8, uint8, error) {
	if len(slices) == 0 {
		return nil, 0, 0, nil
	}

	if len(hashOrder) == 0 {
		return nil, 0, 0, fmt.Errorf("hash order missing for payload reconstruction")
	}

	sliceGroups := make(map[hash.Hash][]SliceRecord)
	for _, slice := range slices {
		sliceGroups[slice.ChunkHash] = append(sliceGroups[slice.ChunkHash], slice)
	}

	var payload bytes.Buffer
	var rsData, rsParity uint8

	for idx, chunkHash := range hashOrder {
		stripeSlices, ok := sliceGroups[chunkHash]
		if !ok {
			return nil, 0, 0, fmt.Errorf("missing slice group for hash %x", chunkHash)
		}

		encryptedChunk, err := k.reedSolomonReconstructor(stripeSlices)
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

		if rsData == 0 && len(stripeSlices) > 0 {
			rsData = stripeSlices[0].RSDataSlices
			rsParity = stripeSlices[0].RSParitySlices
		}
	}

	return payload.Bytes(), rsData, rsParity, nil
}
