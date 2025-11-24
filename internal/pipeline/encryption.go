package pipeline

import (
	"bytes"
	"fmt"
	"io"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/pkg/encrypt"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	"github.com/i5heu/ouroboros-kv/internal/types"
	chunker "github.com/ipfs/boxo/chunker"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/reedsolomon"
)

func chunkerFunc(payload []byte) ([][]byte, error) {
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

func splitIntoRSSlices(rsDataSlices, rsParitySlices uint8, sealedChunks []*encrypt.EncryptResult, chunkHashes []hash.Hash) ([]types.SealedSlice, error) {
	enc, err := reedsolomon.New(int(rsDataSlices), int(rsParitySlices))
	if err != nil {
		return nil, fmt.Errorf("error creating reed solomon encoder: %w", err)
	}

	var records []types.SealedSlice
	for i, sealedChunk := range sealedChunks {
		originalSize := uint64(len(sealedChunk.Ciphertext))
		slices, err := enc.Split(sealedChunk.Ciphertext)
		if err != nil {
			return nil, fmt.Errorf("error splitting encrypted chunk: %w", err)
		}

		if len(slices) != int(rsDataSlices+rsParitySlices) {
			return nil, fmt.Errorf("unexpected number of slices: got %d, expected %d", len(slices), rsDataSlices+rsParitySlices)
		}

		for j, slice := range slices {
			record := types.SealedSlice{
				ChunkHash:       chunkHashes[i],
				SealedHash:      hash.HashBytes(sealedChunk.Ciphertext),
				RSDataSlices:    rsDataSlices,
				RSParitySlices:  rsParitySlices,
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

func EncodePayload(payload []byte, rsDataSlices, rsParitySlices uint8, c *crypt.Crypt) ([]types.SealedSlice, []hash.Hash, error) {
	if len(payload) == 0 {
		return nil, nil, nil
	}

	chunks, err := chunkerFunc(payload)
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
		sealedChunk, err := c.Encrypt(chunk)
		if err != nil {
			return nil, nil, err
		}
		sealedChunks = append(sealedChunks, sealedChunk)
	}

	slices, err := splitIntoRSSlices(rsDataSlices, rsParitySlices, sealedChunks, chunkHashes)
	if err != nil {
		return nil, nil, err
	}

	return slices, chunkHashes, nil
}
