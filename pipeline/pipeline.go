package pipeline

import (
	"bytes"
	"io"

	chunker "github.com/ipfs/boxo/chunker"
	"github.com/klauspost/compress/zstd"
)

// Chunk breaks the provided data into Buzhash chunks.
func Chunk(data []byte) ([][]byte, error) {
	reader := bytes.NewReader(data)
	bz := chunker.NewBuzhash(reader)

	var chunks [][]byte
	for {
		chunk, err := bz.NextBytes()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

// CompressWithZstd compresses data using the Zstandard algorithm.
func CompressWithZstd(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf)
	if err != nil {
		return nil, err
	}
	if _, err = enc.Write(data); err != nil {
		return nil, err
	}
	if err = enc.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecompressWithZstd decompresses Zstandard-compressed data.
func DecompressWithZstd(data []byte) ([]byte, error) {
	reader := bytes.NewReader(data)
	dec, err := zstd.NewReader(reader)
	if err != nil {
		return nil, err
	}
	defer dec.Close()

	var buf bytes.Buffer
	if _, err = io.Copy(&buf, dec); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
