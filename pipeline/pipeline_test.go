package pipeline

import (
	"bytes"
	"testing"
)

func TestChunk(t *testing.T) {
	data := bytes.Repeat([]byte("hello world"), 100)

	chunks, err := Chunk(data)
	if err != nil {
		t.Fatalf("Chunk failed: %v", err)
	}
	if len(chunks) == 0 {
		t.Fatalf("expected at least one chunk")
	}

	var combined []byte
	for _, c := range chunks {
		combined = append(combined, c...)
	}
	if !bytes.Equal(combined, data) {
		t.Fatalf("chunked data does not reconstruct original")
	}
}

func TestZstdCompressDecompress(t *testing.T) {
	data := bytes.Repeat([]byte("compression test"), 50)

	compressed, err := CompressWithZstd(data)
	if err != nil {
		t.Fatalf("CompressWithZstd failed: %v", err)
	}

	decompressed, err := DecompressWithZstd(compressed)
	if err != nil {
		t.Fatalf("DecompressWithZstd failed: %v", err)
	}
	if !bytes.Equal(decompressed, data) {
		t.Fatalf("expected decompressed data to match original")
	}
}

func TestDecompressWithInvalidData(t *testing.T) {
	_, err := DecompressWithZstd([]byte("not zstd"))
	if err == nil {
		t.Fatalf("expected error when decompressing invalid data")
	}
}
