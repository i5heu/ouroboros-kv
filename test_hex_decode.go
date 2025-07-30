package ouroboroskv

import (
	"encoding/hex"
	"fmt"
	"testing"
)

func TestDoubleHexDecoding(t *testing.T) {
	// The stored double-encoded hex (first 128 chars)
	doubleEncoded := "6266636639333863663661613864383565326264613462643664383061346438613535633638363461386631383965633361393066313865656532396433653138"

	// Decode to get the "inner" hex string
	decoded, err := hex.DecodeString(doubleEncoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	fmt.Printf("Decoded to: %s\n", string(decoded))
	fmt.Printf("Length: %d\n", len(decoded))
}
