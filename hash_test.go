package ouroboroskv

import (
	"fmt"
	"testing"

	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/i5heu/ouroboros-kv/storage"
	"github.com/stretchr/testify/require"
)

func TestHashConversion(t *testing.T) {
	// Create a test hash
	testKey := hash.HashString("parent-test")

	// Print it in different formats
	fmt.Printf("Hash as %%x: %x\n", testKey)
	fmt.Printf("Hash as %%s: %s\n", testKey)
	fmt.Printf("Hash as %%v: %v\n", testKey)

	// Try to create the prefix the same way as in storage
	prefix1 := fmt.Sprintf("%s%x:", storage.ParentPrefix, testKey)
	fmt.Printf("Storage prefix: %s\n", prefix1)

	// Try to create the prefix the same way as in query
	prefix2 := fmt.Sprintf("%s%x:", storage.ParentPrefix, testKey)
	fmt.Printf("Query prefix: %s\n", prefix2)

	// They should be the same
	require.Equal(t, prefix1, prefix2)
}
