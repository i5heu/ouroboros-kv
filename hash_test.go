package ouroboroskv

import (
	"fmt"
	"testing"

	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/stretchr/testify/require"
)

func TestHashConversion(t *testing.T) {
	// Create a test hash
	testKey := hash.HashString("parent-test")

	// Print it in different formats
	fmt.Printf("Hash as %%x: %x\n", testKey)
	fmt.Printf("Hash as %%s: %s\n", testKey)
	fmt.Printf("Hash as %%v: %v\n", testKey)
	fmt.Printf("len(testKey)=%d len(string(testKey[:]))=%d\n", len(testKey), len(string(testKey[:])))

	// Try to create the prefix the same way as in storage
	prefix1 := fmt.Sprintf("%s%x:", PARENT_PREFIX, testKey[:])
	fmt.Printf("Storage prefix: %s\n", prefix1)

	// Try to create the prefix the same way as in query
	prefix2 := fmt.Sprintf("%s%x:", PARENT_PREFIX, testKey[:])
	fmt.Printf("Query prefix: %s\n", prefix2)
	fmt.Printf("testKey.String()=%s len=%d\n", testKey.String(), len(testKey.String()))
	parsed, err := hash.HashHexadecimal(testKey.String())
	fmt.Printf("parsed==testKey? %v err=%v\n", parsed == testKey, err)

	// They should be the same
	require.Equal(t, prefix1, prefix2)
}
