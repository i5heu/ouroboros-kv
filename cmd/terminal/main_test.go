package main

import (
	"encoding/base64"
	"testing"

	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/stretchr/testify/require"
)

type stubKeyLister struct {
	keys []hash.Hash
	err  error
}

func (s stubKeyLister) ListKeys() ([]hash.Hash, error) {
	return s.keys, s.err
}

func TestDecodeExactHash(t *testing.T) {
	h := hash.HashBytes([]byte("example"))
	encoded := base64.StdEncoding.EncodeToString(h[:])

	decoded, ok := decodeExactHash(encoded)
	require.True(t, ok)
	require.Equal(t, h, decoded)

	// Removing more than padding should fail to decode into a full hash
	trimmed := encoded[:len(encoded)-10]
	decoded2, ok := decodeExactHash(trimmed)
	require.False(t, ok)
	require.Equal(t, hash.Hash{}, decoded2)
}

func TestFindHashByPrefix(t *testing.T) {
	first := hash.HashBytes([]byte("first"))
	second := hash.HashBytes([]byte("second"))
	keys := []hash.Hash{first, second}

	prefix := base64.StdEncoding.EncodeToString(first[:])[:10]
	resolved, err := findHashByPrefix(keys, prefix)
	require.NoError(t, err)
	require.Equal(t, first, resolved)

	_, err = findHashByPrefix(keys, "nomatch")
	require.Error(t, err)

	var nearA hash.Hash
	for i := range nearA {
		nearA[i] = 0xAA
	}
	nearB := nearA
	nearB[len(nearB)-1] = 0xBB
	ambiguousKeys := []hash.Hash{nearA, nearB}
	ambiguousPrefix := base64.StdEncoding.EncodeToString(nearA[:])[:80]
	_, err = findHashByPrefix(ambiguousKeys, ambiguousPrefix)
	require.Error(t, err)
}

func TestResolveHashInputWithPrefix(t *testing.T) {
	first := hash.HashBytes([]byte("first"))
	second := hash.HashBytes([]byte("second"))
	encodedFirst := base64.StdEncoding.EncodeToString(first[:])

	lister := stubKeyLister{keys: []hash.Hash{first, second}}

	resolved, err := resolveHashInput(lister, encodedFirst[:12])
	require.NoError(t, err)
	require.Equal(t, first, resolved)

	_, err = resolveHashInput(lister, "unknown")
	require.Error(t, err)
}
