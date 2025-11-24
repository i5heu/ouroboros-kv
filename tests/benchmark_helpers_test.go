package ouroboroskv__test

import "github.com/i5heu/ouroboros-crypt/pkg/hash"

func hashSliceContains(hashes []hash.Hash, target hash.Hash) bool {
	for _, h := range hashes {
		if h == target {
			return true
		}
	}
	return false
}

func findHashIndex(hashes []hash.Hash, target hash.Hash) int {
	for idx, h := range hashes {
		if h == target {
			return idx
		}
	}
	return -1
}

func copyBytes(src []byte) []byte {
	return append([]byte(nil), src...)
}
