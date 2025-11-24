package ouroboroskv__test

import (
	"sync"

	"github.com/i5heu/ouroboros-crypt/pkg/hash"
)

type liveKeyTracker struct {
	mu   sync.RWMutex
	keys map[hash.Hash]struct{}
}

func newLiveKeyTracker() *liveKeyTracker {
	return &liveKeyTracker{keys: make(map[hash.Hash]struct{})}
}

func (t *liveKeyTracker) add(key hash.Hash) {
	t.mu.Lock()
	t.keys[key] = struct{}{}
	t.mu.Unlock()
}

func (t *liveKeyTracker) addMany(keys []hash.Hash) {
	t.mu.Lock()
	for _, key := range keys {
		t.keys[key] = struct{}{}
	}
	t.mu.Unlock()
}

func (t *liveKeyTracker) remove(key hash.Hash) {
	t.mu.Lock()
	delete(t.keys, key)
	t.mu.Unlock()
}

func (t *liveKeyTracker) contains(key hash.Hash) bool {
	t.mu.RLock()
	_, ok := t.keys[key]
	t.mu.RUnlock()
	return ok
}

func (t *liveKeyTracker) intersection(keys []hash.Hash) []hash.Hash {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([]hash.Hash, 0, len(keys))
	for _, key := range keys {
		if _, ok := t.keys[key]; ok {
			result = append(result, key)
		}
	}
	return result
}

func (t *liveKeyTracker) snapshot() []hash.Hash {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([]hash.Hash, 0, len(t.keys))
	for key := range t.keys {
		result = append(result, key)
	}
	return result
}

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
