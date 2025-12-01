package store

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

// InspectRelationshipCounts returns the number of parent->child and child->parent
// entries currently stored in the KV backend. Useful for manual verification
// and heavy delete tests.
func (k *KV) InspectRelationshipCounts() (parentEntries int, childEntries int, err error) {
	err = k.badgerDB.View(func(txn *badger.Txn) error {
		parentEntries = countKeysWithPrefix(txn, []byte(PARENT_PREFIX))
		childEntries = countKeysWithPrefix(txn, []byte(CHILD_PREFIX))
		return nil
	})
	return parentEntries, childEntries, err
}

// InspectRelationshipKeys returns up to limit entries for the requested
// relationship prefix. If limit is zero, all matching keys are returned.
func (k *KV) InspectRelationshipKeys(prefix string, limit int) ([]string, error) {
	var rawPrefix []byte
	switch prefix {
	case "parent":
		rawPrefix = []byte(PARENT_PREFIX)
	case "child":
		rawPrefix = []byte(CHILD_PREFIX)
	default:
		return nil, fmt.Errorf("unknown relationship prefix %q", prefix)
	}

	keys := make([]string, 0)
	err := k.badgerDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(rawPrefix); it.ValidForPrefix(rawPrefix); it.Next() {
			if limit > 0 && len(keys) >= limit {
				break
			}
			keys = append(keys, string(it.Item().Key()))
		}
		return nil
	})

	return keys, err
}

func countKeysWithPrefix(txn *badger.Txn, prefix []byte) int {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := txn.NewIterator(opts)
	defer it.Close()

	count := 0
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		count++
	}
	return count
}
