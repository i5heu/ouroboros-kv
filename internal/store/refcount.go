package store

import (
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
)

const REFCOUNT_PREFIX = "refcount:"

func refCountKeyBytes(key hash.Hash) []byte {
	return []byte(fmt.Sprintf("%s%x", REFCOUNT_PREFIX, key))
}

func encodeRefCount(count uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, count)
	return buf
}

func decodeRefCount(data []byte) uint64 {
	if len(data) >= 8 {
		return binary.BigEndian.Uint64(data[:8])
	}

	var buf [8]byte
	copy(buf[8-len(data):], data)
	return binary.BigEndian.Uint64(buf[:])
}

func (k *KV) getRefCountTxn(txn *badger.Txn, key hash.Hash) (uint64, error) {
	item, err := txn.Get(refCountKeyBytes(key))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}

	var value []byte
	if err := item.Value(func(val []byte) error {
		value = append([]byte(nil), val...)
		return nil
	}); err != nil {
		return 0, err
	}

	if len(value) == 0 {
		return 0, nil
	}

	return decodeRefCount(value), nil
}

func (k *KV) getRefCount(key hash.Hash) (uint64, error) {
	var count uint64
	err := k.badgerDB.View(func(txn *badger.Txn) error {
		var err error
		count, err = k.getRefCountTxn(txn, key)
		return err
	})

	return count, err
}

func (k *KV) getRefCounts(keys []hash.Hash) (map[hash.Hash]uint64, error) {
	unique := make(map[hash.Hash]struct{})
	for _, key := range keys {
		unique[key] = struct{}{}
	}

	counts := make(map[hash.Hash]uint64, len(unique))
	err := k.badgerDB.View(func(txn *badger.Txn) error {
		for key := range unique {
			count, err := k.getRefCountTxn(txn, key)
			if err != nil {
				return err
			}
			counts[key] = count
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	for key := range unique {
		if _, ok := counts[key]; !ok {
			counts[key] = 0
		}
	}

	return counts, nil
}
