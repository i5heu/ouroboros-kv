package store

import (
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
)

const ALIAS_PREFIX = "alias:"

type aliasMapping struct {
	Alias     hash.Hash
	Canonical hash.Hash
}

func aliasKeyBytes(key hash.Hash) []byte {
	return []byte(fmt.Sprintf("%s%x", ALIAS_PREFIX, key))
}

func generateAliasKey(canonical hash.Hash, aliasIndex uint64) hash.Hash {
	if aliasIndex == 0 {
		return canonical
	}

	suffix := make([]byte, len(canonical)+8)
	copy(suffix, canonical[:])
	binary.BigEndian.PutUint64(suffix[len(canonical):], aliasIndex)

	return hash.HashBytes(suffix)
}

func (k *KV) storeAliasWithBatch(wb *badger.WriteBatch, alias, canonical hash.Hash) error {
	value := make([]byte, len(canonical))
	copy(value, canonical[:])
	return wb.Set(aliasKeyBytes(alias), value)
}

func (k *KV) storeAliasTxn(txn *badger.Txn, alias, canonical hash.Hash) error {
	value := make([]byte, len(canonical))
	copy(value, canonical[:])
	return txn.Set(aliasKeyBytes(alias), value)
}

func (k *KV) deleteAliasTxn(txn *badger.Txn, alias hash.Hash) error {
	return txn.Delete(aliasKeyBytes(alias))
}

func (k *KV) resolveAliasTxn(txn *badger.Txn, key hash.Hash) (hash.Hash, bool, error) {
	item, err := txn.Get(aliasKeyBytes(key))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return hash.Hash{}, false, nil
		}
		return hash.Hash{}, false, err
	}

	var value []byte
	if err := item.Value(func(val []byte) error {
		value = append([]byte(nil), val...)
		return nil
	}); err != nil {
		return hash.Hash{}, false, err
	}

	var canonical hash.Hash
	copy(canonical[:], value)
	return canonical, true, nil
}
