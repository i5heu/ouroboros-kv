package ouroboroskv

import (
	"sync/atomic"

	"github.com/dgraph-io/badger/v4"
)

func (k *KV) Write(key []byte, content []byte) error {
	atomic.AddUint64(&k.writeCounter, 1)

	err := k.badgerDB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, content)
	})
	if err != nil {
		log.Fatal(err)
		return err
	}
	return err
}
