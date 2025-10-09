package ouroboroskv

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestValidateAllReportsCorruption(t *testing.T) {
	kv, _, cleanup := setupKVForIntegration(t)
	defer cleanup()

	data := applyTestDefaults(Data{
		MetaData:                []byte("validation-corruption"),
		Content:                 []byte("validation-content"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	})

	key, err := kv.WriteData(data)
	require.NoError(t, err)

	err = kv.badgerDB.Update(func(txn *badger.Txn) error {
		metadata, err := kv.loadMetadata(txn, key)
		if err != nil {
			return err
		}
		if len(metadata.ShardHashes) == 0 {
			return fmt.Errorf("expected shard hashes for validation test")
		}
		chunkKey := fmt.Sprintf("%s%x_%d", CHUNK_PREFIX, metadata.ShardHashes[0], 0)
		return txn.Set([]byte(chunkKey), []byte{0x00, 0x01})
	})
	require.NoError(t, err)

	results, err := kv.ValidateAll()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Error(t, results[0].Err)
}

func TestValidateKeyDetectsMismatch(t *testing.T) {
	kv, _, cleanup := setupKVForIntegration(t)
	defer cleanup()

	data := applyTestDefaults(Data{
		MetaData:                []byte("validation-mismatch"),
		Content:                 []byte("validation-mismatch-content"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	})

	key, err := kv.WriteData(data)
	require.NoError(t, err)

	err = kv.badgerDB.Update(func(txn *badger.Txn) error {
		metadata, err := kv.loadMetadata(txn, key)
		if err != nil {
			return err
		}
		metadata.Created++
		return kv.storeMetadata(txn, metadata)
	})
	require.NoError(t, err)

	err = kv.ValidateKey(key)
	require.Error(t, err)
}
