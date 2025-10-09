package ouroboroskv

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestListStoredDataSkipsCorruptedEntries(t *testing.T) {
	kv, _, cleanup := setupKVForIntegration(t)
	defer cleanup()

	data := applyTestDefaults(Data{
		MetaData:                []byte("list-corruption"),
		Content:                 []byte("list-corruption-content"),
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
			return fmt.Errorf("expected shards for list test")
		}
		chunkKey := fmt.Sprintf("%s%x_%d", CHUNK_PREFIX, metadata.ShardHashes[0], 0)
		return txn.Set([]byte(chunkKey), []byte{0xFF})
	})
	require.NoError(t, err)

	infos, err := kv.ListStoredData()
	require.NoError(t, err)
	require.Empty(t, infos)
}
