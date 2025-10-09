package ouroboroskv

import (
	"fmt"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func TestCollectChildrenSkipsInvalidEntries(t *testing.T) {
	kv, _, cleanup := setupKVForIntegration(t)
	defer cleanup()

	parentData := applyTestDefaults(Data{
		MetaData:                []byte("parent-invalid-check"),
		Content:                 []byte("parent-invalid-content"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	})

	parentKey, err := kv.WriteData(parentData)
	require.NoError(t, err)

	childData := applyTestDefaults(Data{
		MetaData:                []byte("child-invalid-check"),
		Content:                 []byte("child-invalid-content"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
		Parent:                  parentKey,
	})

	childKey, err := kv.WriteData(childData)
	require.NoError(t, err)

	// Seed malformed relationship records that should be ignored by collectChildrenForTxn.
	err = kv.badgerDB.Update(func(txn *badger.Txn) error {
		prefix := fmt.Sprintf("%s%x", PARENT_PREFIX, parentKey)
		if err := txn.Set([]byte(prefix), []byte("")); err != nil {
			return err
		}

		shortKey := fmt.Sprintf("%s%x:%s", PARENT_PREFIX, parentKey, "deadbeef")
		if err := txn.Set([]byte(shortKey), []byte("")); err != nil {
			return err
		}

		badHex := fmt.Sprintf("%s%x:%s", PARENT_PREFIX, parentKey, strings.Repeat("g", 128))
		if err := txn.Set([]byte(badHex), []byte("")); err != nil {
			return err
		}

		duplicate := fmt.Sprintf("%s%x:%x", PARENT_PREFIX, parentKey, childKey)
		if err := txn.Set([]byte(duplicate), []byte("")); err != nil {
			return err
		}

		return nil
	})
	require.NoError(t, err)

	children, err := kv.GetChildren(parentKey)
	require.NoError(t, err)
	require.Contains(t, children, childKey)
}
