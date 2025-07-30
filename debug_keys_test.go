package ouroboroskv

import (
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/i5heu/ouroboros-crypt/hash"
)

func TestDebugKeys(t *testing.T) {
	kv, cleanup := setupTestKVForStorage(t)
	defer cleanup()

	// Write one data object
	data := Data{
		Key:                     hash.HashString("debug-test"),
		Content:                 []byte("Debug test content"),
		ReedSolomonShards:       2,
		ReedSolomonParityShards: 1,
	}

	err := kv.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData failed: %v", err)
	}

	// Debug: List all keys in the database
	err = kv.badgerDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		fmt.Printf("=== All keys in database ===\n")
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())
			fmt.Printf("Key: %s (len=%d)\n", key, len(key))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to debug keys: %v", err)
	}

	// Debug: Expected key format
	expectedKey := fmt.Sprintf("%s%x", METADATA_PREFIX, data.Key)
	fmt.Printf("=== Expected key format ===\n")
	fmt.Printf("Expected key: %s (len=%d)\n", expectedKey, len(expectedKey))
	fmt.Printf("Metadata prefix: %s (len=%d)\n", METADATA_PREFIX, len(METADATA_PREFIX))
	fmt.Printf("Hash hex: %x (len=%d)\n", data.Key, len(fmt.Sprintf("%x", data.Key)))

	// Test ListKeys function
	listedKeys, err := kv.ListKeys()
	if err != nil {
		t.Fatalf("ListKeys failed: %v", err)
	}

	fmt.Printf("=== ListKeys result ===\n")
	fmt.Printf("Found %d keys\n", len(listedKeys))
	for i, key := range listedKeys {
		fmt.Printf("Listed key %d: %x\n", i, key)
	}

	// Manual test of HashHexadecimal function
	fmt.Printf("=== Manual HashHexadecimal test ===\n")
	testHashHex := "6266636639333863663661613864383565326264613462643664383061346438613535633638363461386631383965633361393066313865656532396433653138643939353062363033636262306630633161623930313862303536363332663362336539616666393838653434326262383633616631363335393330366630"
	fmt.Printf("Full hex length: %d\n", len(testHashHex))

	// Try with first 128 chars
	shortHex := testHashHex[:128]
	fmt.Printf("Short hex length: %d\n", len(shortHex))
	testHash, err := hash.HashHexadecimal(shortHex)
	if err != nil {
		fmt.Printf("HashHexadecimal error: %v\n", err)
	} else {
		fmt.Printf("HashHexadecimal success: %x\n", testHash)
	}

	// Compare original key with listed key
	fmt.Printf("=== Comparison ===\n")
	fmt.Printf("Original key: %x\n", data.Key)
	if len(listedKeys) > 0 {
		fmt.Printf("Listed key:   %x\n", listedKeys[0])
		fmt.Printf("Keys equal:   %v\n", data.Key == listedKeys[0])
	}
}
