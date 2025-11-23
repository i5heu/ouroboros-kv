package ouroboroskv

import (
	"fmt"
	"os"
	"testing"

	"log/slog"

	"github.com/dgraph-io/badger/v4"
	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	"github.com/stretchr/testify/require"
)

func TestDebugParentChildStorage(t *testing.T) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "ouroboros-debug-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a crypt instance for testing
	cryptInstance := crypt.New()

	// Create config
	config := &Config{
		Paths:            []string{tempDir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	// Initialize KV
	kv, err := Init(cryptInstance, config)
	require.NoError(t, err)
	defer kv.badgerDB.Close()

	// Create simple test data
	parentData := Data{
		Meta:           []byte("debug parent metadata"),
		Content:        []byte("I am the parent"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}

	// Store parent first
	parentKey, err := kv.WriteData(parentData)
	require.NoError(t, err)
	fmt.Printf("Storing parent with key: %x\n", parentKey)

	childData := Data{
		Meta:           []byte("debug child metadata"),
		Content:        []byte("I am child"),
		Parent:         parentKey,
		RSDataSlices:   3,
		RSParitySlices: 2,
	}

	// Store child
	childKey, err := kv.WriteData(childData)
	require.NoError(t, err)
	fmt.Printf("Storing child with key: %x and parent: %x\n", childKey, parentKey)

	// Debug: Check what keys exist in the database
	kv.badgerDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		fmt.Println("All keys in database:")
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()
			fmt.Printf("  Key: %s\n", string(key))
		}
		return nil
	})

	// Test GetChildren with debugging
	fmt.Printf("Looking for children of parent: %x\n", parentKey)

	// Create the exact prefix we'll be searching for
	searchPrefix := fmt.Sprintf("%s%x:", PARENT_PREFIX, parentKey)
	fmt.Printf("Search prefix: %s\n", searchPrefix) // Check if any keys match this prefix
	kv.badgerDB.View(func(txn *badger.Txn) error {
		prefix := []byte(searchPrefix)
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		fmt.Printf("Keys matching prefix '%s':\n", searchPrefix)
		count := 0
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			fmt.Printf("  Matched: %s\n", string(key))
			count++
		}
		fmt.Printf("Total matches: %d\n", count)
		return nil
	})

	children, err := kv.GetChildren(parentKey)
	require.NoError(t, err)
	fmt.Printf("Found %d children: %v\n", len(children), children)

	// Let me manually test the parsing logic
	testKey := "parent:ca554068e9109c5fbb193bdaeec5bcbcfe2754528f3cf52a4078f00f76138b3071dc37aa16b7c727fce21f56b5252c1cf41c26f3605879004aeff5af75e00ce0:bc0a7a7acbebda6b21f2eef5ff2a4f5f1629bd0212e5db0f654555874c00e13421c494db2fdab1abc192b665d851acae71e80d7f4971fe7c83be3a3115786708"
	prefix := fmt.Sprintf("%s%x:", PARENT_PREFIX, parentKey)
	fmt.Printf("Prefix: '%s'\n", prefix)
	fmt.Printf("Test key: '%s'\n", testKey)
	fmt.Printf("Prefix length: %d\n", len(prefix))

	if len(testKey) > len(prefix) {
		childHashHex := testKey[len(prefix):]
		fmt.Printf("Extracted child hash hex: '%s'\n", childHashHex)
		fmt.Printf("Child hash hex length: %d\n", len(childHashHex))

		if len(childHashHex) == 128 {
			childHash, err := hash.HashHexadecimal(childHashHex)
			if err == nil {
				fmt.Printf("Successfully parsed child hash: %x\n", childHash)
			} else {
				fmt.Printf("Failed to parse child hash: %v\n", err)
			}
		} else {
			fmt.Printf("Wrong length: expected 128, got %d\n", len(childHashHex))
		}
	}

	// Test GetParent
	fmt.Printf("Looking for parent of child: %x\n", childKey)
	parent, err := kv.GetParent(childKey)
	require.NoError(t, err)
	fmt.Printf("Found parent: %s\n", parent)
}
