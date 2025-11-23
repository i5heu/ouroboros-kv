package store

import (
	"fmt"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v4"
	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	"github.com/i5heu/ouroboros-kv/pkg/config"
)

// setupTestKVForRelationships creates a test KV instance for relationship tests
func setupTestKVForRelationships(t *testing.T) (*KV, func()) {
	tempDir, err := os.MkdirTemp("", "ouroboros-kv-rel-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	cryptInstance := crypt.New()

	cfg := &config.Config{
		Paths:            []string{tempDir},
		MinimumFreeSpace: 1,
	}

	kv, err := Init(cryptInstance, cfg)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to init KV: %v", err)
	}

	cleanup := func() {
		kv.badgerDB.Close()
		os.RemoveAll(tempDir)
	}

	return kv, cleanup
}

// TestDeleteRelationshipEntries tests the deleteRelationshipEntries function
func TestDeleteRelationshipEntries(t *testing.T) {
	kv, cleanup := setupTestKVForRelationships(t)
	defer cleanup()

	// Create parent and child hashes
	parent := hash.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
		17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
		33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
		49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64}
	child := hash.Hash{65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
		81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96,
		97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112,
		113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128}

	// First create the relationship entries
	err := kv.badgerDB.Update(func(txn *badger.Txn) error {
		parentToChildKey := fmt.Sprintf("%s%s:%s", PARENT_PREFIX, parent, child)
		if err := txn.Set([]byte(parentToChildKey), []byte("relationship")); err != nil {
			return err
		}

		childToParentKey := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, child, parent)
		if err := txn.Set([]byte(childToParentKey), []byte("relationship")); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Failed to create relationship entries: %v", err)
	}

	// Verify the entries exist
	err = kv.badgerDB.View(func(txn *badger.Txn) error {
		parentToChildKey := fmt.Sprintf("%s%s:%s", PARENT_PREFIX, parent, child)
		_, err := txn.Get([]byte(parentToChildKey))
		if err != nil {
			return fmt.Errorf("parent->child key not found: %w", err)
		}

		childToParentKey := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, child, parent)
		_, err = txn.Get([]byte(childToParentKey))
		if err != nil {
			return fmt.Errorf("child->parent key not found: %w", err)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Failed to verify relationship entries exist: %v", err)
	}

	// Now delete the relationship entries
	err = kv.badgerDB.Update(func(txn *badger.Txn) error {
		return deleteRelationshipEntries(txn, parent, child)
	})
	if err != nil {
		t.Fatalf("deleteRelationshipEntries failed: %v", err)
	}

	// Verify the entries are deleted
	err = kv.badgerDB.View(func(txn *badger.Txn) error {
		parentToChildKey := fmt.Sprintf("%s%s:%s", PARENT_PREFIX, parent, child)
		_, err := txn.Get([]byte(parentToChildKey))
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf("parent->child key should be deleted, got error: %v", err)
		}

		childToParentKey := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, child, parent)
		_, err = txn.Get([]byte(childToParentKey))
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf("child->parent key should be deleted, got error: %v", err)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Failed to verify relationship entries deleted: %v", err)
	}
}

// TestDeleteRelationshipEntriesNonExistent tests deleting non-existent relationships
func TestDeleteRelationshipEntriesNonExistent(t *testing.T) {
	kv, cleanup := setupTestKVForRelationships(t)
	defer cleanup()

	// Create parent and child hashes that don't have relationships
	parent := hash.Hash{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}
	child := hash.Hash{2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
		2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
		2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
		2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2}

	// Delete non-existent relationships should not error
	err := kv.badgerDB.Update(func(txn *badger.Txn) error {
		return deleteRelationshipEntries(txn, parent, child)
	})
	if err != nil {
		t.Fatalf("deleteRelationshipEntries should not fail on non-existent entries: %v", err)
	}
}

// TestDeleteRelationshipEntriesMultipleChildren tests deleting one relationship while others exist
func TestDeleteRelationshipEntriesMultipleChildren(t *testing.T) {
	kv, cleanup := setupTestKVForRelationships(t)
	defer cleanup()

	// Create parent with multiple children
	parent := hash.Hash{10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
		10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
		10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10,
		10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10}
	child1 := hash.Hash{20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
		20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
		20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20,
		20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20}
	child2 := hash.Hash{30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30,
		30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30,
		30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30,
		30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30, 30}

	// Create relationships for both children
	err := kv.badgerDB.Update(func(txn *badger.Txn) error {
		// Parent -> Child1
		if err := txn.Set([]byte(fmt.Sprintf("%s%s:%s", PARENT_PREFIX, parent, child1)), []byte("rel1")); err != nil {
			return err
		}
		if err := txn.Set([]byte(fmt.Sprintf("%s%s:%s", CHILD_PREFIX, child1, parent)), []byte("rel1")); err != nil {
			return err
		}

		// Parent -> Child2
		if err := txn.Set([]byte(fmt.Sprintf("%s%s:%s", PARENT_PREFIX, parent, child2)), []byte("rel2")); err != nil {
			return err
		}
		if err := txn.Set([]byte(fmt.Sprintf("%s%s:%s", CHILD_PREFIX, child2, parent)), []byte("rel2")); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Failed to create relationships: %v", err)
	}

	// Delete only child1 relationship
	err = kv.badgerDB.Update(func(txn *badger.Txn) error {
		return deleteRelationshipEntries(txn, parent, child1)
	})
	if err != nil {
		t.Fatalf("deleteRelationshipEntries failed: %v", err)
	}

	// Verify child1 relationship is deleted
	err = kv.badgerDB.View(func(txn *badger.Txn) error {
		key1 := fmt.Sprintf("%s%s:%s", PARENT_PREFIX, parent, child1)
		_, err := txn.Get([]byte(key1))
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf("child1 parent->child key should be deleted")
		}

		key2 := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, child1, parent)
		_, err = txn.Get([]byte(key2))
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf("child1 child->parent key should be deleted")
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Child1 relationships not properly deleted: %v", err)
	}

	// Verify child2 relationship still exists
	err = kv.badgerDB.View(func(txn *badger.Txn) error {
		key1 := fmt.Sprintf("%s%s:%s", PARENT_PREFIX, parent, child2)
		_, err := txn.Get([]byte(key1))
		if err != nil {
			return fmt.Errorf("child2 parent->child key should still exist: %w", err)
		}

		key2 := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, child2, parent)
		_, err = txn.Get([]byte(key2))
		if err != nil {
			return fmt.Errorf("child2 child->parent key should still exist: %w", err)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Child2 relationships should still exist: %v", err)
	}
}

// TestDeleteRelationshipEntriesPartialFailure tests when only one key exists
func TestDeleteRelationshipEntriesPartialFailure(t *testing.T) {
	kv, cleanup := setupTestKVForRelationships(t)
	defer cleanup()

	parent := hash.Hash{40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
		40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
		40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
		40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40}
	child := hash.Hash{50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50,
		50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50,
		50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50,
		50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50}

	// Create only parent->child key (incomplete relationship)
	err := kv.badgerDB.Update(func(txn *badger.Txn) error {
		parentToChildKey := fmt.Sprintf("%s%s:%s", PARENT_PREFIX, parent, child)
		return txn.Set([]byte(parentToChildKey), []byte("partial"))
	})
	if err != nil {
		t.Fatalf("Failed to create partial relationship: %v", err)
	}

	// Delete should succeed even if only one key exists
	err = kv.badgerDB.Update(func(txn *badger.Txn) error {
		return deleteRelationshipEntries(txn, parent, child)
	})
	if err != nil {
		t.Fatalf("deleteRelationshipEntries should handle partial relationships: %v", err)
	}

	// Verify both keys are gone
	err = kv.badgerDB.View(func(txn *badger.Txn) error {
		key1 := fmt.Sprintf("%s%s:%s", PARENT_PREFIX, parent, child)
		_, err := txn.Get([]byte(key1))
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf("parent->child key should be deleted")
		}

		key2 := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, child, parent)
		_, err = txn.Get([]byte(key2))
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf("child->parent key should not exist")
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Keys not properly cleaned: %v", err)
	}
}

// TestDeleteRelationshipEntriesViaDeleteData tests that DeleteData properly cleans up relationships
func TestDeleteRelationshipEntriesViaDeleteData(t *testing.T) {
	kv, cleanup := setupTestKVForRelationships(t)
	defer cleanup()

	// Create parent data
	parentData := Data{
		Content:        []byte("parent content"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	parentKey, err := kv.WriteData(parentData)
	if err != nil {
		t.Fatalf("Failed to write parent data: %v", err)
	}

	// Create child data with parent reference
	childData := Data{
		Content:        []byte("child content"),
		Parent:         parentKey,
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	childKey, err := kv.WriteData(childData)
	if err != nil {
		t.Fatalf("Failed to write child data: %v", err)
	}

	// Verify relationship exists
	err = kv.badgerDB.View(func(txn *badger.Txn) error {
		parentToChildKey := fmt.Sprintf("%s%s:%s", PARENT_PREFIX, parentKey, childKey)
		_, err := txn.Get([]byte(parentToChildKey))
		if err != nil {
			return fmt.Errorf("parent->child relationship should exist: %w", err)
		}

		childToParentKey := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, childKey, parentKey)
		_, err = txn.Get([]byte(childToParentKey))
		if err != nil {
			return fmt.Errorf("child->parent relationship should exist: %w", err)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Relationships not created: %v", err)
	}

	// Delete parent data
	err = kv.DeleteData(parentKey)
	if err != nil {
		t.Fatalf("Failed to delete parent data: %v", err)
	}

	// Verify relationships are cleaned up after parent deletion
	// The child should now be orphaned (no parent relationship)
	err = kv.badgerDB.View(func(txn *badger.Txn) error {
		childToParentKey := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, childKey, parentKey)
		_, err := txn.Get([]byte(childToParentKey))
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf("child->parent relationship should be cleaned up after parent deletion, got: %v", err)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Relationships not cleaned up after parent deletion: %v", err)
	}
}

// TestDeleteRelationshipEntriesChain tests deleting relationships in a chain
func TestDeleteRelationshipEntriesChain(t *testing.T) {
	kv, cleanup := setupTestKVForRelationships(t)
	defer cleanup()

	// Create a chain: grandparent -> parent -> child
	grandparentData := Data{
		Content:        []byte("grandparent"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	grandparentKey, err := kv.WriteData(grandparentData)
	if err != nil {
		t.Fatalf("Failed to write grandparent: %v", err)
	}

	parentData := Data{
		Content:        []byte("parent"),
		Parent:         grandparentKey,
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	parentKey, err := kv.WriteData(parentData)
	if err != nil {
		t.Fatalf("Failed to write parent: %v", err)
	}

	childData := Data{
		Content:        []byte("child"),
		Parent:         parentKey,
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	childKey, err := kv.WriteData(childData)
	if err != nil {
		t.Fatalf("Failed to write child: %v", err)
	}

	// Delete middle node (parent)
	err = kv.DeleteData(parentKey)
	if err != nil {
		t.Fatalf("Failed to delete parent: %v", err)
	}

	// Verify grandparent->parent relationship is cleaned
	err = kv.badgerDB.View(func(txn *badger.Txn) error {
		key := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, parentKey, grandparentKey)
		_, err := txn.Get([]byte(key))
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf("parent->grandparent relationship should be deleted")
		}

		// Child->parent relationship should also be cleaned
		key2 := fmt.Sprintf("%s%s:%s", CHILD_PREFIX, childKey, parentKey)
		_, err = txn.Get([]byte(key2))
		if err != badger.ErrKeyNotFound {
			return fmt.Errorf("child->parent relationship should be cleaned: %v", err)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Chain relationships not properly cleaned: %v", err)
	}
}
