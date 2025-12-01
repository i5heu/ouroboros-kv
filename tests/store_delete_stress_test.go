package ouroboroskv__test

import (
	"fmt"
	"testing"

	ouroboroskv "github.com/i5heu/ouroboros-kv"
)

// TestRapidCreateDeleteRelationships rapidly creates and tears down trees with
// parent/child links to ensure no metadata or chunks leak between cycles.
func TestRapidCreateDeleteRelationships(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping rapid create/delete test in short mode")
	}

	store, cleanup := newDeleteAdvancedTestStore(t)
	defer cleanup()

	const (
		cycles                = 30
		childrenPerParent     = 10
		grandchildrenPerChild = 30
	)

	for cycle := 0; cycle < cycles; cycle++ {
		parentKey := mustWriteRapid(t, store, fmt.Sprintf("cycle-%d-parent", cycle), ouroboroskv.Hash{})

		childKeys := make([]ouroboroskv.Hash, 0, childrenPerParent)
		grandchildren := make(map[ouroboroskv.Hash][]ouroboroskv.Hash)

		for childIdx := 0; childIdx < childrenPerParent; childIdx++ {
			childLabel := fmt.Sprintf("cycle-%d-child-%d", cycle, childIdx)
			childKey := mustWriteRapid(t, store, childLabel, parentKey)
			childKeys = append(childKeys, childKey)

			for grandIdx := 0; grandIdx < grandchildrenPerChild; grandIdx++ {
				gcLabel := fmt.Sprintf("%s-grandchild-%d", childLabel, grandIdx)
				gcKey := mustWriteRapid(t, store, gcLabel, childKey)
				grandchildren[childKey] = append(grandchildren[childKey], gcKey)
			}
		}

		if err := store.DeleteData(parentKey); err != nil {
			t.Fatalf("cycle %d: DeleteData(parent) failed: %v", cycle, err)
		}

		for _, childKey := range childKeys {
			parent, err := store.GetParent(childKey)
			if err != nil {
				t.Fatalf("cycle %d: GetParent(child) failed: %v", cycle, err)
			}
			if parent != (ouroboroskv.Hash{}) {
				t.Fatalf("cycle %d: child %x still has parent after deleting root", cycle, childKey)
			}
		}

		for i := len(childKeys) - 1; i >= 0; i-- {
			childKey := childKeys[i]
			for _, gcKey := range grandchildren[childKey] {
				exists, err := store.DataExists(gcKey)
				if err != nil {
					t.Fatalf("cycle %d: DataExists(grandchild) failed: %v", cycle, err)
				}
				if !exists {
					t.Fatalf("cycle %d: grandchild %x missing before child delete", cycle, gcKey)
				}
				parent, err := store.GetParent(gcKey)
				if err != nil {
					t.Fatalf("cycle %d: GetParent(grandchild) failed: %v", cycle, err)
				}
				if parent != childKey {
					t.Fatalf("cycle %d: grandchild parent mismatch, expected %x got %x", cycle, childKey, parent)
				}
			}

			if err := store.DeleteData(childKey); err != nil {
				t.Fatalf("cycle %d: DeleteData(child) failed: %v", cycle, err)
			}

			for _, gcKey := range grandchildren[childKey] {
				parent, err := store.GetParent(gcKey)
				if err != nil {
					t.Fatalf("cycle %d: GetParent(orphaned grandchild) failed: %v", cycle, err)
				}
				if parent != (ouroboroskv.Hash{}) {
					t.Fatalf("cycle %d: grandchild %x still references deleted child", cycle, gcKey)
				}
			}
		}

		for _, gcList := range grandchildren {
			for _, gcKey := range gcList {
				if err := store.DeleteData(gcKey); err != nil {
					t.Fatalf("cycle %d: DeleteData(grandchild) failed: %v", cycle, err)
				}
			}
		}

		keys, err := store.ListKeys()
		if err != nil {
			t.Fatalf("cycle %d: ListKeys failed: %v", cycle, err)
		}
		if len(keys) != 0 {
			t.Fatalf("cycle %d: expected empty store, found %d keys", cycle, len(keys))
		}
	}
}

func mustWriteRapid(t *testing.T, store ouroboroskv.Store, label string, parent ouroboroskv.Hash) ouroboroskv.Hash {
	t.Helper()

	data := ouroboroskv.Data{
		Content:        []byte("rapid-content-" + label),
		Meta:           []byte("rapid-meta-" + label),
		Parent:         parent,
		RSDataSlices:   3,
		RSParitySlices: 2,
		ContentType:    "application/octet-stream",
	}

	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("WriteData(%s) failed: %v", label, err)
	}
	return key
}
