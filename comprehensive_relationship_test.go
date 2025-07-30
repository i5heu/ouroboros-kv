package ouroboroskv

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/hash"
)

func TestComprehensiveRelationships(t *testing.T) {
	// Create a temporary directory for the test
	rand.Seed(time.Now().UnixNano())
	tempDir := filepath.Join(os.TempDir(), fmt.Sprintf("ouroboros-comprehensive-test-%d", rand.Int()))
	defer os.RemoveAll(tempDir)

	// Initialize the database
	config := &StoreConfig{
		Paths:            []string{tempDir},
		MinimumFreeSpace: 1,
	}

	// Initialize crypt
	cryptInstance := crypt.New()

	kv, err := Init(cryptInstance, config)
	if err != nil {
		t.Fatalf("Failed to create OuroborosKV: %v", err)
	}
	defer kv.badgerDB.Close()

	// Store some test data with relationships
	grandparentData := Data{
		Key:                     hash.HashString("grandparent-test"),
		Content:                 []byte("I am the grandparent"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}
	parentData := Data{
		Key:                     hash.HashString("parent-test"),
		Content:                 []byte("I am the parent"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}
	child1Data := Data{
		Key:                     hash.HashString("child1-test"),
		Content:                 []byte("I am child 1"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}
	child2Data := Data{
		Key:                     hash.HashString("child2-test"),
		Content:                 []byte("I am child 2"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}
	grandchild1Data := Data{
		Key:                     hash.HashString("grandchild1-test"),
		Content:                 []byte("I am grandchild 1"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	}

	// Store grandparent first
	err = kv.WriteData(grandparentData)
	if err != nil {
		t.Fatalf("Failed to store grandparent: %v", err)
	}
	grandparentHash := grandparentData.Key
	fmt.Printf("Stored grandparent: %x\n", grandparentHash)

	// Store parent with grandparent as parent
	parentData.Parent = grandparentHash
	err = kv.WriteData(parentData)
	if err != nil {
		t.Fatalf("Failed to store parent: %v", err)
	}
	parentHash := parentData.Key
	fmt.Printf("Stored parent: %x (child of %x)\n", parentHash, grandparentHash)

	// Store child1 with parent as parent
	child1Data.Parent = parentHash
	err = kv.WriteData(child1Data)
	if err != nil {
		t.Fatalf("Failed to store child1: %v", err)
	}
	child1Hash := child1Data.Key
	fmt.Printf("Stored child1: %x (child of %x)\n", child1Hash, parentHash)

	// Store child2 with parent as parent
	child2Data.Parent = parentHash
	err = kv.WriteData(child2Data)
	if err != nil {
		t.Fatalf("Failed to store child2: %v", err)
	}
	child2Hash := child2Data.Key
	fmt.Printf("Stored child2: %x (child of %x)\n", child2Hash, parentHash)

	// Store grandchild1 with child1 as parent
	grandchild1Data.Parent = child1Hash
	err = kv.WriteData(grandchild1Data)
	if err != nil {
		t.Fatalf("Failed to store grandchild1: %v", err)
	}
	grandchild1Hash := grandchild1Data.Key
	fmt.Printf("Stored grandchild1: %x (child of %x)\n", grandchild1Hash, child1Hash)

	// Test GetChildren
	fmt.Println("\n=== Testing GetChildren ===")

	grandparentChildren, err := kv.GetChildren(grandparentHash)
	if err != nil {
		t.Fatalf("Failed to get grandparent children: %v", err)
	}
	fmt.Printf("Grandparent has %d children: %v\n", len(grandparentChildren), grandparentChildren)
	if len(grandparentChildren) != 1 {
		t.Errorf("Expected 1 child for grandparent, got %d", len(grandparentChildren))
	}

	parentChildren, err := kv.GetChildren(parentHash)
	if err != nil {
		t.Fatalf("Failed to get parent children: %v", err)
	}
	fmt.Printf("Parent has %d children: %v\n", len(parentChildren), parentChildren)
	if len(parentChildren) != 2 {
		t.Errorf("Expected 2 children for parent, got %d", len(parentChildren))
	}

	child1Children, err := kv.GetChildren(child1Hash)
	if err != nil {
		t.Fatalf("Failed to get child1 children: %v", err)
	}
	fmt.Printf("Child1 has %d children: %v\n", len(child1Children), child1Children)
	if len(child1Children) != 1 {
		t.Errorf("Expected 1 child for child1, got %d", len(child1Children))
	}

	child2Children, err := kv.GetChildren(child2Hash)
	if err != nil {
		t.Fatalf("Failed to get child2 children: %v", err)
	}
	fmt.Printf("Child2 has %d children: %v\n", len(child2Children), child2Children)
	if len(child2Children) != 0 {
		t.Errorf("Expected 0 children for child2, got %d", len(child2Children))
	}

	// Test GetParent
	fmt.Println("\n=== Testing GetParent ===")

	parentParent, err := kv.GetParent(parentHash)
	if err != nil {
		t.Fatalf("Failed to get parent's parent: %v", err)
	}
	fmt.Printf("Parent's parent: %x\n", parentParent)
	if fmt.Sprintf("%x", parentParent) != fmt.Sprintf("%x", grandparentHash) {
		t.Errorf("Parent's parent doesn't match grandparent")
	}

	child1Parent, err := kv.GetParent(child1Hash)
	if err != nil {
		t.Fatalf("Failed to get child1's parent: %v", err)
	}
	fmt.Printf("Child1's parent: %x\n", child1Parent)
	if fmt.Sprintf("%x", child1Parent) != fmt.Sprintf("%x", parentHash) {
		t.Errorf("Child1's parent doesn't match parent")
	}

	grandchild1Parent, err := kv.GetParent(grandchild1Hash)
	if err != nil {
		t.Fatalf("Failed to get grandchild1's parent: %v", err)
	}
	fmt.Printf("Grandchild1's parent: %x\n", grandchild1Parent)
	if fmt.Sprintf("%x", grandchild1Parent) != fmt.Sprintf("%x", child1Hash) {
		t.Errorf("Grandchild1's parent doesn't match child1")
	}

	// Test GetDescendants
	fmt.Println("\n=== Testing GetDescendants ===")

	grandparentDescendants, err := kv.GetDescendants(grandparentHash)
	if err != nil {
		t.Fatalf("Failed to get grandparent descendants: %v", err)
	}
	fmt.Printf("Grandparent has %d descendants: %v\n", len(grandparentDescendants), grandparentDescendants)
	if len(grandparentDescendants) != 4 { // parent, child1, child2, grandchild1
		t.Errorf("Expected 4 descendants for grandparent, got %d", len(grandparentDescendants))
	}

	parentDescendants, err := kv.GetDescendants(parentHash)
	if err != nil {
		t.Fatalf("Failed to get parent descendants: %v", err)
	}
	fmt.Printf("Parent has %d descendants: %v\n", len(parentDescendants), parentDescendants)
	if len(parentDescendants) != 3 { // child1, child2, grandchild1
		t.Errorf("Expected 3 descendants for parent, got %d", len(parentDescendants))
	}

	child1Descendants, err := kv.GetDescendants(child1Hash)
	if err != nil {
		t.Fatalf("Failed to get child1 descendants: %v", err)
	}
	fmt.Printf("Child1 has %d descendants: %v\n", len(child1Descendants), child1Descendants)
	if len(child1Descendants) != 1 { // grandchild1
		t.Errorf("Expected 1 descendant for child1, got %d", len(child1Descendants))
	}

	// Test GetAncestors
	fmt.Println("\n=== Testing GetAncestors ===")

	grandchild1Ancestors, err := kv.GetAncestors(grandchild1Hash)
	if err != nil {
		t.Fatalf("Failed to get grandchild1 ancestors: %v", err)
	}
	fmt.Printf("Grandchild1 has %d ancestors: %v\n", len(grandchild1Ancestors), grandchild1Ancestors)
	if len(grandchild1Ancestors) != 3 { // child1, parent, grandparent
		t.Errorf("Expected 3 ancestors for grandchild1, got %d", len(grandchild1Ancestors))
	}

	child2Ancestors, err := kv.GetAncestors(child2Hash)
	if err != nil {
		t.Fatalf("Failed to get child2 ancestors: %v", err)
	}
	fmt.Printf("Child2 has %d ancestors: %v\n", len(child2Ancestors), child2Ancestors)
	if len(child2Ancestors) != 2 { // parent, grandparent
		t.Errorf("Expected 2 ancestors for child2, got %d", len(child2Ancestors))
	}

	// Test GetRoots
	fmt.Println("\n=== Testing GetRoots ===")

	roots, err := kv.GetRoots()
	if err != nil {
		t.Fatalf("Failed to get roots: %v", err)
	}
	fmt.Printf("Found %d roots: %v\n", len(roots), roots)
	if len(roots) != 1 {
		t.Errorf("Expected 1 root, got %d", len(roots))
	}
	if fmt.Sprintf("%x", roots[0]) != fmt.Sprintf("%x", grandparentHash) {
		t.Errorf("Root doesn't match grandparent")
	}

	fmt.Println("\n✅ All relationship tests passed!")
}
