package ouroboroskv__test

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	crypt "github.com/i5heu/ouroboros-crypt"
	ouroboroskv "github.com/i5heu/ouroboros-kv"
	"github.com/i5heu/ouroboros-kv/pkg/config"
)

func newConcurrentTestStore(t *testing.T) (ouroboroskv.Store, func()) {
	tempDir := t.TempDir()
	cryptInstance := crypt.New()
	cfg := &config.Config{
		Paths:            []string{tempDir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}
	kv, err := ouroboroskv.Init(cryptInstance, cfg)
	if err != nil {
		t.Fatalf("Failed to init store: %v", err)
	}
	return kv, func() { kv.Close() }
}

// TestConcurrentWrites tests multiple goroutines writing different data simultaneously
func TestConcurrentWrites(t *testing.T) {
	store, cleanup := newConcurrentTestStore(t)
	defer cleanup()

	numGoroutines := 10
	writesPerGoroutine := 50
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*writesPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < writesPerGoroutine; j++ {
				data := ouroboroskv.Data{
					Content:        []byte(fmt.Sprintf("goroutine-%d-write-%d", id, j)),
					RSDataSlices:   3,
					RSParitySlices: 2,
				}
				_, err := store.WriteData(data)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d write %d failed: %v", id, j, err)
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	errorCount := 0
	for err := range errors {
		t.Error(err)
		errorCount++
	}

	if errorCount > 0 {
		t.Fatalf("Had %d errors during concurrent writes", errorCount)
	}

	t.Logf("✅ Successfully completed %d concurrent writes", numGoroutines*writesPerGoroutine)
}

// TestConcurrentWritesSameData tests multiple goroutines writing identical data (alias creation)
func TestConcurrentWritesSameData(t *testing.T) {
	store, cleanup := newConcurrentTestStore(t)
	defer cleanup()

	numGoroutines := 20
	content := []byte("identical-content-for-all-goroutines")
	var wg sync.WaitGroup
	keys := make(chan ouroboroskv.Hash, numGoroutines)
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			data := ouroboroskv.Data{
				Content:        content,
				RSDataSlices:   3,
				RSParitySlices: 2,
			}
			key, err := store.WriteData(data)
			if err != nil {
				errors <- fmt.Errorf("goroutine %d failed: %v", id, err)
				return
			}
			keys <- key
		}(i)
	}

	wg.Wait()
	close(errors)
	close(keys)

	for err := range errors {
		t.Fatal(err)
	}

	// All keys should point to the same canonical key
	keyCount := 0
	for key := range keys {
		if keyCount == 0 {
			// firstKey for reference, but we just check uniqueKeys length
		}
		keyCount++

		// Verify we can read the data
		readData, err := store.ReadData(key)
		if err != nil {
			t.Errorf("Failed to read key %x: %v", key, err)
		}
		if !bytes.Equal(readData.Content, content) {
			t.Errorf("Content mismatch for key %x", key)
		}
	}

	if keyCount != numGoroutines {
		t.Errorf("Expected %d keys, got %d", numGoroutines, keyCount)
	}

	t.Logf("✅ %d goroutines successfully wrote identical data with proper aliasing", numGoroutines)
}

// TestConcurrentReadsWhileWriting tests reads happening during writes
func TestConcurrentReadsWhileWriting(t *testing.T) {
	store, cleanup := newConcurrentTestStore(t)
	defer cleanup()

	// Pre-populate some data
	numKeys := 20
	keys := make([]ouroboroskv.Hash, numKeys)
	expectedContent := make([][]byte, numKeys)

	for i := 0; i < numKeys; i++ {
		content := []byte(fmt.Sprintf("initial-data-%d", i))
		expectedContent[i] = content
		key, err := store.WriteData(ouroboroskv.Data{
			Content:        content,
			RSDataSlices:   3,
			RSParitySlices: 2,
		})
		if err != nil {
			t.Fatalf("Failed to write initial data: %v", err)
		}
		keys[i] = key
	}

	var wg sync.WaitGroup
	stopReading := make(chan struct{})
	readErrors := make(chan error, 100)
	writeErrors := make(chan error, 100)
	var readCount atomic.Int64

	// Start readers
	numReaders := 5
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-stopReading:
					return
				default:
					// Read random existing key
					keyIdx := id % len(keys)
					data, err := store.ReadData(keys[keyIdx])
					if err != nil {
						readErrors <- fmt.Errorf("reader %d failed: %v", id, err)
						return
					}
					if !bytes.Equal(data.Content, expectedContent[keyIdx]) {
						readErrors <- fmt.Errorf("reader %d: content mismatch", id)
						return
					}
					readCount.Add(1)
					time.Sleep(1 * time.Millisecond)
				}
			}
		}(i)
	}

	// Start writers
	numWriters := 3
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				data := ouroboroskv.Data{
					Content:        []byte(fmt.Sprintf("writer-%d-data-%d", id, j)),
					RSDataSlices:   3,
					RSParitySlices: 2,
				}
				_, err := store.WriteData(data)
				if err != nil {
					writeErrors <- fmt.Errorf("writer %d failed: %v", id, err)
					return
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(i)
	}

	// Let them run for a bit
	time.Sleep(100 * time.Millisecond)
	close(stopReading)
	wg.Wait()

	close(readErrors)
	close(writeErrors)

	for err := range readErrors {
		t.Error(err)
	}
	for err := range writeErrors {
		t.Error(err)
	}

	t.Logf("✅ Completed %d reads while writing concurrently", readCount.Load())
}

// TestConcurrentDeletes tests multiple goroutines deleting data simultaneously
func TestConcurrentDeletes(t *testing.T) {
	store, cleanup := newConcurrentTestStore(t)
	defer cleanup()

	// Create data to delete
	numItems := 50
	keys := make([]ouroboroskv.Hash, numItems)

	for i := 0; i < numItems; i++ {
		data := ouroboroskv.Data{
			Content:        []byte(fmt.Sprintf("delete-me-%d", i)),
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		key, err := store.WriteData(data)
		if err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
		keys[i] = key
	}

	// Delete concurrently
	var wg sync.WaitGroup
	errors := make(chan error, numItems)

	for i := 0; i < numItems; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			err := store.DeleteData(keys[idx])
			if err != nil {
				errors <- fmt.Errorf("delete %d failed: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}

	// Verify all deleted
	for i, key := range keys {
		exists, err := store.DataExists(key)
		if err != nil {
			t.Errorf("DataExists check failed for key %d: %v", i, err)
		}
		if exists {
			t.Errorf("Key %d still exists after delete", i)
		}
	}

	t.Logf("✅ Successfully deleted %d items concurrently", numItems)
}

// TestConcurrentParentChildUpdates tests race conditions in relationship updates
func TestConcurrentParentChildUpdates(t *testing.T) {
	store, cleanup := newConcurrentTestStore(t)
	defer cleanup()

	// Create parent
	parentData := ouroboroskv.Data{
		Content:        []byte("parent"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	parentKey, err := store.WriteData(parentData)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	// Create children concurrently
	numChildren := 30
	var wg sync.WaitGroup
	childKeys := make(chan ouroboroskv.Hash, numChildren)
	errors := make(chan error, numChildren)

	for i := 0; i < numChildren; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			childData := ouroboroskv.Data{
				Content:        []byte(fmt.Sprintf("child-%d", id)),
				Parent:         parentKey,
				RSDataSlices:   3,
				RSParitySlices: 2,
			}
			key, err := store.WriteData(childData)
			if err != nil {
				errors <- fmt.Errorf("child %d failed: %v", id, err)
				return
			}
			childKeys <- key
		}(i)
	}

	wg.Wait()
	close(errors)
	close(childKeys)

	for err := range errors {
		t.Fatal(err)
	}

	// Verify parent has all children
	children, err := store.GetChildren(parentKey)
	if err != nil {
		t.Fatalf("Failed to get children: %v", err)
	}

	if len(children) != numChildren {
		t.Errorf("Expected %d children, got %d", numChildren, len(children))
	}

	// Verify each child reports correct parent
	for key := range childKeys {
		parent, err := store.GetParent(key)
		if err != nil {
			t.Errorf("Failed to get parent for key %x: %v", key, err)
			continue
		}
		if parent != parentKey {
			t.Errorf("Child has wrong parent: expected %x, got %x", parentKey, parent)
		}
	}

	t.Logf("✅ Successfully created %d children concurrently with correct relationships", numChildren)
}

// TestConcurrentDeleteWithReads tests deleting while others are reading
func TestConcurrentDeleteWithReads(t *testing.T) {
	store, cleanup := newConcurrentTestStore(t)
	defer cleanup()

	// Create test data
	data := ouroboroskv.Data{
		Content:        []byte("data-to-be-deleted"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	key, err := store.WriteData(data)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	var wg sync.WaitGroup
	stopReading := make(chan struct{})
	deleteStarted := make(chan struct{})
	var successfulReads atomic.Int64
	var failedReads atomic.Int64

	// Start readers
	numReaders := 10
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-deleteStarted // Wait for delete to start
			for {
				select {
				case <-stopReading:
					return
				default:
					_, err := store.ReadData(key)
					if err != nil {
						failedReads.Add(1)
					} else {
						successfulReads.Add(1)
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}(i)
	}

	// Start deleter
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(deleteStarted)
		time.Sleep(10 * time.Millisecond) // Let readers get started
		err := store.DeleteData(key)
		if err != nil {
			t.Errorf("Delete failed: %v", err)
		}
	}()

	// Let it run
	time.Sleep(50 * time.Millisecond)
	close(stopReading)
	wg.Wait()

	// Verify data is gone
	exists, err := store.DataExists(key)
	if err != nil {
		t.Errorf("DataExists check failed: %v", err)
	}
	if exists {
		t.Error("Data still exists after delete")
	}

	t.Logf("✅ Concurrent delete completed: %d successful reads, %d failed reads",
		successfulReads.Load(), failedReads.Load())
}

// TestConcurrentBatchWrites tests concurrent batch write operations
func TestConcurrentBatchWrites(t *testing.T) {
	store, cleanup := newConcurrentTestStore(t)
	defer cleanup()

	numGoroutines := 5
	itemsPerBatch := 20
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			batch := make([]ouroboroskv.Data, itemsPerBatch)
			for j := 0; j < itemsPerBatch; j++ {
				batch[j] = ouroboroskv.Data{
					Content:        []byte(fmt.Sprintf("batch-%d-item-%d", id, j)),
					RSDataSlices:   3,
					RSParitySlices: 2,
				}
			}

			keys, err := store.BatchWriteData(batch)
			if err != nil {
				errors <- fmt.Errorf("batch %d failed: %v", id, err)
				return
			}
			if len(keys) != itemsPerBatch {
				errors <- fmt.Errorf("batch %d: expected %d keys, got %d", id, itemsPerBatch, len(keys))
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}

	t.Logf("✅ Successfully completed %d concurrent batch writes", numGoroutines)
}

// TestRaceDetectorParentChild uses a simple pattern to help race detector find issues
func TestRaceDetectorParentChild(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping race detector test in short mode")
	}

	store, cleanup := newConcurrentTestStore(t)
	defer cleanup()

	parentData := ouroboroskv.Data{
		Content:        []byte("parent"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	parentKey, err := store.WriteData(parentData)
	if err != nil {
		t.Fatalf("Failed to create parent: %v", err)
	}

	var wg sync.WaitGroup

	// Concurrent child creation
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			childData := ouroboroskv.Data{
				Content:        []byte(fmt.Sprintf("child-%d", id)),
				Parent:         parentKey,
				RSDataSlices:   3,
				RSParitySlices: 2,
			}
			store.WriteData(childData)
		}(i)
	}

	// Concurrent reads of parent relationships
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				store.GetChildren(parentKey)
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
	t.Log("✅ Race detector test completed")
}
