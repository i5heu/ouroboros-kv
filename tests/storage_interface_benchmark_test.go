package ouroboroskv__test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"testing"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	ouroboroskv "github.com/i5heu/ouroboros-kv"
	"github.com/stretchr/testify/require"
)

// Storage Interface Benchmarks - Minimal Overhead
// These benchmarks measure raw Storage interface performance with pre-generated data

// prepareTestData generates test data structures before benchmarking to minimize overhead
func prepareTestData(count int) ([]ouroboroskv.Data, []hash.Hash) {
	dataList := make([]ouroboroskv.Data, count)
	hashList := make([]hash.Hash, count)

	for i := 0; i < count; i++ {
		// Generate deterministic content
		content := make([]byte, 1024) // 1KB per entry
		binary.BigEndian.PutUint64(content[:8], uint64(i))
		for j := 8; j < len(content); j++ {
			content[j] = byte(i % 256)
		}

		dataList[i] = ouroboroskv.Data{
			Content:        content,
			ContentType:    "application/octet-stream",
			Parent:         hash.Hash{},
			Aliases:        []hash.Hash{},
			RSDataSlices:   2,
			RSParitySlices: 1,
		}

		// Pre-calculate expected hash (will be replaced with actual hash after write)
		hashList[i] = hash.Hash{}
	}

	return dataList, hashList
}

// BenchmarkStorageWrite measures raw write performance at Storage interface level
func BenchmarkStorageWrite(b *testing.B) {
	dir := b.TempDir()
	cryptInstance := crypt.New()
	config := &ouroboroskv.Config{
		Paths:            []string{dir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	kv, err := ouroboroskv.Init(cryptInstance, config)
	require.NoError(b, err)
	defer kv.Close()

	// Pre-generate data
	dataList, _ := prepareTestData(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := kv.WriteData(dataList[i])
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	// Report metrics
	bytesWritten := int64(b.N * 1024)
	b.SetBytes(1024) // 1KB per operation
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/sec")
	b.ReportMetric(float64(bytesWritten)/(1024*1024)/b.Elapsed().Seconds(), "MB/sec")
}

// BenchmarkStorageRead measures raw read performance at Storage interface level
func BenchmarkStorageRead(b *testing.B) {
	dir := b.TempDir()
	cryptInstance := crypt.New()
	config := &ouroboroskv.Config{
		Paths:            []string{dir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	kv, err := ouroboroskv.Init(cryptInstance, config)
	require.NoError(b, err)
	defer kv.Close()

	// Pre-generate and write data
	dataList, hashList := prepareTestData(b.N)
	for i := 0; i < b.N; i++ {
		h, err := kv.WriteData(dataList[i])
		require.NoError(b, err)
		hashList[i] = h
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := kv.ReadData(hashList[i])
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	// Report metrics
	bytesRead := int64(b.N * 1024)
	b.SetBytes(1024) // 1KB per operation
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "reads/sec")
	b.ReportMetric(float64(bytesRead)/(1024*1024)/b.Elapsed().Seconds(), "MB/sec")
}

// BenchmarkStorageDelete measures raw delete performance at Storage interface level
func BenchmarkStorageDelete(b *testing.B) {
	dir := b.TempDir()
	cryptInstance := crypt.New()
	config := &ouroboroskv.Config{
		Paths:            []string{dir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	kv, err := ouroboroskv.Init(cryptInstance, config)
	require.NoError(b, err)
	defer kv.Close()

	// Pre-generate and write data
	dataList, hashList := prepareTestData(b.N)
	for i := 0; i < b.N; i++ {
		h, err := kv.WriteData(dataList[i])
		require.NoError(b, err)
		hashList[i] = h
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := kv.DeleteData(hashList[i])
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	// Report metrics
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "deletes/sec")
}

// BenchmarkStorageBatchWrite measures batch write performance
func BenchmarkStorageBatchWrite(b *testing.B) {
	dir := b.TempDir()
	cryptInstance := crypt.New()
	config := &ouroboroskv.Config{
		Paths:            []string{dir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	kv, err := ouroboroskv.Init(cryptInstance, config)
	require.NoError(b, err)
	defer kv.Close()

	batchSizes := []int{10, 50, 100}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize%d", batchSize), func(b *testing.B) {
			// Pre-generate batches
			batches := make([][]ouroboroskv.Data, (b.N+batchSize-1)/batchSize)
			for i := range batches {
				batchData, _ := prepareTestData(batchSize)
				batches[i] = batchData
			}

			totalOps := 0
			b.ResetTimer()
			for i := 0; i < len(batches) && totalOps < b.N; i++ {
				_, err := kv.BatchWriteData(batches[i])
				if err != nil {
					b.Fatal(err)
				}
				totalOps += batchSize
			}
			b.StopTimer()

			bytesWritten := int64(totalOps * 1024)
			b.SetBytes(int64(batchSize * 1024))
			b.ReportMetric(float64(totalOps)/b.Elapsed().Seconds(), "writes/sec")
			b.ReportMetric(float64(bytesWritten)/(1024*1024)/b.Elapsed().Seconds(), "MB/sec")
		})
	}
}

// BenchmarkStorageBatchRead measures batch read performance
func BenchmarkStorageBatchRead(b *testing.B) {
	dir := b.TempDir()
	cryptInstance := crypt.New()
	config := &ouroboroskv.Config{
		Paths:            []string{dir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	kv, err := ouroboroskv.Init(cryptInstance, config)
	require.NoError(b, err)
	defer kv.Close()

	batchSizes := []int{10, 50, 100}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize%d", batchSize), func(b *testing.B) {
			// Pre-generate and write data
			totalNeeded := b.N * batchSize
			dataList, _ := prepareTestData(totalNeeded)

			allHashes := make([]hash.Hash, totalNeeded)
			for i := 0; i < totalNeeded; i++ {
				h, err := kv.WriteData(dataList[i])
				require.NoError(b, err)
				allHashes[i] = h
			}

			// Prepare batches of hashes
			batches := make([][]hash.Hash, b.N)
			for i := 0; i < b.N; i++ {
				batches[i] = allHashes[i*batchSize : (i+1)*batchSize]
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := kv.BatchReadData(batches[i])
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()

			bytesRead := int64(b.N * batchSize * 1024)
			b.SetBytes(int64(batchSize * 1024))
			b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "reads/sec")
			b.ReportMetric(float64(bytesRead)/(1024*1024)/b.Elapsed().Seconds(), "MB/sec")
		})
	}
}

// BenchmarkStorageDataExists measures key existence check performance
func BenchmarkStorageDataExists(b *testing.B) {
	dir := b.TempDir()
	cryptInstance := crypt.New()
	config := &ouroboroskv.Config{
		Paths:            []string{dir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	kv, err := ouroboroskv.Init(cryptInstance, config)
	require.NoError(b, err)
	defer kv.Close()

	// Pre-generate and write a fixed dataset of 1000 entries
	datasetSize := 1000
	dataList, hashList := prepareTestData(datasetSize)
	for i := 0; i < datasetSize; i++ {
		h, err := kv.WriteData(dataList[i])
		require.NoError(b, err)
		hashList[i] = h
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Cycle through the dataset
		exists, err := kv.DataExists(hashList[i%datasetSize])
		if err != nil {
			b.Fatal(err)
		}
		if !exists {
			b.Fatal("expected data to exist")
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "checks/sec")
}

// BenchmarkStorageGetDataInfo measures metadata retrieval performance
func BenchmarkStorageGetDataInfo(b *testing.B) {
	dir := b.TempDir()
	cryptInstance := crypt.New()
	config := &ouroboroskv.Config{
		Paths:            []string{dir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	kv, err := ouroboroskv.Init(cryptInstance, config)
	require.NoError(b, err)
	defer kv.Close()

	// Pre-generate and write a fixed dataset of 1000 entries
	datasetSize := 1000
	dataList, hashList := prepareTestData(datasetSize)
	for i := 0; i < datasetSize; i++ {
		h, err := kv.WriteData(dataList[i])
		require.NoError(b, err)
		hashList[i] = h
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Cycle through the dataset
		_, err := kv.GetDataInfo(hashList[i%datasetSize])
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "info/sec")
}

// BenchmarkStorageWriteReadDelete measures complete lifecycle performance
func BenchmarkStorageWriteReadDelete(b *testing.B) {
	dir := b.TempDir()
	cryptInstance := crypt.New()
	config := &ouroboroskv.Config{
		Paths:            []string{dir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	kv, err := ouroboroskv.Init(cryptInstance, config)
	require.NoError(b, err)
	defer kv.Close()

	// Pre-generate data
	dataList, _ := prepareTestData(b.N)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Write
		h, err := kv.WriteData(dataList[i])
		if err != nil {
			b.Fatal(err)
		}

		// Read
		_, err = kv.ReadData(h)
		if err != nil {
			b.Fatal(err)
		}

		// Delete
		err = kv.DeleteData(h)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	b.SetBytes(1024 * 3) // 1KB * 3 operations
	b.ReportMetric(float64(b.N*3)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkStorageHierarchical measures hierarchical relationship performance
func BenchmarkStorageHierarchical(b *testing.B) {
	dir := b.TempDir()
	cryptInstance := crypt.New()
	config := &ouroboroskv.Config{
		Paths:            []string{dir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	kv, err := ouroboroskv.Init(cryptInstance, config)
	require.NoError(b, err)
	defer kv.Close()

	// Create a parent hierarchy: 1 root -> 10 children each -> 10 grandchildren each
	content := bytes.Repeat([]byte{0xFF}, 512)

	// Pre-create root
	rootData := ouroboroskv.Data{
		Content:        content,
		ContentType:    "application/octet-stream",
		Parent:         hash.Hash{},
		RSDataSlices:   2,
		RSParitySlices: 1,
	}
	rootHash, err := kv.WriteData(rootData)
	require.NoError(b, err)

	// Pre-create children
	childHashes := make([]hash.Hash, 10)
	for i := 0; i < 10; i++ {
		childData := ouroboroskv.Data{
			Content:        content,
			ContentType:    "application/octet-stream",
			Parent:         rootHash,
			RSDataSlices:   2,
			RSParitySlices: 1,
		}
		childHashes[i], err = kv.WriteData(childData)
		require.NoError(b, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Cycle through operations on the hierarchy
		switch i % 4 {
		case 0: // Get children
			_, err := kv.GetChildren(rootHash)
			if err != nil {
				b.Fatal(err)
			}
		case 1: // Get parent
			_, err := kv.GetParent(childHashes[i%len(childHashes)])
			if err != nil {
				b.Fatal(err)
			}
		case 2: // Get ancestors
			_, err := kv.GetAncestors(childHashes[i%len(childHashes)])
			if err != nil {
				b.Fatal(err)
			}
		case 3: // Get descendants
			_, err := kv.GetDescendants(rootHash)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "nav/sec")
}
