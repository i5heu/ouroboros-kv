package ouroboroskv__test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync/atomic"
	"testing"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	ouroboroskv "github.com/i5heu/ouroboros-kv"
	"github.com/stretchr/testify/require"
)

// BenchmarkRealisticWorkload simulates real-world usage patterns with 100% deterministic operations.
// It creates a single DB instance and performs a mix of:
// - Document storage with hierarchical relationships (simulating file systems)
// - Cache-like ephemeral data with short lifecycles
// - Batch operations for efficiency
// - Concurrent reads of stable data
// - Integrity verification throughout
func BenchmarkRealisticWorkload(b *testing.B) {
	// Initialize DB once for the entire benchmark
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

	tracker := newLiveKeyTracker()

	// Operation counters for performance statistics
	var (
		totalReads   atomic.Uint64
		totalWrites  atomic.Uint64
		totalDeletes atomic.Uint64
		batchWrites  atomic.Uint64
		batchReads   atomic.Uint64
		cacheHits    atomic.Uint64
		validations  atomic.Uint64
		navigations  atomic.Uint64
	)

	// Phase 1: Create stable baseline dataset (simulating initial system state)
	const (
		projectCount     = 8  // Simulate 8 projects
		filesPerProject  = 16 // Each project has 16 files
		versionsPerFile  = 3  // Each file has 3 versions
		cacheBucketCount = 4  // Number of cache buckets
		cacheItemsPerOp  = 5  // Cache items per operation
	)

	type project struct {
		rootKey  hash.Hash
		fileKeys []hash.Hash
		versions [][]hash.Hash // versions[fileIdx][versionIdx]
	}

	projects := make([]project, projectCount)

	// Create project hierarchy (simulating file system structure)
	for p := 0; p < projectCount; p++ {
		// Create project root
		rootContent := make([]byte, 512)
		binary.BigEndian.PutUint64(rootContent, uint64(p))
		copy(rootContent[8:], []byte(fmt.Sprintf("project-%d-root", p)))

		rootData := ouroboroskv.Data{
			Content:        rootContent,
			ContentType:    "application/project-root",
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		rootKey, err := kv.WriteData(rootData)
		require.NoError(b, err)
		tracker.add(rootKey)

		projects[p].rootKey = rootKey
		projects[p].fileKeys = make([]hash.Hash, filesPerProject)
		projects[p].versions = make([][]hash.Hash, filesPerProject)

		// Create files for this project
		for f := 0; f < filesPerProject; f++ {
			fileContent := make([]byte, 1024)
			binary.BigEndian.PutUint64(fileContent, uint64(p*1000+f))
			copy(fileContent[8:], []byte(fmt.Sprintf("project-%d-file-%d", p, f)))

			fileData := ouroboroskv.Data{
				Content:        fileContent,
				Parent:         rootKey,
				ContentType:    "application/file",
				Meta:           []byte{byte(p), byte(f)},
				RSDataSlices:   3,
				RSParitySlices: 2,
			}
			fileKey, err := kv.WriteData(fileData)
			require.NoError(b, err)
			tracker.add(fileKey)

			projects[p].fileKeys[f] = fileKey
			projects[p].versions[f] = make([]hash.Hash, versionsPerFile)

			// Create versions for this file
			for v := 0; v < versionsPerFile; v++ {
				versionContent := make([]byte, 2048)
				binary.BigEndian.PutUint64(versionContent, uint64(p*10000+f*100+v))
				for i := 8; i < len(versionContent); i++ {
					versionContent[i] = byte((p + f + v + i) % 251)
				}

				versionData := ouroboroskv.Data{
					Content:        versionContent,
					Parent:         fileKey,
					ContentType:    "application/file-version",
					Meta:           []byte{byte(v)},
					RSDataSlices:   3,
					RSParitySlices: 2,
				}
				versionKey, err := kv.WriteData(versionData)
				require.NoError(b, err)
				tracker.add(versionKey)

				projects[p].versions[f][v] = versionKey
			}
		}
	}

	// Verify initial structure
	for p := 0; p < projectCount; p++ {
		children, err := kv.GetChildren(projects[p].rootKey)
		require.NoError(b, err)
		require.GreaterOrEqual(b, len(children), filesPerProject, "project %d missing files", p)

		for f := 0; f < filesPerProject; f++ {
			versions, err := kv.GetChildren(projects[p].fileKeys[f])
			require.NoError(b, err)
			require.GreaterOrEqual(b, len(versions), versionsPerFile, "file %d missing versions", f)
		}
	}

	// Track expected static keys count
	expectedStaticCount := projectCount + (projectCount * filesPerProject) + (projectCount * filesPerProject * versionsPerFile)

	var opCounter uint64

	b.ResetTimer()

	// Phase 2: Realistic concurrent workload simulation
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			opID := atomic.AddUint64(&opCounter, 1)
			rng := rand.New(rand.NewSource(int64(opID)))

			// Deterministic operation selection based on opID
			// Distribution: 40% read, 25% cache lifecycle, 20% batch ops, 15% navigation
			operation := opID % 20

			switch {
			// 40% - Read operations (0-7): Simulate users reading files/versions
			case operation < 8:
				projectIdx := int(opID % uint64(projectCount))
				fileIdx := int((opID / uint64(projectCount)) % uint64(filesPerProject))
				versionIdx := int((opID / uint64(projectCount*filesPerProject)) % uint64(versionsPerFile))

				targetKey := projects[projectIdx].versions[fileIdx][versionIdx]

				// Read and verify content
				readData, err := kv.ReadData(targetKey)
				totalReads.Add(1)
				if err != nil {
					b.Errorf("op %d: read failed: %v", opID, err)
					continue
				}

				// Verify deterministic content pattern
				expectedPrefix := uint64(projectIdx*10000 + fileIdx*100 + versionIdx)
				actualPrefix := binary.BigEndian.Uint64(readData.Content)
				if actualPrefix != expectedPrefix {
					b.Errorf("op %d: content corruption detected", opID)
				}

				// Verify integrity
				if err := kv.Validate(targetKey); err != nil {
					b.Errorf("op %d: validation failed: %v", opID, err)
				}
				validations.Add(1)

				// Check metadata
				info, err := kv.GetDataInfo(targetKey)
				if err != nil {
					b.Errorf("op %d: GetDataInfo failed: %v", opID, err)
				} else if info.ClearTextSize == 0 {
					b.Errorf("op %d: invalid metadata", opID)
				}

			// 25% - Cache lifecycle (8-12): Simulate cache with TTL-like behavior
			case operation >= 8 && operation < 13:
				bucketIdx := int(opID % uint64(cacheBucketCount))
				cacheKeys := make([]hash.Hash, 0, cacheItemsPerOp)

				// Write cache entries
				for i := 0; i < cacheItemsPerOp; i++ {
					cacheContent := make([]byte, 512)
					cacheID := opID*1000 + uint64(i)
					binary.BigEndian.PutUint64(cacheContent, cacheID)
					rng.Read(cacheContent[8:])

					cacheData := ouroboroskv.Data{
						Content:        cacheContent,
						ContentType:    "application/cache",
						Meta:           []byte{byte(bucketIdx), byte(i)},
						RSDataSlices:   2,
						RSParitySlices: 1,
					}

					cacheKey, err := kv.WriteData(cacheData)
					totalWrites.Add(1)
					if err != nil {
						b.Errorf("op %d: cache write failed: %v", opID, err)
						continue
					}
					tracker.add(cacheKey)
					cacheKeys = append(cacheKeys, cacheKey)
				}

				// Immediately read back (cache hit simulation)
				for _, cacheKey := range cacheKeys {
					readData, err := kv.ReadData(cacheKey)
					totalReads.Add(1)
					cacheHits.Add(1)
					if err != nil {
						b.Errorf("op %d: cache read failed: %v", opID, err)
						continue
					}
					if len(readData.Content) != 512 {
						b.Errorf("op %d: cache content size mismatch", opID)
					}
				}

				// Verify cache entries exist
				for _, cacheKey := range cacheKeys {
					exists, err := kv.DataExists(cacheKey)
					if err != nil {
						b.Errorf("op %d: DataExists failed: %v", opID, err)
					} else if !exists {
						b.Errorf("op %d: cache entry missing", opID)
					}
				}

				// Evict cache (TTL expiration simulation)
				for _, cacheKey := range cacheKeys {
					if err := kv.DeleteData(cacheKey); err != nil {
						b.Errorf("op %d: cache eviction failed: %v", opID, err)
						continue
					}
					totalDeletes.Add(1)
					tracker.remove(cacheKey)
				}

				// Verify eviction
				for _, cacheKey := range cacheKeys {
					exists, err := kv.DataExists(cacheKey)
					if err != nil {
						b.Errorf("op %d: post-eviction check failed: %v", opID, err)
					} else if exists {
						b.Errorf("op %d: cache entry still exists after eviction", opID)
					}
				}

			// 20% - Batch operations (13-16): Simulate bulk uploads/downloads
			case operation >= 13 && operation < 17:
				batchSize := 6
				projectIdx := int(opID % uint64(projectCount))
				parentKey := projects[projectIdx].rootKey

				batch := make([]ouroboroskv.Data, batchSize)
				expectedContents := make([][]byte, batchSize)

				for i := 0; i < batchSize; i++ {
					content := make([]byte, 768)
					batchID := opID*100 + uint64(i)
					binary.BigEndian.PutUint64(content, batchID)
					for j := 8; j < len(content); j++ {
						content[j] = byte((int(batchID) + j) % 251)
					}
					expectedContents[i] = copyBytes(content)

					batch[i] = ouroboroskv.Data{
						Content:        content,
						Parent:         parentKey,
						ContentType:    "application/batch-upload",
						Meta:           []byte{byte(i)},
						RSDataSlices:   3,
						RSParitySlices: 2,
					}
				}

				// Batch write with retry on conflict (realistic behavior)
				var keys []hash.Hash
				maxRetries := 3
				for retry := 0; retry < maxRetries; retry++ {
					keys, err = kv.BatchWriteData(batch)
					if err == nil {
						break
					}
					if retry == maxRetries-1 {
						b.Errorf("op %d: batch write failed after %d retries: %v", opID, maxRetries, err)
						continue
					}
				}
				if len(keys) == 0 {
					continue
				}
				batchWrites.Add(1)
				totalWrites.Add(uint64(batchSize))
				tracker.addMany(keys)

				// Batch read back
				readBatch, err := kv.BatchReadData(keys)
				batchReads.Add(1)
				totalReads.Add(uint64(batchSize))
				if err != nil {
					b.Errorf("op %d: batch read failed: %v", opID, err)
				} else {
					for i := range readBatch {
						if !bytes.Equal(readBatch[i].Content, expectedContents[i]) {
							b.Errorf("op %d: batch content mismatch at %d", opID, i)
						}
						if err := kv.Validate(keys[i]); err != nil {
							b.Errorf("op %d: batch validation failed at %d: %v", opID, i, err)
						}
						validations.Add(1)
					}
				}

				// Cleanup batch items
				for _, key := range keys {
					if err := kv.DeleteData(key); err != nil {
						b.Errorf("op %d: batch cleanup failed: %v", opID, err)
						continue
					}
					totalDeletes.Add(1)
					tracker.remove(key)
				}

			// 15% - Navigation operations (17-19): Simulate tree traversal
			default:
				navigations.Add(1)
				projectIdx := int(opID % uint64(projectCount))
				proj := projects[projectIdx]

				// List root keys
				roots, err := kv.ListRootKeys()
				if err != nil {
					b.Errorf("op %d: ListRootKeys failed: %v", opID, err)
				} else if !hashSliceContains(roots, proj.rootKey) {
					b.Errorf("op %d: project root missing from roots", opID)
				}

				// Navigate project structure
				children, err := kv.GetChildren(proj.rootKey)
				if err != nil {
					b.Errorf("op %d: GetChildren failed: %v", opID, err)
				} else if len(children) < filesPerProject {
					b.Errorf("op %d: project missing files", opID)
				}

				// Get descendants count
				descendants, err := kv.GetDescendants(proj.rootKey)
				if err != nil {
					b.Errorf("op %d: GetDescendants failed: %v", opID, err)
				} else {
					expectedDescendants := filesPerProject + (filesPerProject * versionsPerFile)
					if len(descendants) < expectedDescendants {
						b.Errorf("op %d: descendant count too low: got %d, want >=%d", opID, len(descendants), expectedDescendants)
					}
				}

				// Verify file parent relationships
				fileIdx := int((opID / uint64(projectCount)) % uint64(filesPerProject))
				fileKey := proj.fileKeys[fileIdx]

				parent, err := kv.GetParent(fileKey)
				if err != nil {
					b.Errorf("op %d: GetParent failed: %v", opID, err)
				} else if parent != proj.rootKey {
					b.Errorf("op %d: parent relationship broken", opID)
				}

				// Get ancestors
				ancestors, err := kv.GetAncestors(fileKey)
				if err != nil {
					b.Errorf("op %d: GetAncestors failed: %v", opID, err)
				} else if len(ancestors) == 0 || ancestors[0] != proj.rootKey {
					b.Errorf("op %d: ancestor chain incorrect", opID)
				}

				// List all keys and verify known keys are present
				allKeys, err := kv.ListKeys()
				if err != nil {
					b.Errorf("op %d: ListKeys failed: %v", opID, err)
				} else {
					knownKeys := tracker.intersection(allKeys)
					if len(knownKeys) < expectedStaticCount {
						b.Errorf("op %d: ListKeys missing known keys", opID)
					}
				}
			}
		}
	})

	b.StopTimer()

	// Phase 3: Final integrity verification
	b.Logf("Verifying %d projects with complete hierarchies", projectCount)

	for p := 0; p < projectCount; p++ {
		proj := projects[p]

		// Verify project root
		rootData, err := kv.ReadData(proj.rootKey)
		require.NoError(b, err, "final: project %d root read failed", p)
		require.Contains(b, string(rootData.Content), fmt.Sprintf("project-%d-root", p))
		require.NoError(b, kv.Validate(proj.rootKey), "final: project %d root validation failed", p)

		// Verify all files
		for f := 0; f < filesPerProject; f++ {
			fileKey := proj.fileKeys[f]
			fileData, err := kv.ReadData(fileKey)
			require.NoError(b, err, "final: project %d file %d read failed", p, f)
			require.Contains(b, string(fileData.Content), fmt.Sprintf("project-%d-file-%d", p, f))
			require.NoError(b, kv.Validate(fileKey), "final: project %d file %d validation failed", p, f)

			parent, err := kv.GetParent(fileKey)
			require.NoError(b, err, "final: project %d file %d parent check failed", p, f)
			require.Equal(b, proj.rootKey, parent, "final: project %d file %d parent mismatch", p, f)

			// Verify all versions
			for v := 0; v < versionsPerFile; v++ {
				versionKey := proj.versions[f][v]
				versionData, err := kv.ReadData(versionKey)
				require.NoError(b, err, "final: project %d file %d version %d read failed", p, f, v)

				expectedPrefix := uint64(p*10000 + f*100 + v)
				actualPrefix := binary.BigEndian.Uint64(versionData.Content)
				require.Equal(b, expectedPrefix, actualPrefix, "final: project %d file %d version %d content corrupted", p, f, v)
				require.NoError(b, kv.Validate(versionKey), "final: project %d file %d version %d validation failed", p, f, v)

				versionParent, err := kv.GetParent(versionKey)
				require.NoError(b, err, "final: project %d file %d version %d parent check failed", p, f, v)
				require.Equal(b, fileKey, versionParent, "final: project %d file %d version %d parent mismatch", p, f, v)
			}
		}

		// Verify hierarchy integrity
		children, err := kv.GetChildren(proj.rootKey)
		require.NoError(b, err, "final: project %d children check failed", p)
		require.GreaterOrEqual(b, len(children), filesPerProject, "final: project %d lost files", p)

		descendants, err := kv.GetDescendants(proj.rootKey)
		require.NoError(b, err, "final: project %d descendants check failed", p)
		expectedDescendants := filesPerProject + (filesPerProject * versionsPerFile)
		require.GreaterOrEqual(b, len(descendants), expectedDescendants, "final: project %d lost descendants", p)
	}

	// Verify root keys list
	roots, err := kv.ListRootKeys()
	require.NoError(b, err, "final: ListRootKeys failed")
	for p := 0; p < projectCount; p++ {
		require.True(b, hashSliceContains(roots, projects[p].rootKey), "final: project %d root missing", p)
	}

	// Verify live keys snapshot
	liveKeys := tracker.snapshot()
	require.GreaterOrEqual(b, len(liveKeys), expectedStaticCount, "final: live keys count too low")

	for _, key := range liveKeys {
		info, err := kv.GetDataInfo(key)
		require.NoError(b, err, "final: GetDataInfo failed for live key")
		require.Greater(b, info.ClearTextSize, uint64(0), "final: live key has zero size")
	}

	// Final validation sweep
	results, err := kv.ValidateAll()
	require.NoError(b, err, "final: ValidateAll failed")
	require.GreaterOrEqual(b, len(results), expectedStaticCount, "final: ValidateAll incomplete")

	b.Logf("Successfully verified integrity of %d total objects", len(liveKeys))

	// Performance Statistics
	reads := totalReads.Load()
	writes := totalWrites.Load()
	deletes := totalDeletes.Load()
	totalOps := reads + writes + deletes

	b.Logf("")
	b.Logf("=== Performance Statistics ===")
	b.Logf("Total Operations:     %d", totalOps)
	b.Logf("  - Reads:            %d (%.1f%%)", reads, float64(reads)/float64(totalOps)*100)
	b.Logf("  - Writes:           %d (%.1f%%)", writes, float64(writes)/float64(totalOps)*100)
	b.Logf("  - Deletes:          %d (%.1f%%)", deletes, float64(deletes)/float64(totalOps)*100)
	b.Logf("")
	b.Logf("Batch Operations:")
	b.Logf("  - Batch Writes:     %d", batchWrites.Load())
	b.Logf("  - Batch Reads:      %d", batchReads.Load())
	b.Logf("")
	b.Logf("Cache Performance:")
	b.Logf("  - Cache Hits:       %d", cacheHits.Load())
	b.Logf("")
	b.Logf("Integrity:")
	b.Logf("  - Validations:      %d", validations.Load())
	b.Logf("  - Navigations:      %d", navigations.Load())
	b.Logf("")
	b.Logf("Throughput:")
	if b.Elapsed().Seconds() > 0 {
		b.Logf("  - Ops/sec:          %.0f", float64(totalOps)/b.Elapsed().Seconds())
		b.Logf("  - Reads/sec:        %.0f", float64(reads)/b.Elapsed().Seconds())
		b.Logf("  - Writes/sec:       %.0f", float64(writes)/b.Elapsed().Seconds())
	}
	b.Logf("==============================")
}
