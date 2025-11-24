package ouroboroskv__test

import (
	"bytes"
	"encoding/binary"
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

// BenchmarkDeterministicParallel runs a 100% deterministic benchmark
// that writes, reads, and deletes data in parallel.
func BenchmarkDeterministicParallel(b *testing.B) {
	// 1. Setup Store
	dir := b.TempDir()
	cryptInstance := crypt.New()
	config := &ouroboroskv.Config{
		Paths:            []string{dir},
		MinimumFreeSpace: 1,
		// Suppress logs for benchmark unless failed
		Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	kv, err := ouroboroskv.Init(cryptInstance, config)
	require.NoError(b, err)
	defer kv.Close()

	tracker := newLiveKeyTracker()

	// 2. Pre-populate deterministic data set once for the entire benchmark
	const staticDataCount = 128
	const expectedRootChildren = staticDataCount / 2

	rootData := ouroboroskv.Data{
		Content:        []byte("deterministic-root-node"),
		RSDataSlices:   3,
		RSParitySlices: 2,
	}
	rootKey, err := kv.WriteData(rootData)
	require.NoError(b, err)
	tracker.add(rootKey)

	staticKeys := make([]hash.Hash, staticDataCount)
	staticValues := make([][]byte, staticDataCount)

	for i := 0; i < staticDataCount; i++ {
		content := make([]byte, 2048)
		binary.BigEndian.PutUint64(content, uint64(i))
		binary.BigEndian.PutUint64(content[len(content)-8:], uint64(staticDataCount-i))
		for j := 8; j < len(content)-8; j++ {
			content[j] = byte((i + j) % 251)
		}
		parent := hash.Hash{}
		if i%2 == 0 {
			parent = rootKey
		}

		data := ouroboroskv.Data{
			Content:        content,
			Parent:         parent,
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		key, err := kv.WriteData(data)
		require.NoError(b, err)
		staticKeys[i] = key
		staticValues[i] = copyBytes(content)
		tracker.add(key)
	}

	children, err := kv.GetChildren(rootKey)
	require.NoError(b, err)
	require.GreaterOrEqual(b, len(children), expectedRootChildren, "root lost baseline children before benchmark")

	// Global counter for deterministic operation IDs
	var opCounter uint64

	b.ResetTimer()

	// 3. Run Parallel Benchmark with multiple deterministic workflows
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&opCounter, 1)
			rng := rand.New(rand.NewSource(int64(id)))

			switch id % 4 {
			case 0:
				size := 1024 + rng.Intn(4096)
				content := make([]byte, size)
				binary.BigEndian.PutUint64(content, id)
				binary.BigEndian.PutUint64(content[size-8:], id^0xDEADBEEF)
				parent := rootKey
				if id%3 == 0 {
					parent = hash.Hash{}
				}

				data := ouroboroskv.Data{
					Content:        content,
					Parent:         parent,
					RSDataSlices:   3,
					RSParitySlices: 2,
				}

				key, err := kv.WriteData(data)
				if err != nil {
					b.Errorf("Op %d: write failed: %v", id, err)
					continue
				}

				tracker.add(key)

				readData, err := kv.ReadData(key)
				if err != nil {
					b.Errorf("Op %d: read failed: %v", id, err)
					continue
				}
				if !bytes.Equal(readData.Content, content) {
					b.Errorf("Op %d: content mismatch after write/read cycle", id)
				}

				if err := kv.Validate(key); err != nil {
					b.Errorf("Op %d: validate failed: %v", id, err)
				}

				if info, err := kv.GetDataInfo(key); err != nil {
					b.Errorf("Op %d: GetDataInfo failed: %v", id, err)
				} else if info.ClearTextSize == 0 {
					b.Errorf("Op %d: data info reports zero clear-text size", id)
				}

				if err := kv.DeleteData(key); err != nil {
					b.Errorf("Op %d: delete failed: %v", id, err)
					continue
				}

				tracker.remove(key)

				if exists, err := kv.DataExists(key); err != nil {
					b.Errorf("Op %d: DataExists failed: %v", id, err)
				} else if exists {
					b.Errorf("Op %d: key still exists after delete", id)
				}
			case 1:
				parentIdx := int(id % uint64(staticDataCount))
				parentKey := staticKeys[parentIdx]

				content := make([]byte, 1536)
				binary.BigEndian.PutUint64(content, id<<1)
				childData := ouroboroskv.Data{
					Content:        content,
					Parent:         parentKey,
					RSDataSlices:   3,
					RSParitySlices: 2,
				}

				childKey, err := kv.WriteData(childData)
				if err != nil {
					b.Errorf("Op %d: child write failed: %v", id, err)
					continue
				}

				tracker.add(childKey)

				if parent, err := kv.GetParent(childKey); err != nil {
					b.Errorf("Op %d: get parent failed: %v", id, err)
				} else if parent != parentKey {
					b.Errorf("Op %d: child stored wrong parent", id)
				}

				if ancestors, err := kv.GetAncestors(childKey); err != nil {
					b.Errorf("Op %d: get ancestors failed: %v", id, err)
				} else if len(ancestors) == 0 || ancestors[0] != parentKey {
					b.Errorf("Op %d: ancestor chain incorrect", id)
				}

				if children, err := kv.GetChildren(parentKey); err != nil {
					b.Errorf("Op %d: get children failed: %v", id, err)
				} else if !hashSliceContains(children, childKey) {
					b.Errorf("Op %d: parent missing newly created child", id)
				}

				if descendants, err := kv.GetDescendants(parentKey); err != nil {
					b.Errorf("Op %d: get descendants failed: %v", id, err)
				} else if len(descendants) == 0 {
					b.Errorf("Op %d: no descendants returned for parent", id)
				}

				if err := kv.Validate(childKey); err != nil {
					b.Errorf("Op %d: validate child failed: %v", id, err)
				}

				if err := kv.DeleteData(childKey); err != nil {
					b.Errorf("Op %d: delete child failed: %v", id, err)
				}
				tracker.remove(childKey)
			case 2:
				const batchSize = 4
				batch := make([]ouroboroskv.Data, batchSize)
				batchContents := make([][]byte, batchSize)

				for i := 0; i < batchSize; i++ {
					size := 512 + rng.Intn(2048)
					content := make([]byte, size)
					rng.Read(content)
					batchContents[i] = copyBytes(content)

					parent := hash.Hash{}
					if i%2 == 0 {
						parent = staticKeys[(int(id)+i)%staticDataCount]
					}

					batch[i] = ouroboroskv.Data{
						Content:        content,
						Parent:         parent,
						RSDataSlices:   2,
						RSParitySlices: 1,
					}
				}

				keys, err := kv.BatchWriteData(batch)
				if err != nil {
					b.Errorf("Op %d: batch write failed: %v", id, err)
					continue
				}

				tracker.addMany(keys)

				readBack, err := kv.BatchReadData(keys)
				if err != nil {
					b.Errorf("Op %d: batch read failed: %v", id, err)
				} else {
					for i := range readBack {
						if !bytes.Equal(readBack[i].Content, batchContents[i]) {
							b.Errorf("Op %d: batch content mismatch at idx %d", id, i)
						}
						if err := kv.Validate(keys[i]); err != nil {
							b.Errorf("Op %d: validate batch entry %d failed: %v", id, i, err)
						}
					}
				}

				for _, key := range keys {
					if err := kv.DeleteData(key); err != nil {
						b.Errorf("Op %d: delete batch key failed: %v", id, err)
						continue
					}
					tracker.remove(key)
				}
			default:
				idx := int(id % uint64(staticDataCount))
				key := staticKeys[idx]
				expectedContent := staticValues[idx]

				readData, err := kv.ReadData(key)
				if err != nil {
					b.Errorf("Op %d: static read failed: %v", id, err)
					continue
				}
				if !bytes.Equal(readData.Content, expectedContent) {
					b.Errorf("Op %d: static content mismatch for key %d", id, idx)
				}

				if exists, err := kv.DataExists(key); err != nil {
					b.Errorf("Op %d: DataExists failed: %v", id, err)
				} else if !exists {
					b.Errorf("Op %d: expected static key %d to exist", id, idx)
				}

				if err := kv.Validate(key); err != nil {
					b.Errorf("Op %d: validate static key failed: %v", id, err)
				}

				if info, err := kv.GetDataInfo(key); err != nil {
					b.Errorf("Op %d: data info lookup failed: %v", id, err)
				} else if info.ClearTextSize == 0 {
					b.Errorf("Op %d: data info reports zero size", id)
				}

				batchKeys := make([]hash.Hash, 0, 5)
				for i := 0; i < 5; i++ {
					batchKeys = append(batchKeys, staticKeys[(idx+i)%staticDataCount])
				}

				if batchData, err := kv.BatchReadData(batchKeys); err != nil {
					b.Errorf("Op %d: batch read static failed: %v", id, err)
				} else {
					for i := range batchData {
						exp := staticValues[(idx+i)%staticDataCount]
						if !bytes.Equal(batchData[i].Content, exp) {
							b.Errorf("Op %d: batch static mismatch at offset %d", id, i)
						}
					}
				}

				if keys, err := kv.ListKeys(); err != nil {
					b.Errorf("Op %d: ListKeys failed: %v", id, err)
				} else {
					known := tracker.intersection(keys)
					if len(known) < len(staticKeys) {
						b.Errorf("Op %d: ListKeys returned too few known entries", id)
					}
					if !hashSliceContains(keys, key) {
						b.Errorf("Op %d: ListKeys missing expected key", id)
					}
				}

				if roots, err := kv.ListRootKeys(); err != nil {
					b.Errorf("Op %d: ListRootKeys failed: %v", id, err)
				} else if !hashSliceContains(roots, rootKey) {
					b.Errorf("Op %d: root key missing from root list", id)
				}

				if descendants, err := kv.GetDescendants(rootKey); err != nil {
					b.Errorf("Op %d: GetDescendants failed: %v", id, err)
				} else if len(descendants) < expectedRootChildren {
					b.Errorf("Op %d: descendant count too low", id)
				}
			}
		}
	})

	b.StopTimer()

	// 4. Final Integrity Check across the static dataset
	for i, key := range staticKeys {
		readData, err := kv.ReadData(key)
		require.NoError(b, err, "Final check: failed to read static key %d", i)
		require.Equal(b, staticValues[i], readData.Content, "Final check: content mismatch for static key %d", i)
		if i%2 == 0 {
			parent, err := kv.GetParent(key)
			require.NoError(b, err, "Final check: failed to read parent for static key %d", i)
			require.Equal(b, rootKey, parent, "Final check: static key %d lost its parent", i)
		} else {
			parent, err := kv.GetParent(key)
			require.NoError(b, err, "Final check: failed to read parent for standalone key %d", i)
			require.Equal(b, hash.Hash{}, parent, "Final check: standalone key %d unexpectedly gained a parent", i)
		}
		require.NoError(b, kv.Validate(key), "Final check: validation failed for static key %d", i)
	}

	descendants, err := kv.GetDescendants(rootKey)
	require.NoError(b, err)
	require.GreaterOrEqual(b, len(descendants), expectedRootChildren, "Root lost descendants during benchmark")

	roots, err := kv.ListRootKeys()
	require.NoError(b, err)
	require.True(b, hashSliceContains(roots, rootKey), "Root missing from root listing")

	liveKeys := tracker.snapshot()
	require.GreaterOrEqual(b, len(liveKeys), len(staticKeys)+1, "Final check: live key snapshot missing entries")
	for _, key := range liveKeys {
		info, err := kv.GetDataInfo(key)
		require.NoError(b, err, "Final check: GetDataInfo failed for live key")
		require.Greater(b, info.ClearTextSize, uint64(0), "Final check: live key reports zero size")
	}

	results, err := kv.ValidateAll()
	require.NoError(b, err)
	require.GreaterOrEqual(b, len(results), len(staticKeys)+1, "Unexpected validation coverage")
}
