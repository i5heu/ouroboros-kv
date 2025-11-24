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

func BenchmarkConcurrent(b *testing.B) {
	dir := b.TempDir()
	cryptInstance := crypt.New()
	config := &ouroboroskv.Config{
		Paths:            []string{dir},
		MinimumFreeSpace: 1,
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

	kv, err := ouroboroskv.Init(cryptInstance, config)
	require.NoError(b, err)
	b.Cleanup(func() {
		kv.Close()
	})

	const (
		rootCount        = 6
		childrenPerRoot  = 24
		staticPayloadLen = 2048
	)

	rootKeys := make([]hash.Hash, 0, rootCount)
	staticKeys := make([]hash.Hash, 0, rootCount*(childrenPerRoot+1))
	staticContents := make([][]byte, 0, cap(staticKeys))
	staticParents := make([]hash.Hash, 0, cap(staticKeys))

	recordStatic := func(key hash.Hash, content []byte, parent hash.Hash) {
		staticKeys = append(staticKeys, key)
		staticContents = append(staticContents, copyBytes(content))
		staticParents = append(staticParents, parent)
	}

	for r := 0; r < rootCount; r++ {
		rootContent := make([]byte, staticPayloadLen)
		binary.BigEndian.PutUint64(rootContent, uint64(r))
		for i := 8; i < len(rootContent); i++ {
			rootContent[i] = byte((r + i) % 251)
		}

		rootData := ouroboroskv.Data{
			Content:        rootContent,
			ContentType:    "application/ouroboros-root",
			RSDataSlices:   3,
			RSParitySlices: 2,
		}
		rootKey, err := kv.WriteData(rootData)
		require.NoError(b, err)
		rootKeys = append(rootKeys, rootKey)
		recordStatic(rootKey, rootContent, hash.Hash{})

		for c := 0; c < childrenPerRoot; c++ {
			childContent := make([]byte, staticPayloadLen/2)
			binary.BigEndian.PutUint64(childContent, uint64(r)<<32|uint64(c))
			for i := 8; i < len(childContent); i++ {
				childContent[i] = byte((r + c + i) % 247)
			}

			childData := ouroboroskv.Data{
				Content:        childContent,
				Parent:         rootKey,
				ContentType:    "application/ouroboros-child",
				Meta:           []byte{byte(r), byte(c)},
				RSDataSlices:   3,
				RSParitySlices: 2,
			}

			childKey, err := kv.WriteData(childData)
			require.NoError(b, err)
			recordStatic(childKey, childContent, rootKey)
		}
	}

	staticCount := len(staticKeys)
	require.Equal(b, len(staticParents), staticCount)

	for idx, rootKey := range rootKeys {
		children, err := kv.GetChildren(rootKey)
		require.NoError(b, err, "initial check: root %d children lookup failed", idx)
		require.GreaterOrEqual(b, len(children), childrenPerRoot, "initial check: root %d missing children", idx)
	}

	var opCounter atomic.Uint64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := opCounter.Add(1)
			rng := rand.New(rand.NewSource(int64(id)))

			switch id % 6 {
			case 0:
				parent := rootKeys[int(id)%len(rootKeys)]
				size := 1024 + rng.Intn(4096)
				content := make([]byte, size)
				rng.Read(content)

				data := ouroboroskv.Data{
					Content:        content,
					Parent:         parent,
					ContentType:    "application/ephemeral",
					RSDataSlices:   3,
					RSParitySlices: 2,
				}
				key, err := kv.WriteData(data)
				if err != nil {
					b.Errorf("op %d: write failed: %v", id, err)
					continue
				}

				readData, err := kv.ReadData(key)
				if err != nil {
					b.Errorf("op %d: read failed: %v", id, err)
				} else if !bytes.Equal(readData.Content, content) {
					b.Errorf("op %d: content mismatch", id)
				}

				if err := kv.Validate(key); err != nil {
					b.Errorf("op %d: validate failed: %v", id, err)
				}

				if err := kv.DeleteData(key); err != nil {
					b.Errorf("op %d: delete failed: %v", id, err)
					continue
				}

				if exists, err := kv.DataExists(key); err != nil {
					b.Errorf("op %d: DataExists failed: %v", id, err)
				} else if exists {
					b.Errorf("op %d: key still exists after delete", id)
				}
			case 1:
				batchSize := 3 + int(id%4)
				batch := make([]ouroboroskv.Data, batchSize)
				expected := make([][]byte, batchSize)

				for i := 0; i < batchSize; i++ {
					size := 768 + rng.Intn(1536)
					content := make([]byte, size)
					rng.Read(content)
					expected[i] = copyBytes(content)

					parent := hash.Hash{}
					if i%2 == 0 {
						parent = rootKeys[(int(id)+i)%len(rootKeys)]
					}

					batch[i] = ouroboroskv.Data{
						Content:        content,
						Parent:         parent,
						ContentType:    "application/batch",
						RSDataSlices:   2 + uint8(i%2),
						RSParitySlices: 1 + uint8((i+1)%2),
					}
				}

				keys, err := kv.BatchWriteData(batch)
				if err != nil {
					b.Errorf("op %d: batch write failed: %v", id, err)
					continue
				}

				readBack, err := kv.BatchReadData(keys)
				if err != nil {
					b.Errorf("op %d: batch read failed: %v", id, err)
				} else {
					for i := range readBack {
						if !bytes.Equal(readBack[i].Content, expected[i]) {
							b.Errorf("op %d: batch content mismatch at %d", id, i)
						}
						if err := kv.Validate(keys[i]); err != nil {
							b.Errorf("op %d: validate batch entry %d failed: %v", id, i, err)
						}
					}
				}

				for _, key := range keys {
					if err := kv.DeleteData(key); err != nil {
						b.Errorf("op %d: delete batch key failed: %v", id, err)
					}
				}
			case 2:
				idx := int((id + 7) % uint64(staticCount))
				key := staticKeys[idx]
				expected := staticContents[idx]

				readData, err := kv.ReadData(key)
				if err != nil {
					b.Errorf("op %d: static read failed: %v", id, err)
					continue
				}
				if !bytes.Equal(readData.Content, expected) {
					b.Errorf("op %d: static content mismatch idx %d", id, idx)
				}

				if parent, err := kv.GetParent(key); err != nil {
					b.Errorf("op %d: get parent failed: %v", id, err)
				} else if parent != staticParents[idx] {
					b.Errorf("op %d: parent mismatch for static idx %d", id, idx)
				}

				if err := kv.Validate(key); err != nil {
					b.Errorf("op %d: validate static key failed: %v", id, err)
				}

				batchKeys := make([]hash.Hash, 0, 6)
				for i := 0; i < 6; i++ {
					batchKeys = append(batchKeys, staticKeys[(idx+i)%staticCount])
				}

				if batchData, err := kv.BatchReadData(batchKeys); err != nil {
					b.Errorf("op %d: batch read static failed: %v", id, err)
				} else {
					for i := range batchData {
						exp := staticContents[(idx+i)%staticCount]
						if !bytes.Equal(batchData[i].Content, exp) {
							b.Errorf("op %d: batch static mismatch offset %d", id, i)
						}
					}
				}
			case 3:
				parentIdx := int(id % uint64(staticCount))
				parentKey := staticKeys[parentIdx]
				childContent := make([]byte, 1536)
				rng.Read(childContent)

				child := ouroboroskv.Data{
					Content:        childContent,
					Parent:         parentKey,
					ContentType:    "application/tmp-child",
					RSDataSlices:   3,
					RSParitySlices: 2,
				}

				childKey, err := kv.WriteData(child)
				if err != nil {
					b.Errorf("op %d: relationship write failed: %v", id, err)
					continue
				}

				if parent, err := kv.GetParent(childKey); err != nil {
					b.Errorf("op %d: get parent for child failed: %v", id, err)
				} else if parent != parentKey {
					b.Errorf("op %d: child parent mismatch", id)
				}

				if children, err := kv.GetChildren(parentKey); err != nil {
					b.Errorf("op %d: get children failed: %v", id, err)
				} else if !hashSliceContains(children, childKey) {
					b.Errorf("op %d: new child missing from children list", id)
				}

				if ancestors, err := kv.GetAncestors(childKey); err != nil {
					b.Errorf("op %d: get ancestors failed: %v", id, err)
				} else if len(ancestors) == 0 {
					b.Errorf("op %d: ancestor list empty", id)
				}

				if err := kv.Validate(childKey); err != nil {
					b.Errorf("op %d: validate child failed: %v", id, err)
				}

				if err := kv.DeleteData(childKey); err != nil {
					b.Errorf("op %d: delete child failed: %v", id, err)
				}
			case 4:
				sampleRoot := rootKeys[int(id)%len(rootKeys)]

				if keys, err := kv.ListKeys(); err != nil {
					b.Errorf("op %d: ListKeys failed: %v", id, err)
				} else if len(keys) < staticCount {
					b.Errorf("op %d: ListKeys returned too few entries", id)
				}

				if roots, err := kv.ListRootKeys(); err != nil {
					b.Errorf("op %d: ListRootKeys failed: %v", id, err)
				} else if !hashSliceContains(roots, sampleRoot) {
					b.Errorf("op %d: missing root key in root list", id)
				}

				if stored, err := kv.ListStoredData(); err != nil {
					b.Errorf("op %d: ListStoredData failed: %v", id, err)
				} else if len(stored) < staticCount {
					b.Errorf("op %d: ListStoredData returned too few entries", id)
				}

				if info, err := kv.GetDataInfo(sampleRoot); err != nil {
					b.Errorf("op %d: GetDataInfo failed: %v", id, err)
				} else if info.ClearTextSize == 0 {
					b.Errorf("op %d: root info reported zero size", id)
				}

				if descendants, err := kv.GetDescendants(sampleRoot); err != nil {
					b.Errorf("op %d: GetDescendants failed: %v", id, err)
				} else if len(descendants) < childrenPerRoot {
					b.Errorf("op %d: descendant count too low", id)
				}

				if err := kv.Validate(sampleRoot); err != nil {
					b.Errorf("op %d: validate root failed: %v", id, err)
				}
			case 5:
				// Periodically stress validation across a subset
				if id%25 == 0 {
					if results, err := kv.ValidateAll(); err != nil {
						b.Errorf("op %d: ValidateAll failed: %v", id, err)
					} else if len(results) < staticCount {
						b.Errorf("op %d: ValidateAll returned too few entries", id)
					}
				}

				idx := int(id % uint64(staticCount))
				key := staticKeys[idx]

				if exists, err := kv.DataExists(key); err != nil {
					b.Errorf("op %d: DataExists failed: %v", id, err)
				} else if !exists {
					b.Errorf("op %d: expected static key to exist", id)
				}

				readData, err := kv.ReadData(key)
				if err != nil {
					b.Errorf("op %d: read for validation subset failed: %v", id, err)
				} else if !bytes.Equal(readData.Content, staticContents[idx]) {
					b.Errorf("op %d: subset content mismatch", id)
				}
			}
		}
	})

	b.StopTimer()

	for i, key := range staticKeys {
		readData, err := kv.ReadData(key)
		require.NoError(b, err, "final check: failed to read key %d", i)
		require.Equal(b, staticContents[i], readData.Content, "final check: content mismatch for key %d", i)

		parent, err := kv.GetParent(key)
		require.NoError(b, err, "final check: failed to read parent for key %d", i)
		require.Equal(b, staticParents[i], parent, "final check: parent mismatch for key %d", i)

		require.NoError(b, kv.Validate(key), "final check: validation failed for key %d", i)
	}

	for idx, rootKey := range rootKeys {
		children, err := kv.GetChildren(rootKey)
		require.NoError(b, err, "final check: get children for root %d failed", idx)
		require.GreaterOrEqual(b, len(children), childrenPerRoot, "final check: root %d lost children", idx)
	}

	roots, err := kv.ListRootKeys()
	require.NoError(b, err, "final check: ListRootKeys failed")
	for _, rootKey := range rootKeys {
		require.True(b, hashSliceContains(roots, rootKey), "final check: missing root key in roots list")
	}

	validateAllResults, err := kv.ValidateAll()
	require.NoError(b, err, "final check: ValidateAll failed")
	require.GreaterOrEqual(b, len(validateAllResults), len(staticKeys), "final check: ValidateAll coverage too small")
}
