package ouroboroskv

import (
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"google.golang.org/protobuf/proto"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	pb "github.com/i5heu/ouroboros-kv/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupKVForIntegration(t *testing.T) (*KV, string, func()) {
	t.Helper()

	tempDir, err := os.MkdirTemp("", "ouroboros-kv-integration-")
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	config := &Config{
		Paths:            []string{tempDir},
		MinimumFreeSpace: 0,
		Logger:           logger,
	}

	kv, err := Init(crypt.New(), config)
	require.NoError(t, err)

	cleanup := func() {
		err := kv.Close()
		assert.NoError(t, err)
		os.RemoveAll(tempDir)
	}

	return kv, tempDir, cleanup
}

// TestEndToEndWorkflowCoverage performs a comprehensive integration scenario that exercises
// the full read/write pipelines, metadata inspection helpers, validation helpers and
// relationship traversal utilities.
func TestEndToEndWorkflowCoverage(t *testing.T) {
	kv, basePath, cleanup := setupKVForIntegration(t)
	defer cleanup()

	emptyBatchKeys, err := kv.BatchWriteData(nil)
	require.NoError(t, err)
	require.Empty(t, emptyBatchKeys)

	// Create a reusable alias catalogue to exercise alias serialization in the key computation.
	aliasOne := hash.HashBytes([]byte("alias-one"))
	aliasTwo := hash.HashBytes([]byte("alias-two"))

	// Seed the store with a child entry so the parent written afterwards can reference it.
	childData := applyTestDefaults(Data{
		MetaData:                []byte("child-meta-data"),
		Content:                 []byte(strings.Repeat("child-content-block-", 16)),
		ReedSolomonShards:       4,
		ReedSolomonParityShards: 2,
		Aliases:                 []hash.Hash{aliasOne},
	})

	childKey, err := kv.WriteData(childData)
	require.NoError(t, err)
	require.False(t, isEmptyHash(childKey))

	exists, err := kv.DataExists(childKey)
	require.NoError(t, err)
	require.True(t, exists)

	missingKey := hash.HashBytes([]byte("missing-key"))
	exists, err = kv.DataExists(missingKey)
	require.NoError(t, err)
	require.False(t, exists)

	// Parent entry references the child and uses multiple aliases to exercise canonicalDataKey coverage.
	parentData := applyTestDefaults(Data{
		MetaData:                []byte(strings.Repeat("parent-metadata-", 8)),
		Content:                 []byte(strings.Repeat("parent-content-segment-", 24)),
		Children:                []hash.Hash{childKey},
		ReedSolomonShards:       4,
		ReedSolomonParityShards: 2,
		Aliases:                 []hash.Hash{aliasOne, aliasTwo},
	})

	parentKey, err := kv.WriteData(parentData)
	require.NoError(t, err)
	require.False(t, parentKey == childKey)

	parentReadBack, err := kv.ReadData(parentKey)
	require.NoError(t, err)
	assert.Equal(t, parentKey, parentReadBack.Key)
	assert.Equal(t, parentData.MetaData, parentReadBack.MetaData)
	assert.Equal(t, parentData.Content, parentReadBack.Content)
	assert.ElementsMatch(t, parentData.Aliases, parentReadBack.Aliases)
	assert.Contains(t, parentReadBack.Children, childKey)

	// Relationship helpers should describe the structure encoded above.
	children, err := kv.GetChildren(parentKey)
	require.NoError(t, err)
	assert.Contains(t, children, childKey)

	parentForChild, err := kv.GetParent(childKey)
	require.NoError(t, err)
	assert.Equal(t, parentKey, parentForChild)

	descendants, err := kv.GetDescendants(parentKey)
	require.NoError(t, err)
	assert.Contains(t, descendants, childKey)

	ancestors, err := kv.GetAncestors(childKey)
	require.NoError(t, err)
	assert.Contains(t, ancestors, parentKey)

	// Batch read should gracefully handle empty inputs and the populated keys.
	batchRead, err := kv.BatchReadData(nil)
	require.NoError(t, err)
	require.Nil(t, batchRead)

	batched, err := kv.BatchReadData([]hash.Hash{parentKey, childKey})
	require.NoError(t, err)
	require.Len(t, batched, 2)

	// Additional data is written via the batch API to exercise its encoding and relationship logic.
	grandChildData := applyTestDefaults(Data{
		MetaData:                []byte("grandchild-metadata"),
		Content:                 []byte(strings.Repeat("grandchild-content-", 20)),
		Parent:                  parentKey,
		ReedSolomonShards:       4,
		ReedSolomonParityShards: 2,
	})

	siblingData := applyTestDefaults(Data{
		MetaData:                []byte("sibling-metadata"),
		Content:                 []byte(strings.Repeat("sibling-content-", 18)),
		ReedSolomonShards:       4,
		ReedSolomonParityShards: 2,
	})

	batchKeys, err := kv.BatchWriteData([]Data{grandChildData, siblingData})
	require.NoError(t, err)
	require.Len(t, batchKeys, 2)

	grandChildKey := batchKeys[0]
	siblingKey := batchKeys[1]
	require.False(t, isEmptyHash(grandChildKey))
	require.False(t, isEmptyHash(siblingKey))

	bridgeData := applyTestDefaults(Data{
		MetaData:                []byte("bridge-metadata"),
		Content:                 []byte(strings.Repeat("bridge-content-", 12)),
		Parent:                  parentKey,
		Children:                []hash.Hash{childKey, grandChildKey},
		ReedSolomonShards:       4,
		ReedSolomonParityShards: 2,
		Aliases:                 []hash.Hash{hash.HashBytes([]byte("bridge-alias"))},
	})

	expectedBridgeKey := expectedKeyForData(bridgeData)
	bridgeKey, err := kv.WriteData(bridgeData)
	require.NoError(t, err)
	assert.Equal(t, expectedBridgeKey, bridgeKey)

	// After adding the grandchild the parent should now have two descendants.
	children, err = kv.GetChildren(parentKey)
	require.NoError(t, err)
	assert.Contains(t, children, childKey)
	assert.Contains(t, children, grandChildKey)
	assert.Contains(t, children, bridgeKey)

	descendants, err = kv.GetDescendants(parentKey)
	require.NoError(t, err)
	assert.Contains(t, descendants, childKey)
	assert.Contains(t, descendants, grandChildKey)
	assert.Contains(t, descendants, bridgeKey)

	grandAncestors, err := kv.GetAncestors(grandChildKey)
	require.NoError(t, err)
	assert.Contains(t, grandAncestors, parentKey)

	// Global listing helpers should expose every key exactly once.
	allKeys, err := kv.ListKeys()
	require.NoError(t, err)
	assert.Len(t, allKeys, 5)

	sort.Slice(allKeys, func(i, j int) bool { return allKeys[i].String() < allKeys[j].String() })
	expectedKeys := []hash.Hash{childKey, parentKey, grandChildKey, siblingKey, bridgeKey}
	sort.Slice(expectedKeys, func(i, j int) bool { return expectedKeys[i].String() < expectedKeys[j].String() })
	assert.Equal(t, expectedKeys, allKeys)

	roots, err := kv.GetRoots()
	require.NoError(t, err)
	assert.Contains(t, roots, parentKey)
	assert.Contains(t, roots, childKey)
	assert.Contains(t, roots, siblingKey)
	assert.NotContains(t, roots, grandChildKey)

	rootKeys, err := kv.ListRootKeys()
	require.NoError(t, err)
	assert.Contains(t, rootKeys, parentKey)

	dataInfos, err := kv.ListStoredData()
	require.NoError(t, err)
	require.Len(t, dataInfos, 5)

	infoByKey := make(map[hash.Hash]DataInfo)
	for _, info := range dataInfos {
		infoByKey[info.Key] = info
		assert.NotZero(t, info.StorageSize)
		assert.NotZero(t, info.NumShards)
		assert.NotEmpty(t, info.MetaData)
	}

	parentInfoFull, err := kv.GetDataInfo(parentKey)
	require.NoError(t, err)
	assert.Equal(t, parentKey, parentInfoFull.Key)
	assert.Equal(t, parentData.MetaData, parentInfoFull.MetaData)
	assert.Equal(t, parentData.ReedSolomonShards, parentInfoFull.ReedSolomonShards)
	assert.Equal(t, parentData.ReedSolomonParityShards, parentInfoFull.ReedSolomonParityShards)

	parentInfo, ok := infoByKey[parentKey]
	require.True(t, ok)
	assert.Equal(t, parentData.MetaData, parentInfo.MetaData)
	assert.Equal(t, len(parentInfo.ChunkHashes), parentInfo.NumChunks)

	childInfo, ok := infoByKey[childKey]
	require.True(t, ok)
	assert.Equal(t, childData.MetaData, childInfo.MetaData)

	formattedParent := parentInfo.FormatDataInfo()
	assert.Contains(t, formattedParent, parentInfo.KeyBase64)
	assert.Contains(t, formattedParent, "Data Key")

	assert.Equal(t, "5 B", formatBytes(5))
	assert.Equal(t, "1.0 KB", formatBytes(1024))
	assert.Equal(t, "1.0 MB", formatBytes(1024*1024))

	require.NoError(t, displayDiskUsage([]string{basePath}))
	err = displayDiskUsage([]string{filepath.Join(basePath, "missing-subdir")})
	assert.Error(t, err)
	mountPoint, device, err := getDeviceAndMountPoint(basePath)
	require.NoError(t, err)
	assert.NotEmpty(t, mountPoint)
	assert.NotEmpty(t, device)

	// Validation utilities should confirm the database is consistent.
	require.NoError(t, kv.ValidateKey(parentKey))
	err = kv.ValidateKey(missingKey)
	assert.Error(t, err)

	validationResults, err := kv.ValidateAll()
	require.NoError(t, err)
	require.Len(t, validationResults, 5)
	for _, res := range validationResults {
		assert.True(t, res.Passed(), "expected validation to pass for %x", res.Key)
		assert.NotEmpty(t, res.KeyBase64)
	}

	// Delete a key and confirm it disappears from the index surfaces.
        require.NoError(t, kv.DeleteData(siblingKey))

        exists, err = kv.DataExists(siblingKey)
        require.NoError(t, err)
        assert.False(t, exists)

        err = kv.DeleteData(siblingKey)
        assert.Error(t, err)

	allKeys, err = kv.ListKeys()
	require.NoError(t, err)
	assert.Len(t, allKeys, 4)

	// Exercise additional encoding and decoding helpers directly to capture edge cases in a high-level manner.
	encodedParent, err := kv.encodeDataPipeline(parentData)
	require.NoError(t, err)
	decodedParent, err := kv.decodeDataPipeline(encodedParent)
	require.NoError(t, err)
	assert.Equal(t, parentData.Content, decodedParent.Content)
	assert.Equal(t, parentData.MetaData, decodedParent.MetaData)

	_, err = kv.encodeDataPipeline(Data{Content: []byte("error-case"), ReedSolomonShards: 0, ReedSolomonParityShards: 0})
	assert.Error(t, err)

	chunkGroups := append([]kvDataShard(nil), encodedParent.Shards...)
	if len(chunkGroups) > 0 {
		reconstructed, err := kv.reedSolomonReconstructor(chunkGroups[:int(parentData.ReedSolomonShards)])
		require.NoError(t, err)
		_, err = kv.crypt.Decrypt(reconstructed)
		require.NoError(t, err)
	}

	emptyPayload, shardCount, parityCount, err := kv.reconstructPayload(nil, nil)
	require.NoError(t, err)
	assert.Empty(t, emptyPayload)
	assert.Zero(t, shardCount)
	assert.Zero(t, parityCount)

	validationResults, err = kv.ValidateAll()
	require.NoError(t, err)
	require.Len(t, validationResults, 4)

	fallbackKey := hash.HashBytes([]byte("fallback-root-key"))
	err = kv.badgerDB.Update(func(txn *badger.Txn) error {
		metadataKey := fmt.Sprintf("%s%x", METADATA_PREFIX, parentKey)
		item, err := txn.Get([]byte(metadataKey))
		if err != nil {
			return err
		}

		var payload []byte
		if err := item.Value(func(val []byte) error {
			payload = append([]byte(nil), val...)
			return nil
		}); err != nil {
			return err
		}

		protoMetadata := &pb.KvDataHashProto{}
		if err := proto.Unmarshal(payload, protoMetadata); err != nil {
			return err
		}

		protoMetadata.Key = nil
		protoMetadata.Parent = nil
		protoMetadata.Created = time.Now().Unix()

		newPayload, err := proto.Marshal(protoMetadata)
		if err != nil {
			return err
		}

		fallbackKeyBytes := METADATA_PREFIX + fallbackKey.String()
		return txn.Set([]byte(fallbackKeyBytes), newPayload)
	})
	require.NoError(t, err)

	rootKeys, err = kv.ListRootKeys()
	require.NoError(t, err)
	assert.Contains(t, rootKeys, fallbackKey)

	invalidWriteData := applyTestDefaults(Data{
		Key:                     hash.HashBytes([]byte("pre-seeded-key")),
		MetaData:                []byte("invalid"),
		Content:                 []byte("invalid"),
		ReedSolomonShards:       4,
		ReedSolomonParityShards: 2,
	})
	_, err = kv.WriteData(invalidWriteData)
	assert.Error(t, err)

	_, err = kv.BatchWriteData([]Data{invalidWriteData})
	assert.Error(t, err)

	_, err = kv.ReadData(missingKey)
	assert.Error(t, err)

	_, err = kv.BatchReadData([]hash.Hash{missingKey})
	assert.Error(t, err)

	err = kv.DeleteData(missingKey)
	assert.Error(t, err)

	emptyPipelineData := applyTestDefaults(Data{
		ReedSolomonShards:       4,
		ReedSolomonParityShards: 2,
	})
	encodedEmpty, err := kv.encodeDataPipeline(emptyPipelineData)
	require.NoError(t, err)
	assert.Empty(t, encodedEmpty.Shards)
	assert.Empty(t, encodedEmpty.MetaShards)

	decodedEmpty, err := kv.decodeDataPipeline(encodedEmpty)
	require.NoError(t, err)
	assert.Empty(t, decodedEmpty.Content)
	assert.Empty(t, decodedEmpty.MetaData)

	assert.Nil(t, serializeHashesToBytes(nil))
	hashes, err := deserializeHashesFromBytes(nil)
	require.NoError(t, err)
	assert.Nil(t, hashes)

	_, err = deserializeHashesFromBytes([]byte{1, 2, 3})
	assert.Error(t, err)

	tempDir2, err := os.MkdirTemp("", "config-validation-")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir2)

	cfg := &Config{}
	assert.Error(t, cfg.checkConfig())

	filePath := filepath.Join(tempDir2, "file.txt")
	require.NoError(t, os.WriteFile(filePath, []byte("data"), 0o600))

	cfg = &Config{Paths: []string{filePath}}
	assert.Error(t, cfg.checkConfig())

	nestedDir := filepath.Join(tempDir2, "nested", "dir")
	cfg = &Config{Paths: []string{nestedDir}}
	assert.NoError(t, cfg.checkConfig())

	cfg = &Config{Paths: []string{tempDir2}, MinimumFreeSpace: math.MaxInt32}
	assert.Error(t, cfg.checkConfig())

	_, err = Init(crypt.New(), &Config{})
	assert.Error(t, err)

	_, err = Init(crypt.New(), &Config{Paths: []string{tempDir2}, MinimumFreeSpace: math.MaxInt32})
	assert.Error(t, err)

	batchRelationshipData := applyTestDefaults(Data{
		MetaData:                []byte("batch-relationship"),
		Content:                 []byte("batch-relationship-content"),
		Children:                []hash.Hash{childKey, grandChildKey},
		ReedSolomonShards:       4,
		ReedSolomonParityShards: 2,
	})
	extraKeys, err := kv.BatchWriteData([]Data{batchRelationshipData})
	require.NoError(t, err)
	require.Len(t, extraKeys, 1)
	batchRelationshipKey := extraKeys[0]
	assert.False(t, isEmptyHash(batchRelationshipKey))
	require.NoError(t, kv.DeleteData(batchRelationshipKey))

	batchNoMetaData := applyTestDefaults(Data{
		Content:                 []byte("batch-no-metadata"),
		ReedSolomonShards:       4,
		ReedSolomonParityShards: 2,
	})
	noMetaKeys, err := kv.BatchWriteData([]Data{batchNoMetaData})
	require.NoError(t, err)
	require.Len(t, noMetaKeys, 1)
	require.NoError(t, kv.DeleteData(noMetaKeys[0]))

	_, err = kv.reedSolomonSplitter(Data{}, nil, nil)
	assert.Error(t, err)

	_, err = kv.reedSolomonReconstructor([]kvDataShard{})
	assert.Error(t, err)

	invalidHash := hash.HashBytes([]byte("invalid-chunk"))
	invalidShard := kvDataShard{
		ChunkHash:               invalidHash,
		ReedSolomonShards:       1,
		ReedSolomonParityShards: 0,
		ReedSolomonIndex:        5,
		ChunkContent:            []byte{1, 2, 3},
		OriginalSize:            3,
	}
	_, _, _, err = kv.reconstructPayload([]kvDataShard{invalidShard}, []hash.Hash{invalidHash})
	assert.Error(t, err)

	_, _, _, err = kv.reconstructPayload([]kvDataShard{{}}, nil)
	assert.Error(t, err)

	_, err = decompressWithZstd([]byte{0x01, 0x02})
	assert.Error(t, err)

	metadataFreeData := applyTestDefaults(Data{
		Content:                 []byte("metadata-free"),
		ReedSolomonShards:       4,
		ReedSolomonParityShards: 2,
	})
	metadataFreeKey, err := kv.WriteData(metadataFreeData)
	require.NoError(t, err)
	require.NoError(t, kv.DeleteData(metadataFreeKey))

	tempDirNoLogger, err := os.MkdirTemp("", "ouroboros-kv-nolog-")
	require.NoError(t, err)
	defer os.RemoveAll(tempDirNoLogger)

	cfgNoLogger := &Config{Paths: []string{tempDirNoLogger}, MinimumFreeSpace: 0}
	kvWithoutLogger, err := Init(crypt.New(), cfgNoLogger)
	require.NoError(t, err)
	assert.NotNil(t, kvWithoutLogger.config.Logger)
	require.NoError(t, kvWithoutLogger.Close())

	// Exercise additional helper utilities that are not covered by the CRUD flow above.
	assert.NoError(t, displayDiskUsage([]string{"ExamplePath"}))
	err = displayDiskUsage([]string{})
	assert.Error(t, err)

	_, _, err = getDeviceAndMountPoint("/definitely/nonexistent/path")
	assert.Error(t, err)

	assert.True(t, contains("/alpha/beta", "/alpha"))
	assert.True(t, contains("/alpha", "/"))
	assert.False(t, contains("/alpha/beta", "/gamma"))
	assert.False(t, contains("/alpha", ""))

	kv.StartTransactionCounter(nil, 0)
	time.Sleep(1100 * time.Millisecond)
}

func TestCorruptedStorageCoverage(t *testing.T) {
	kv, _, cleanup := setupKVForIntegration(t)
	defer cleanup()

	corruptedData := applyTestDefaults(Data{
		MetaData:                []byte("corrupt-meta"),
		Content:                 []byte("corrupt-content"),
		ReedSolomonShards:       3,
		ReedSolomonParityShards: 2,
	})

	key, err := kv.WriteData(corruptedData)
	require.NoError(t, err)

	var (
		shardHash hash.Hash
		metaKey   string
	)

	err = kv.badgerDB.View(func(txn *badger.Txn) error {
		metadata, err := kv.loadMetadata(txn, key)
		if err != nil {
			return err
		}
		if len(metadata.ShardHashes) == 0 {
			return fmt.Errorf("expected shard hashes for corrupted data")
		}
		shardHash = metadata.ShardHashes[0]
		metaKey = fmt.Sprintf("%s%x", METADATA_CHUNK_PREFIX, metadata.Key)
		return nil
	})
	require.NoError(t, err)

	err = kv.badgerDB.Update(func(txn *badger.Txn) error {
		totalShards := int(corruptedData.ReedSolomonShards + corruptedData.ReedSolomonParityShards)
		for i := 0; i < totalShards; i++ {
			chunkKey := fmt.Sprintf("%s%x_%d", CHUNK_PREFIX, shardHash, i)
			if err := txn.Delete([]byte(chunkKey)); err != nil {
				return err
			}
		}
		if err := txn.Set([]byte(metaKey), []byte{0x01}); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	err = kv.badgerDB.View(func(txn *badger.Txn) error {
		_, viewErr := kv.loadChunksByHash(txn, shardHash)
		if viewErr == nil {
			return fmt.Errorf("expected chunk deletion to cause lookup failure")
		}
		return nil
	})
	require.NoError(t, err)

	_, err = kv.ReadData(key)
	assert.Error(t, err)

	_, err = kv.GetDataInfo(key)
	assert.Error(t, err)

	infos, err := kv.ListStoredData()
	require.NoError(t, err)
	assert.Empty(t, infos)

	results, err := kv.ValidateAll()
	require.NoError(t, err)
	if len(results) == 1 {
		assert.False(t, results[0].Passed())
	} else {
		assert.Empty(t, results)
	}
}
