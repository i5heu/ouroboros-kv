package ouroboroskv

import (
	"testing"

	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodePipelineFailureModes(t *testing.T) {
	kv, _, cleanup := setupKVForIntegration(t)
	defer cleanup()

	data := applyTestDefaults(Data{
		MetaData:                []byte("failure-metadata"),
		Content:                 []byte("failure-content-payload"),
		ReedSolomonShards:       4,
		ReedSolomonParityShards: 2,
	})

	encoded, err := kv.encodeDataPipeline(data)
	require.NoError(t, err)
	require.NotEmpty(t, encoded.Shards)
	require.NotEmpty(t, encoded.ChunkHashes)

	// Appending a phantom hash without shards should cause the decoder to fail when reconstructing.
	missingShardLinked := encoded
	missingShardLinked.ChunkHashes = append([]hash.Hash(nil), encoded.ChunkHashes...)
	missingShardLinked.ChunkHashes = append(missingShardLinked.ChunkHashes, hash.HashBytes([]byte("phantom")))

	_, err = kv.decodeDataPipeline(missingShardLinked)
	assert.Error(t, err)

	// Swapping the expected hash order should yield a hash mismatch.
	swapped := encoded
	swapped.ChunkHashes = append([]hash.Hash(nil), encoded.ChunkHashes...)
	if len(swapped.ChunkHashes) > 1 {
		swapped.ChunkHashes[0], swapped.ChunkHashes[1] = swapped.ChunkHashes[1], swapped.ChunkHashes[0]
		_, err = kv.decodeDataPipeline(swapped)
		assert.Error(t, err)
	}

	// Corrupting the reed solomon index should be reported as invalid.
	corruptIndex := encoded
	corruptIndex.Shards = append([]kvDataShard(nil), encoded.Shards...)
	corruptIndex.Shards[0].ReedSolomonIndex = uint8(len(encoded.Shards) + 5)
	_, err = kv.reedSolomonReconstructor(corruptIndex.Shards[:1])
	assert.Error(t, err)
}

func TestBatchWriteDataRejectsInvalidPayload(t *testing.T) {
	kv, _, cleanup := setupKVForIntegration(t)
	defer cleanup()

	_, err := kv.BatchWriteData([]Data{applyTestDefaults(Data{
		Content:                 []byte("invalid"),
		ReedSolomonShards:       0,
		ReedSolomonParityShards: 0,
	})})
	assert.Error(t, err)
}
