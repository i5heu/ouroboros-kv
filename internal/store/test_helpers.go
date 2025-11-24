package store

import (
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	"github.com/i5heu/ouroboros-kv/internal/types"
)

const testCreatedUnix = 1_700_000_000

func applyTestDefaults(data types.Data) types.Data {
	if data.Created == 0 {
		data.Created = testCreatedUnix
	}
	return data
}

func expectedKeyForData(data types.Data) hash.Hash {
	return computeDataKey(applyTestDefaults(data))
}
