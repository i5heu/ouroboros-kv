package ouroboroskv

import "github.com/i5heu/ouroboros-crypt/hash"

const testCreatedUnix = 1_700_000_000

func applyTestDefaults(data Data) Data {
	if data.Created == 0 {
		data.Created = testCreatedUnix
	}
	return data
}

func expectedKeyForData(data Data) hash.Hash {
	return computeDataKey(applyTestDefaults(data))
}
