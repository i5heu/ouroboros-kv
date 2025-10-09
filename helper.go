package ouroboroskv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/i5heu/ouroboros-crypt/hash"
)

func (k *KV) StartTransactionCounter(paths []string, minimumFreeSpace int) {

	// Start the ticker to log operations per second
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			readOps := atomic.SwapUint64(&k.readCounter, 0)
			writeOps := atomic.SwapUint64(&k.writeCounter, 0)
			k.config.Logger.Info("Chunk operations per second", "read_ops", readOps, "write_ops", writeOps)
		}
	}()
}

func serializeHashesToBytes(hashes []hash.Hash) []byte {
	if len(hashes) == 0 {
		return nil
	}

	buf := make([]byte, len(hashes)*len(hash.Hash{}))
	for i, h := range hashes {
		copy(buf[i*len(hash.Hash{}):(i+1)*len(hash.Hash{})], h[:])
	}
	return buf
}

func deserializeHashesFromBytes(data []byte) ([]hash.Hash, error) {
	if len(data) == 0 {
		return nil, nil
	}

	chunkSize := len(hash.Hash{})
	if len(data)%chunkSize != 0 {
		return nil, fmt.Errorf("invalid metadata shard hash payload length: %d", len(data))
	}

	count := len(data) / chunkSize
	hashes := make([]hash.Hash, 0, count)
	for i := 0; i < count; i++ {
		var h hash.Hash
		copy(h[:], data[i*chunkSize:(i+1)*chunkSize])
		hashes = append(hashes, h)
	}

	return hashes, nil
}

func canonicalizeAliases(aliases []hash.Hash) []hash.Hash {
	if len(aliases) == 0 {
		return nil
	}

	seen := make(map[string]hash.Hash, len(aliases))
	for _, alias := range aliases {
		key := string(alias[:])
		if _, exists := seen[key]; !exists {
			seen[key] = alias
		}
	}

	result := make([]hash.Hash, 0, len(seen))
	for _, alias := range seen {
		result = append(result, alias)
	}

	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i][:], result[j][:]) < 0
	})

	return result
}

func dataKeyMaterial(data Data, aliases []hash.Hash) []byte {
	var buf bytes.Buffer

	writeBytesWithLength(&buf, data.MetaData)
	writeBytesWithLength(&buf, data.Content)
	buf.Write(data.Parent[:])
	_ = binary.Write(&buf, binary.BigEndian, data.CreationUnixTime)
	buf.WriteByte(data.ReedSolomonShards)
	buf.WriteByte(data.ReedSolomonParityShards)

	aliasCount := uint32(len(aliases))
	_ = binary.Write(&buf, binary.BigEndian, aliasCount)
	for _, alias := range aliases {
		buf.Write(alias[:])
	}

	return buf.Bytes()
}

func writeBytesWithLength(buf *bytes.Buffer, payload []byte) {
	length := uint64(len(payload))
	_ = binary.Write(buf, binary.BigEndian, length)
	buf.Write(payload)
}

func generateDataKey(data Data) (hash.Hash, []hash.Hash) {
	aliases := canonicalizeAliases(data.Alias)
	material := dataKeyMaterial(data, aliases)
	return hash.HashBytes(material), aliases
}
