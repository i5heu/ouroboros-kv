package ouroboroskv

import (
	"math/rand"
	"testing"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func BenchmarkWriteReadNavigation(b *testing.B) {
	const (
		keyCount  = 100
		valueSize = 4096
	)

	// Generate random data and keys
	values := make([][]byte, keyCount)
	dataObjs := make([]Data, keyCount)
	for i := 0; i < keyCount; i++ {
		values[i] = make([]byte, valueSize)
		rand.Read(values[i])
		key := hash.HashBytes(values[i])
		dataObjs[i] = Data{
			Key:                     key,
			Content:                 values[i],
			Parent:                  hash.Hash{},
			Children:                []hash.Hash{},
			ReedSolomonShards:       2,
			ReedSolomonParityShards: 1,
		}
	}

	b.Run("WriteData", func(b *testing.B) {
		dir := b.TempDir()
		cryptInstance := crypt.New()
		config := &Config{
			Paths:            []string{dir},
			MinimumFreeSpace: 1,
			Logger:           logrus.New(),
		}
		config.Logger.SetLevel(logrus.ErrorLevel)
		kv, err := Init(cryptInstance, config)
		require.NoError(b, err)
		defer kv.Close()

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			i := n % keyCount
			err := kv.WriteData(dataObjs[i])
			require.NoError(b, err)
		}
	})

	b.Run("ReadData", func(b *testing.B) {
		dir := b.TempDir()
		cryptInstance := crypt.New()
		config := &Config{
			Paths:            []string{dir},
			MinimumFreeSpace: 1,
			Logger:           logrus.New(),
		}
		config.Logger.SetLevel(logrus.ErrorLevel)
		kv, err := Init(cryptInstance, config)
		require.NoError(b, err)
		defer kv.Close()

		// Pre-populate DB for read benchmark
		for i := range dataObjs {
			err := kv.WriteData(dataObjs[i])
			require.NoError(b, err)
		}

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			i := n % keyCount
			read, err := kv.ReadData(dataObjs[i].Key)
			require.NoError(b, err)
			require.Equal(b, dataObjs[i].Content, read.Content)
		}
	})

	b.Run("ListStoredData", func(b *testing.B) {
		dir := b.TempDir()
		cryptInstance := crypt.New()
		config := &Config{
			Paths:            []string{dir},
			MinimumFreeSpace: 1,
			Logger:           logrus.New(),
		}
		config.Logger.SetLevel(logrus.ErrorLevel)
		kv, err := Init(cryptInstance, config)
		require.NoError(b, err)
		defer kv.Close()

		// Pre-populate DB for navigation benchmark
		for i := range dataObjs {
			err := kv.WriteData(dataObjs[i])
			require.NoError(b, err)
		}

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			infos, err := kv.ListStoredData()
			require.NoError(b, err)
			if len(infos) < keyCount {
				b.Fatalf("expected at least %d keys, got %d", keyCount, len(infos))
			}
		}
	})
}
