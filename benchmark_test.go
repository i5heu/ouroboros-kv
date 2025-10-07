package ouroboroskv

import (
	"math/rand"
	"os"
	"testing"

	"log/slog"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/stretchr/testify/require"
)

func BenchmarkWriteReadNavigation(b *testing.B) {
	const valueSize = 4096

	b.Run("WriteData", func(b *testing.B) {
		keyCount := b.N
		values := make([][]byte, keyCount)
		dataObjs := make([]Data, keyCount)
		for i := 0; i < keyCount; i++ {
			values[i] = make([]byte, valueSize)
			rand.Read(values[i])
			dataObjs[i] = Data{
				Content:                 values[i],
				Parent:                  hash.Hash{},
				Children:                []hash.Hash{},
				ReedSolomonShards:       2,
				ReedSolomonParityShards: 1,
			}
		}

		dir := b.TempDir()
		cryptInstance := crypt.New()
		config := &Config{
			Paths:            []string{dir},
			MinimumFreeSpace: 1,
			Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		}
		kv, err := Init(cryptInstance, config)
		require.NoError(b, err)
		defer kv.Close()

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			_, err := kv.WriteData(dataObjs[n])
			require.NoError(b, err)
		}
	})

	b.Run("ReadData", func(b *testing.B) {
		keyCount := b.N
		values := make([][]byte, keyCount)
		dataObjs := make([]Data, keyCount)
		generatedKeys := make([]hash.Hash, keyCount)
		for i := 0; i < keyCount; i++ {
			values[i] = make([]byte, valueSize)
			rand.Read(values[i])
			dataObjs[i] = Data{
				Content:                 values[i],
				Parent:                  hash.Hash{},
				Children:                []hash.Hash{},
				ReedSolomonShards:       2,
				ReedSolomonParityShards: 1,
			}
		}

		dir := b.TempDir()
		cryptInstance := crypt.New()
		config := &Config{
			Paths:            []string{dir},
			MinimumFreeSpace: 1,
			Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		}
		kv, err := Init(cryptInstance, config)
		require.NoError(b, err)
		defer kv.Close()

		// Pre-populate DB for read benchmark
		for i := range dataObjs {
			key, err := kv.WriteData(dataObjs[i])
			require.NoError(b, err)
			generatedKeys[i] = key
		}

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			read, err := kv.ReadData(generatedKeys[n])
			require.NoError(b, err)
			require.Equal(b, dataObjs[n].Content, read.Content)
		}
	})

	b.Run("ListKeys", func(b *testing.B) {
		keyCount := b.N
		values := make([][]byte, keyCount)
		dataObjs := make([]Data, keyCount)
		for i := 0; i < keyCount; i++ {
			values[i] = make([]byte, valueSize)
			rand.Read(values[i])
			dataObjs[i] = Data{
				Content:                 values[i],
				Parent:                  hash.Hash{},
				Children:                []hash.Hash{},
				ReedSolomonShards:       2,
				ReedSolomonParityShards: 1,
			}
		}

		dir := b.TempDir()
		cryptInstance := crypt.New()
		config := &Config{
			Paths:            []string{dir},
			MinimumFreeSpace: 1,
			Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		}
		kv, err := Init(cryptInstance, config)
		require.NoError(b, err)
		defer kv.Close()

		// Pre-populate DB for navigation benchmark
		for i := range dataObjs {
			_, err := kv.WriteData(dataObjs[i])
			require.NoError(b, err)
		}

		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			keys, err := kv.ListKeys()
			require.NoError(b, err)
			if len(keys) < keyCount {
				b.Fatalf("expected at least %d keys, got %d", keyCount, len(keys))
			}
		}
	})
}
