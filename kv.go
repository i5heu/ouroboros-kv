package ouroboroskv

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/hash"

	"github.com/sirupsen/logrus"
)

var log *logrus.Logger

type KV struct {
	badgerDB     *badger.DB
	crypt        *crypt.Crypt
	config       StoreConfig
	readCounter  uint64
	writeCounter uint64
}

type Data struct {
	Key      hash.Hash
	Content  []byte
	Parent   hash.Hash
	Children []hash.Hash
}

type KvData struct {
	Key         hash.Hash
	ChunkHashes []hash.Hash // Hash of KvContentChunks
	Parent      hash.Hash   // Key of the parent chunk
	Children    []hash.Hash // Keys of the child chunks
}

type KvContentChunk struct {
	ChunkHash               hash.Hash // After chunking and before compression, encryption and erasure coding
	ReedSolomonShards       uint8     // Number of shards in Reed-Solomon coding (note that ReedSolomonShards + ReedSolomonParityShards is the total number of shards)
	ReedSolomonParityShards uint8     // Number of parity shards in Reed-Solomon coding (note that ReedSolomonShards + ReedSolomonParityShards is the total number of shards)
	ReedSolomonIndex        uint8     // Index of the chunk in the Reed-Solomon coding (note that ReedSolomonShards + ReedSolomonParityShards is the total number of shards)
	Size                    uint64    // Size of the chunk in bytes
	ChunkContent            []byte    // Content of the chunk after compression, encryption and erasure coding
}

func Init(crypt *crypt.Crypt, config *StoreConfig) (*KV, error) {
	if config.Logger == nil {
		config.Logger = logrus.New()
	}

	log = config.Logger

	err := config.checkConfig()
	if err != nil {
		return nil, fmt.Errorf("error checking config for KeyValStore: %w", err)
	}

	opts := badger.DefaultOptions(config.Paths[0])
	opts.Logger = nil
	opts.ValueLogFileSize = 1024 * 1024 * 100 // Set max size of each value log file to 100MB
	opts.SyncWrites = false

	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	err = displayDiskUsage(config.Paths)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	return &KV{crypt: crypt,
		badgerDB:     db,
		config:       *config,
		readCounter:  0,
		writeCounter: 0,
	}, nil
}
