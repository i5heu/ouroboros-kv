package ouroboroskv

import (
	"fmt"

	"github.com/dgraph-io/badger/v4"
	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	"github.com/i5heu/ouroboros-kv/storage"

	"github.com/sirupsen/logrus"
)

var log *logrus.Logger

type KV struct {
	badgerDB     *badger.DB
	crypt        *crypt.Crypt
	config       Config
	readCounter  uint64
	writeCounter uint64
}

// Data is the user facing "value" or Data of ouroboros-kv, which contains the content and metadata.
type Data struct {
	Key                     hash.Hash   // Key of the content
	Content                 []byte      // The actual content of the data
	Parent                  hash.Hash   // Key of the parent value
	Children                []hash.Hash // Keys of the child values
	ReedSolomonShards       uint8       // Number of shards in Reed-Solomon coding (note that ReedSolomonShards + ReedSolomonParityShards is the total number of shards)
	ReedSolomonParityShards uint8       // Number of parity shards in Reed-Solomon coding (note that ReedSolomonShards + ReedSolomonParityShards is the total number of shards)
}

type kvDataLinked struct {
	Key      hash.Hash
	Shards   []storage.Shard // Hash of KvDataShards
	Parent   hash.Hash       // Key of the parent chunk
	Children []hash.Hash     // Keys of the child chunks
}

func Init(crypt *crypt.Crypt, config *Config) (*KV, error) {
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
	opts.ValueLogFileSize = 1024 * 1024 * 500 // Increase to 500MB per value log file
	opts.SyncWrites = false

	// Increase limits for handling very large files
	opts.BaseTableSize = 128 << 20    // 128MB (default is 2MB)
	opts.LevelSizeMultiplier = 10     // Default is 10
	opts.MaxLevels = 7                // Default is 7
	opts.ValueThreshold = 512         // Store values larger than 512B in value log (smaller threshold)
	opts.NumMemtables = 8             // Increase to 8 (default is 5)
	opts.MemTableSize = 128 << 20     // 128MB per memtable (default is 64MB)
	opts.NumLevelZeroTables = 8       // Increase to 8 (default is 5)
	opts.NumLevelZeroTablesStall = 20 // Increase to 20 (default is 15)

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

// Close closes the BadgerDB instance
func (k *KV) Close() error {
	return k.badgerDB.Close()
}
