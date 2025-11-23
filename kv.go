package ouroboroskv

import (
	"fmt"
	"os"

	"log/slog"

	"github.com/dgraph-io/badger/v4"
	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
)

var log *slog.Logger

type KV struct {
	badgerDB     *badger.DB
	crypt        *crypt.Crypt
	config       Config
	readCounter  uint64
	writeCounter uint64
}

// Data is the user facing "value" or Data of ouroboros-kv, which contains the content and metadata.
type Data struct {
	Key hash.Hash // Key is derived from all fields except Children; must be zero when writing new data because it is generated from the content
	// Part of the key hash:
	Content        []byte    // The actual content of the data (stored encrypted)
	Parent         hash.Hash // Key of the parent value
	Created        int64     // Unix timestamp when the data was created
	RSDataSlices   uint8     // Number of Reed-Solomon data slices per stripe (RSDataSlices + RSParitySlices = total slices)
	RSParitySlices uint8     // Number of Reed-Solomon parity slices per stripe (RSDataSlices + RSParitySlices = total slices)
	// MimeType       string    // MIME type of the content
	// Not part of the key hash:
	Children []hash.Hash // Keys of the child values (stored dynamically and not part of the key hash)
	Meta     []byte      // Dynamic additional metadata (stored encrypted but not part of the key hash) for embeddings, labels, etc.
	Aliases  []hash.Hash // Aliases for the data (not part of the key hash)
	// UnencryptedSystemData []UnencryptedSystemData // Unencrypted system data associated (not part of the key hash and not encrypted but very fast to read and write) for stats, distribution info, etc.
}

// Unencrypted system data associated (not part of the key hash and not encrypted but very fast to read and write) for stats, distribution info, etc.
// type UnencryptedSystemData struct {
// 	Type string // Type of system data (e.g., "config", "stats")
// 	Data []byte // Raw system data payload
// }

// kvRef represents the `Data` structure stored in the key-value store, with chunk hashes instead of full content.
type kvRef struct {
	Key             hash.Hash
	ChunkHashes     []hash.Hash // Hash of deduplicated chunks
	MetaChunkHashes []hash.Hash // Hash of metadata chunks
	Parent          hash.Hash   // Key of the parent chunk
	Created         int64       // Unix timestamp when the data was created
	Aliases         []hash.Hash // Aliases for the data
}

// kvData represents the `Data` structure with full SliceRecords for decryption and reconstruction.
type kvData struct {
	Key             hash.Hash
	Slices          []SealedSlice // Content RSSlices
	ChunkHashes     []hash.Hash   // Order of content chunk hashes
	MetaSlices      []SealedSlice // Metadata RSSlices
	MetaChunkHashes []hash.Hash   // Order of metadata chunk hashes
	Parent          hash.Hash     // Key of the parent chunk
	Children        []hash.Hash   // Keys of the child chunks
	Created         int64         // Unix timestamp when the data was created
	Aliases         []hash.Hash   // Aliases for the data
}

// SealedSlice represents a single Reed-Solomon slice (data or parity) persisted in the key-value store.
type SealedSlice struct {
	ChunkHash       hash.Hash // Hash of the clear chunk produced by Buzhash
	SealedHash      hash.Hash // Hash of the sealed chunk (compressed + encrypted) prior to Reed-Solomon encoding
	RSDataSlices    uint8     // Number of data slices in the originating stripe
	RSParitySlices  uint8     // Number of parity slices in the originating stripe
	RSSliceIndex    uint8     // Index of the slice within the stripe (data slices precede parity slices)
	Size            uint64    // Size of this slice payload in bytes
	OriginalSize    uint64    // Size of the sealed chunk before Reed-Solomon encoding
	EncapsulatedKey []byte    // ML-KEM encapsulated secret for the sealed chunk
	Nonce           []byte    // AES-GCM nonce for encryption
	Payload         []byte    // Slice payload after Reed-Solomon encoding
}

func Init(crypt *crypt.Crypt, config *Config) (*KV, error) {
	if config.Logger == nil {
		config.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
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
		log.Error("failed to open badger DB", "error", err)
		return nil, err
	}

	err = displayDiskUsage(config.Paths)
	if err != nil {
		log.Error("failed to display disk usage", "error", err)
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
	k.badgerDB.RunValueLogGC(0.9) // Attempt to run value log GC before closing
	return k.badgerDB.Close()
}
