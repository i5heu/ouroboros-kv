package ouroboroskv

import (
	"fmt"
	"log/slog"
	"os"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/pkg/hash"
	"github.com/i5heu/ouroboros-kv/internal/store"
	"github.com/i5heu/ouroboros-kv/pkg/config"
)

var log *slog.Logger

type Config = config.Config
type Data = store.Data
type DataInfo = store.DataInfo
type ValidationResult = store.ValidationResult
type Hash = hash.Hash

// Store is the public interface implemented by internal/store.KV.
// Every method below is implemented by the concrete KV type in internal/store.
type Store interface {
	// WriteData stores the provided Data and returns the computed data hash.
	WriteData(data Data) (Hash, error)

	// BatchWriteData stores multiple Data entries in a single batch and returns their keys.
	BatchWriteData(dataList []Data) ([]Hash, error)

	// ReadData reads a Data entry by its key and returns the Data.
	ReadData(key Hash) (Data, error)

	// BatchReadData reads multiple Data entries by their keys and returns the list of Data.
	BatchReadData(keys []Hash) ([]Data, error)

	// DeleteData removes the given data (by key) and associated slices/metadata.
	DeleteData(key Hash) error

	// DataExists returns whether a given key exists in the store.
	DataExists(key Hash) (bool, error)

	// GetChildren returns direct children of the given parent data key.
	GetChildren(parentKey Hash) ([]Hash, error)

	// GetAncestors returns all ancestors for a given leaf key (ordered from parent->root).
	GetAncestors(leafKey Hash) ([]Hash, error)

	// GetDescendants returns all descendants for a given root key.
	GetDescendants(rootKey Hash) ([]Hash, error)

	// GetParent returns the direct parent of a child key or a zero hash if none exists.
	GetParent(childKey Hash) (Hash, error)

	// GetRoots returns keys that are roots (no parent).
	GetRoots() ([]Hash, error)

	// ListKeys returns all keys stored in the database.
	ListKeys() ([]Hash, error)

	// ListRootKeys returns all root keys in the database.
	ListRootKeys() ([]Hash, error)

	// GetDataInfo returns a summarized DataInfo for the given key (metadata/summary).
	GetDataInfo(key Hash) (DataInfo, error)

	// ListStoredData returns DataInfo for every stored entry.
	ListStoredData() ([]DataInfo, error)

	// StartTransactionCounter starts the periodic transaction counter/monitor for given paths.
	StartTransactionCounter(paths []string, minimumFreeSpace int)

	// ValidateKey verifies the data consistency for the given key.
	ValidateKey(key Hash) error

	// ValidateAll performs full validation across the store and returns results per entry.
	ValidateAll() ([]ValidationResult, error)

	// Close closes the underlying store and releases resources.
	Close() error
}

var _ Store = (*store.KV)(nil)

func Init(crypt *crypt.Crypt, config *config.Config) (*store.KV, error) {
	if config.Logger == nil {
		config.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	}

	log = config.Logger

	err := config.CheckConfig()
	if err != nil {
		return nil, fmt.Errorf("error checking config for KeyValStore: %w", err)
	}

	kvImpl, err := store.Init(crypt, (*Config)(config))
	return kvImpl, err
}
