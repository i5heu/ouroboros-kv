 [![Go Reference](https://pkg.go.dev/badge/github.com/i5heu/ouroboros-kv.svg)](https://pkg.go.dev/github.com/i5heu/ouroboros-kv) [![Tests](https://github.com/i5heu/ouroboros-kv/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/i5heu/ouroboros-kv/actions/workflows/go.yml) ![](https://github.com/i5heu/ouroboros-kv/blob/gh-pages/.badges/main/coverage.svg) [![Go Report Card](https://goreportcard.com/badge/github.com/i5heu/ouroboros-kv)](https://goreportcard.com/report/github.com/i5heu/ouroboros-kv) [![wakatime](https://wakatime.com/badge/user/5c9e80ad-4978-4730-9587-c758525cbd4e/project/d9390264-f9b4-4fab-8397-ae875fad5efa.svg)](https://wakatime.com/badge/user/5c9e80ad-4978-4730-9587-c758525cbd4e/project/d9390264-f9b4-4fab-8397-ae875fad5efa.svg)
 
<p align="center">
  <img src=".media/logo.jpeg"  width="300">
</p>


# ouroboros-kv

⚠️ Please note that this is still a prototype that has not yet been fully tested or optimized for production use or speed. There was no security audit performed. Version 2 will include these improvements. ⚠️

A key-value (read hash-data) store implementation in Go that provides advanced data processing features including chunking, erasure coding, encryption, compression, and hierarchical parent-child relationships between keys.

## Features

- **CRUD Operations**: Complete Create, Read, Update, Delete functionality
- **Content-Based Deduplication**: Automatic deduplication via Buzhash chunking
- **Data Compression**: Efficient compression using zstd
- **Strong Encryption**: AES-256-GCM encryption with ML-KEM1024 key encapsulation
- **Erasure Coding**: Reed-Solomon coding for data redundancy and recovery
- **Hierarchical Relationships**: Support for parent-child relationships between data entries
- **High Performance**: Optimized BadgerDB backend with large file handling
- **CLI Interface**: Command-line tool for file storage and retrieval

## Quick Start

### Installation

```bash
go get github.com/i5heu/ouroboros-kv
```

### Basic Usage

```go
package main

import (
    "fmt"
    crypt "github.com/i5heu/ouroboros-crypt"
    "github.com/i5heu/ouroboros-crypt/hash"
    ouroboroskv "github.com/i5heu/ouroboros-kv"
    "github.com/sirupsen/logrus"
)

func main() {
    // Initialize encryption
    cryptInstance := crypt.New()
    
    // Configure storage
    config := &ouroboroskv.Config{
        Paths:            []string{"/path/to/storage"},
        MinimumFreeSpace: 1, // 1GB minimum
        Logger:           logrus.New(),
    }
    
    // Initialize KV store
    kv, err := ouroboroskv.Init(cryptInstance, config)
    if err != nil {
        panic(err)
    }
    defer kv.Close()
    
    // Store data
    data := ouroboroskv.Data{
        Key:                     hash.HashString("my-key"),
        Content:                 []byte("Hello, World!"),
        ReedSolomonShards:       3,
        ReedSolomonParityShards: 2,
    }
    
    err = kv.WriteData(data)
    if err != nil {
        panic(err)
    }
    
    // Read data
    retrievedData, err := kv.ReadData(hash.HashString("my-key"))
    if err != nil {
        panic(err)
    }
    
    fmt.Printf("Retrieved: %s\n", string(retrievedData.Content))
}
```

### CLI Usage

Build and use the CLI tool:

```bash
# Build CLI
go build -o ouroboros-kv-cli ./cmd/cmd/

# Store a file
./ouroboros-kv-cli document.pdf
# Returns: base64-encoded hash

# Retrieve a file
./ouroboros-kv-cli -r <base64-hash> > restored-document.pdf

# Delete a file
./ouroboros-kv-cli -d <base64-hash>

# List all stored data
./ouroboros-kv-cli -ls
```

## Architecture

### Data Processing Pipeline

The data undergoes a processing pipeline during storage, with the reverse process during retrieval:

1. **Chunking**: Data is divided into predictable chunks using Buzhash (cyclic polynomial hashing) for content-based deduplication
2. **Compression**: Chunks are compressed using zstd for space efficiency
3. **Encryption**: Compressed data is encrypted with AES-256-GCM using ML-KEM1024 encapsulated keys
4. **Erasure Coding**: Encrypted data is encoded with Reed-Solomon coding for redundancy and fault tolerance

### Storage in BadgerDB

The system uses BadgerDB as the underlying storage engine with a two-tier key-value structure:

- **Metadata Layer**: Stores `kvDataHash` structures containing chunk references and relationships
- **Data Shards Layer**: Stores individual `kvDataShard` structures with processed content

#### Key Prefixes

- `meta:` - Metadata entries containing chunk hashes and relationships
- `chunk:` - Individual data shards after processing pipeline
- `parent:` - Parent-to-child relationship mappings
- `child:` - Child-to-parent relationship mappings

### Data Structures

#### Core Types

```go
type Data struct {
    Key                     hash.Hash   // Content-based key
    Content                 []byte      // Raw data content
    Parent                  hash.Hash   // Parent entry key
    Children                []hash.Hash // Child entry keys
    ReedSolomonShards       uint8       // Number of data shards
    ReedSolomonParityShards uint8       // Number of parity shards
}
```

#### Internal Storage Types

- `kvDataHash`: Metadata structure linking to data shards
- `kvDataShard`: Individual processed data chunks with encryption and encoding metadata

### Hierarchical Relationships

The system supports complex parent-child relationships:

- **Parent-Child Links**: Bidirectional relationships between entries
- **Ancestor/Descendant Queries**: Recursive relationship traversal
- **Root Detection**: Identification of top-level entries
- **Batch Operations**: Efficient bulk operations maintaining relationships

## API Reference

### Core Operations

- `WriteData(data Data) error` - Store data with processing pipeline
- `ReadData(key hash.Hash) (Data, error)` - Retrieve and reconstruct data
- `DeleteData(key hash.Hash) error` - Remove data and clean up references
- `DataExists(key hash.Hash) (bool, error)` - Check data existence
- `BatchWriteData(dataList []Data) error` - Bulk write operations

### Relationship Operations

- `GetChildren(key hash.Hash) ([]hash.Hash, error)` - Get direct children
- `GetParent(key hash.Hash) (hash.Hash, error)` - Get direct parent
- `GetDescendants(key hash.Hash) ([]hash.Hash, error)` - Get all descendants
- `GetAncestors(key hash.Hash) ([]hash.Hash, error)` - Get all ancestors
- `GetRoots() ([]hash.Hash, error)` - Get all root entries

### Information & Listing

- `ListKeys() ([]hash.Hash, error)` - List all stored keys
- `ListStoredData() ([]DataInfo, error)` - Get detailed information about all data
- `GetDataInfo(key hash.Hash) (DataInfo, error)` - Get detailed info for specific entry

## Configuration

### Config Structure

```go
type Config struct {
    Paths            []string        // Storage paths (currently uses first path only)
    MinimumFreeSpace int            // Minimum free space in GB
    Logger           *logrus.Logger  // Logger instance
}
```

### BadgerDB Optimization

The system is optimized for large file handling:

- 500MB value log files
- 128MB base table size and memtables
- Increased level multipliers and table counts

## Dependencies

- **BadgerDB**: High-performance key-value store
- **ouroboros-crypt**: Encryption and key management
- **Reed-Solomon**: Erasure coding implementation
- **zstd**: Compression algorithm
- **Protocol Buffers**: Serialization format

## Testing

Run the comprehensive test suite:

```bash
# Run all tests
go test ./...

# Run specific test categories
go test -run TestParentChildRelationships
go test -run TestComprehensiveRelationships
go test -run TestLargeFile
```

## License
ouroboros-kv (c) 2025 Mia Heidenstedt and contributors  
   
SPDX-License-Identifier: AGPL-3.0
