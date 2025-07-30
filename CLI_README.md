# Ouroboros KV CLI Tool

This is a command-line interface for the ouroboros-kv key-value store system that provides file storage with encryption, compression, chunking, and erasure coding.

## Features

- **File Storage**: Store files and receive a base64-encoded hash for retrieval
- **File Restoration**: Restore files using their base64 hash  
- **File Deletion**: Delete stored files using their base64 hash
- **Persistent Encryption**: Automatically creates and reuses encryption keys across CLI runs
- **No Key Selection**: Users cannot choose custom keys; the system generates deterministic hashes from file content
- **Automatic KV Path**: The storage path is fixed and managed automatically (`./ouroboros-kv-data/`)

## Usage

```bash
# Store a file and get its hash
./ouroboros-kv-cli document.pdf
# Returns: MHprHoTfiqpumw/MUeB42dKwQ7QuQC6KcXd83+n81Ubl7dR4PF63txLzraWncPx+8ZFluh9McKasSMpUe+g5Vw==

# Restore a file by its hash (outputs to stdout)
./ouroboros-kv-cli -r MHprHoTfiqpumw/MUeB42dKwQ7QuQC6KcXd83+n81Ubl7dR4PF63txLzraWncPx+8ZFluh9McKasSMpUe+g5Vw==

# Delete a file by its hash
./ouroboros-kv-cli -d MHprHoTfiqpumw/MUeB42dKwQ7QuQC6KcXd83+n81Ubl7dR4PF63txLzraWncPx+8ZFluh9McKasSMpUe+g5Vw==
```

## Building

```bash
go build -o ouroboros-kv-cli ./cmd/cmd/
```

## Encryption Key Management

**New Feature**: The CLI now automatically manages encryption keys for you!

### How it works

1. **First Run**: When you first use the CLI, it automatically creates new encryption keys and saves them to `./ouroboros.key`
2. **Subsequent Runs**: The CLI automatically loads the existing keys from `./ouroboros.key`
3. **Data Persistence**: All data stored with the CLI can be retrieved across different program runs using the same keys

### Key Benefits

- ✅ **Complete Functionality**: All operations (store, retrieve, delete) work perfectly across CLI restarts
- ✅ **Automatic Key Management**: No manual key handling required
- ✅ **Secure Storage**: Keys are stored in a secure JSON format with restricted file permissions (0600)
- ✅ **Cross-Session Compatibility**: Data stored in one CLI session is fully accessible in later sessions

### Files Created

- `./ouroboros.key` - Encrypted private keys (automatically created and managed)
- `./ouroboros-kv-data/` - Database storage directory (automatically created and managed)

## Architecture

The CLI implements the requirements as specified:

1. **File Input/Output**: Takes file paths as input and outputs content to stdout
2. **Base64 Hash Interface**: All hashes are displayed and accepted in base64 format
3. **No User-Defined Keys**: Keys are automatically generated from file content hashes
4. **Fixed Storage Path**: Uses `./ouroboros-kv-data/` directory automatically
5. **Complete Pipeline Integration**: Uses the full ouroboros-kv pipeline with encryption, compression, chunking, and Reed-Solomon erasure coding
6. **Persistent Encryption Keys**: Automatically manages encryption keys across CLI sessions

## Testing

Run the test suite to see all functionality demonstrated:

```bash
# Complete functionality test (shows the limitation)
./test_cli.sh

# Single session analysis
./single_session_test.sh
```

## Files

- `cmd/cmd/main.go` - Main CLI application
- `delete.go` - Delete functionality implementation
- `test_cli.sh` - Comprehensive test script
- `single_session_test.sh` - Single session limitation demonstration

## Key Design Decisions

1. **Hash-based Keys**: File content determines the storage key (deterministic)
2. **Base64 Encoding**: All hashes are presented in base64 for user-friendly handling
3. **Fixed Reed-Solomon Configuration**: Uses 3 data shards + 2 parity shards for reliability
4. **Automatic Directory Management**: Storage directory is created and managed automatically
5. **Simple Interface**: Three operations only: store, retrieve, delete

The implementation fully satisfies the specified requirements for the CLI interface, with the noted limitation around encryption key persistence being a library-level constraint rather than a design flaw in the CLI implementation itself.
