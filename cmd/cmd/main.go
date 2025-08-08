package main

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	ouroboroskv "github.com/i5heu/ouroboros-kv"
	"github.com/sirupsen/logrus"
)

const (
	USAGE = `Usage:
  %s <file>                    Store file and return base64 hash
  %s -d <base64_hash>          Delete data by base64 hash
  %s -r <base64_hash>          Restore data by base64 hash (outputs to stdout)
  %s -ls                       List all stored data with detailed information

Examples:
  %s document.pdf              # Store file, returns hash
  %s -d SGVsbG8gV29ybGQ=        # Delete data by hash
  %s -r SGVsbG8gV29ybGQ=        # Restore data by hash
  %s -ls                       # List all stored data

Note: 
  The CLI automatically creates and manages encryption keys in the current directory.
  All data stored with the CLI can be retrieved across different program runs using
  the same encryption keys.
`
)

func main() {
	progName := filepath.Base(os.Args[0])

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, USAGE, progName, progName, progName, progName, progName, progName, progName, progName)
		os.Exit(1)
	}

	// Initialize the KV store with fixed path
	kv, err := initKV()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing KV store: %v\n", err)
		os.Exit(1)
	}
	defer kv.Close()

	// Parse command line arguments
	switch os.Args[1] {
	case "-d":
		if len(os.Args) != 3 {
			fmt.Fprintf(os.Stderr, "Error: -d requires a base64 hash argument\n")
			fmt.Fprintf(os.Stderr, USAGE, progName, progName, progName, progName, progName, progName, progName, progName)
			os.Exit(1)
		}
		err := deleteFile(kv, os.Args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error deleting file: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("File deleted successfully")

	case "-r":
		if len(os.Args) != 3 {
			fmt.Fprintf(os.Stderr, "Error: -r requires a base64 hash argument\n")
			fmt.Fprintf(os.Stderr, USAGE, progName, progName, progName, progName, progName, progName, progName, progName)
			os.Exit(1)
		}
		err := restoreFile(kv, os.Args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error restoring file: %v\n", err)
			os.Exit(1)
		}

	case "-ls":
		if len(os.Args) != 2 {
			fmt.Fprintf(os.Stderr, "Error: -ls does not take any arguments\n")
			fmt.Fprintf(os.Stderr, USAGE, progName, progName, progName, progName, progName, progName, progName, progName)
			os.Exit(1)
		}
		err := listStoredData(kv)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error listing stored data: %v\n", err)
			os.Exit(1)
		}

	default:
		// Store file
		if len(os.Args) != 2 {
			fmt.Fprintf(os.Stderr, "Error: Too many arguments for store operation\n")
			fmt.Fprintf(os.Stderr, USAGE, progName, progName, progName, progName, progName, progName, progName, progName)
			os.Exit(1)
		}

		hashBase64, err := storeFile(kv, os.Args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error storing file: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(hashBase64)
	}
}

func initKV() (*ouroboroskv.KV, error) {
	// Get current working directory and create an absolute path
	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("failed to get current directory: %w", err)
	}

	// Create a fixed directory for the KV store with absolute path
	kvDir := filepath.Join(cwd, "ouroboros-kv-data")
	err = os.MkdirAll(kvDir, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create KV directory: %w", err)
	}

	// Key file path
	keyFile := filepath.Join(cwd, "ouroboros.key")

	// Initialize crypto - try to load from file first, create new if not found
	var cryptInstance *crypt.Crypt
	if _, err := os.Stat(keyFile); err == nil {
		// Key file exists, load it
		cryptInstance, err = crypt.NewFromFile(keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load crypto keys from %s: %w", keyFile, err)
		}
	} else if os.IsNotExist(err) {
		// Key file doesn't exist, create new keys
		cryptInstance = crypt.New()

		// Save the new keys to file
		err = cryptInstance.Keys.SaveToFile(keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to save crypto keys to %s: %w", keyFile, err)
		}
		fmt.Fprintf(os.Stderr, "Created new encryption keys and saved to %s\n", keyFile)
	} else {
		return nil, fmt.Errorf("failed to check key file %s: %w", keyFile, err)
	}

	// Create config with absolute path
	config := &ouroboroskv.Config{
		Paths:            []string{kvDir},
		MinimumFreeSpace: 1, // 1GB minimum
		Logger:           logrus.New(),
	}

	// Set log level to error to reduce noise further
	config.Logger.SetLevel(logrus.ErrorLevel)

	// Initialize KV
	kv, err := ouroboroskv.Init(cryptInstance, config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize KV: %w", err)
	}

	return kv, nil
}

func storeFile(kv *ouroboroskv.KV, filePath string) (string, error) {
	// Read file content
	content, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	// Create data structure
	// The key will be generated from the content hash
	contentHash := hash.HashBytes(content)

	data := ouroboroskv.Data{
		Key:                     contentHash,
		Content:                 content,
		Parent:                  hash.Hash{},   // Empty parent
		Children:                []hash.Hash{}, // No children
		ReedSolomonShards:       3,             // Default values for Reed-Solomon
		ReedSolomonParityShards: 2,
	}

	// Store in KV
	err = kv.WriteData(data)
	if err != nil {
		return "", fmt.Errorf("failed to write data to KV: %w", err)
	}

	// Convert hash to base64
	hashBase64 := base64.StdEncoding.EncodeToString(contentHash[:])
	return hashBase64, nil
}

func deleteFile(kv *ouroboroskv.KV, hashBase64 string) error {
	// Decode base64 hash
	hashBytes, err := base64.StdEncoding.DecodeString(hashBase64)
	if err != nil {
		return fmt.Errorf("invalid base64 hash: %w", err)
	}

	// Convert to hash.Hash
	if len(hashBytes) != 64 {
		return fmt.Errorf("invalid hash length: expected 64 bytes, got %d", len(hashBytes))
	}

	var key hash.Hash
	copy(key[:], hashBytes)

	// Check if data exists
	exists, err := kv.DataExists(key)
	if err != nil {
		return fmt.Errorf("failed to check if data exists: %w", err)
	}

	if !exists {
		return fmt.Errorf("data with hash %s not found", hashBase64)
	}

	// Delete from KV
	err = kv.DeleteData(key)
	if err != nil {
		return fmt.Errorf("failed to delete data: %w", err)
	}

	return nil
}

func restoreFile(kv *ouroboroskv.KV, hashBase64 string) error {
	// Decode base64 hash
	hashBytes, err := base64.StdEncoding.DecodeString(hashBase64)
	if err != nil {
		return fmt.Errorf("invalid base64 hash: %w", err)
	}

	// Convert to hash.Hash
	if len(hashBytes) != 64 {
		return fmt.Errorf("invalid hash length: expected 64 bytes, got %d", len(hashBytes))
	}

	var key hash.Hash
	copy(key[:], hashBytes)

	// Read from KV
	data, err := kv.ReadData(key)
	if err != nil {
		return fmt.Errorf("failed to read data: %w", err)
	}

	// Output content to stdout
	_, err = os.Stdout.Write(data.Content)
	if err != nil {
		return fmt.Errorf("failed to write to stdout: %w", err)
	}

	return nil
}

func listStoredData(kv *ouroboroskv.KV) error {
	// Get detailed information about all stored data
	dataInfos, err := kv.ListStoredData()
	if err != nil {
		return fmt.Errorf("failed to list stored data: %w", err)
	}

	if len(dataInfos) == 0 {
		fmt.Println("No data stored in the database.")
		return nil
	}

	fmt.Printf("Found %d stored data entries:\n\n", len(dataInfos))
	fmt.Println("=" + fmt.Sprintf("%0*s", 80, "="))

	for i, info := range dataInfos {
		fmt.Printf("Entry %d:\n", i+1)
		fmt.Print(info.FormatDataInfo())
		if i < len(dataInfos)-1 {
			fmt.Printf("%s\n", fmt.Sprintf("%0*s", 80, "-"))
		}
	}

	// Summary statistics
	var totalClearSize, totalStorageSize uint64
	var totalChunks, totalShards int

	for _, info := range dataInfos {
		totalClearSize += info.ClearTextSize
		totalStorageSize += info.StorageSize
		totalChunks += info.NumChunks
		totalShards += info.NumShards
	}

	fmt.Println("=" + fmt.Sprintf("%0*s", 80, "="))
	fmt.Printf("SUMMARY:\n")
	fmt.Printf("Total Entries: %d\n", len(dataInfos))
	fmt.Printf("Total Clear Text Size: %s (%d bytes)\n", formatBytes(totalClearSize), totalClearSize)
	fmt.Printf("Total Storage Size: %s (%d bytes)\n", formatBytes(totalStorageSize), totalStorageSize)
	fmt.Printf("Overall Compression Ratio: %.2fx\n", float64(totalStorageSize)/float64(totalClearSize))
	fmt.Printf("Total Chunks: %d, Total Shards: %d\n", totalChunks, totalShards)

	return nil
}

// formatBytes returns a human-readable byte size (duplicate of the one in list.go for CLI use)
func formatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
