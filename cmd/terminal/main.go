package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	crypt "github.com/i5heu/ouroboros-crypt"
	"github.com/i5heu/ouroboros-crypt/hash"
	ouroboroskv "github.com/i5heu/ouroboros-kv"
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
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("failed to determine user home directory: %w", err)
	}

	kvDir := filepath.Join(homeDir, ".ouroboros-kv")
	if err := os.MkdirAll(kvDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create KV directory: %w", err)
	}

	keyFile := filepath.Join(kvDir, "ouroboros.key")

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
		if err := cryptInstance.Keys.SaveToFile(keyFile); err != nil {
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
		Logger:           slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	}

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

	metaPayload := struct {
		FileName      string `json:"fileName"`
		SavedAt       string `json:"savedAt"`
		FileSizeBytes int64  `json:"fileSizeBytes"`
	}{
		FileName:      filepath.Base(filePath),
		SavedAt:       time.Now().UTC().Format(time.RFC3339),
		FileSizeBytes: int64(len(content)),
	}

	metaBytes, err := json.Marshal(metaPayload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Create data structure (key will be generated by WriteData)
	data := ouroboroskv.Data{
		Content:                 content,
		MetaData:                metaBytes,
		Parent:                  hash.Hash{},   // Empty parent
		Children:                []hash.Hash{}, // No children
		ReedSolomonShards:       3,             // Default values for Reed-Solomon
		ReedSolomonParityShards: 2,
	}

	// Store in KV
	key, err := kv.WriteData(data)
	if err != nil {
		return "", fmt.Errorf("failed to write data to KV: %w", err)
	}

	// Convert hash to base64
	hashBase64 := base64.StdEncoding.EncodeToString(key[:])
	return hashBase64, nil
}

func deleteFile(kv *ouroboroskv.KV, hashInput string) error {
	key, err := resolveHashInput(kv, hashInput)
	if err != nil {
		return err
	}

	// Check if data exists
	exists, err := kv.DataExists(key)
	if err != nil {
		return fmt.Errorf("failed to check if data exists: %w", err)
	}

	if !exists {
		return fmt.Errorf("data with hash %s not found", hashInput)
	}

	// Delete from KV
	err = kv.DeleteData(key)
	if err != nil {
		return fmt.Errorf("failed to delete data: %w", err)
	}

	return nil
}

func restoreFile(kv *ouroboroskv.KV, hashInput string) error {
	key, err := resolveHashInput(kv, hashInput)
	if err != nil {
		return err
	}

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

	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "INDEX\tHASH (BASE64)\tFILENAME\tSIZE\tCONTENT CHUNKS\tMETADATA CHUNKS\tSAVED AT")

	var (
		totalContentChunks  int
		totalMetadataChunks int
		totalFileSize       uint64
	)

	for i, info := range dataInfos {
		fileName := "<unknown>"
		savedAt := "-"
		fileSize := info.ClearTextSize

		var metaPayload struct {
			FileName      string `json:"fileName"`
			SavedAt       string `json:"savedAt"`
			FileSizeBytes int64  `json:"fileSizeBytes"`
		}

		if len(info.MetaData) > 0 {
			if err := json.Unmarshal(info.MetaData, &metaPayload); err == nil {
				if metaPayload.FileName != "" {
					fileName = metaPayload.FileName
				}
				if metaPayload.SavedAt != "" {
					savedAt = metaPayload.SavedAt
				}
				if metaPayload.FileSizeBytes > 0 {
					fileSize = uint64(metaPayload.FileSizeBytes)
				}
			} else {
				var legacy map[string]string
				if err := json.Unmarshal(info.MetaData, &legacy); err == nil {
					if name := legacy["fileName"]; name != "" {
						fileName = name
					}
					if ts := legacy["savedAt"]; ts != "" {
						savedAt = ts
					}
				}
			}
		}

		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%d\t%d\t%s\n",
			i+1,
			shortenHash(info.KeyBase64),
			fileName,
			formatBytes(fileSize),
			info.NumChunks,
			info.MetaNumChunks,
			savedAt,
		)

		totalContentChunks += info.NumChunks
		totalMetadataChunks += info.MetaNumChunks
		totalFileSize += fileSize
	}

	if err := tw.Flush(); err != nil {
		return fmt.Errorf("failed to render table: %w", err)
	}

	fmt.Println()
	fmt.Printf("Summary:\n")
	fmt.Printf("  Entries: %d\n", len(dataInfos))
	fmt.Printf("  Total stored size: %s (%d bytes)\n", formatBytes(totalFileSize), totalFileSize)
	fmt.Printf("  Content chunks: %d\n", totalContentChunks)
	fmt.Printf("  Metadata chunks: %d\n", totalMetadataChunks)
	fmt.Printf("  All chunks: %d\n", totalContentChunks+totalMetadataChunks)

	return nil
}

func shortenHash(hashBase64 string) string {
	const maxLen = 16
	if len(hashBase64) <= maxLen {
		return hashBase64
	}
	return hashBase64[:maxLen-3] + "..."
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

type keyLister interface {
	ListKeys() ([]hash.Hash, error)
}

func resolveHashInput(provider keyLister, input string) (hash.Hash, error) {
	if key, ok := decodeExactHash(input); ok {
		return key, nil
	}

	keys, err := provider.ListKeys()
	if err != nil {
		return hash.Hash{}, fmt.Errorf("failed to list keys for hash resolution: %w", err)
	}

	return findHashByPrefix(keys, input)
}

func decodeExactHash(input string) (hash.Hash, bool) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return hash.Hash{}, false
	}

	paddingNeeded := len(trimmed) % 4
	if paddingNeeded != 0 {
		trimmed += strings.Repeat("=", 4-paddingNeeded)
	}

	decoded, err := base64.StdEncoding.DecodeString(trimmed)
	if err != nil || len(decoded) != len(hash.Hash{}) {
		return hash.Hash{}, false
	}

	var key hash.Hash
	copy(key[:], decoded)
	return key, true
}

func findHashByPrefix(keys []hash.Hash, prefix string) (hash.Hash, error) {
	if prefix == "" {
		return hash.Hash{}, fmt.Errorf("hash prefix cannot be empty")
	}

	var matches []hash.Hash

	for _, key := range keys {
		encoded := base64.StdEncoding.EncodeToString(key[:])
		if strings.HasPrefix(encoded, prefix) {
			matches = append(matches, key)
		}
	}

	switch len(matches) {
	case 0:
		return hash.Hash{}, fmt.Errorf("no data found matching hash prefix %q", prefix)
	case 1:
		return matches[0], nil
	default:
		return hash.Hash{}, fmt.Errorf("multiple entries match hash prefix %q; please provide more characters", prefix)
	}
}
