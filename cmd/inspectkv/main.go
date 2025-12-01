package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"

	crypt "github.com/i5heu/ouroboros-crypt"
	ouroboroskv "github.com/i5heu/ouroboros-kv"
)

func main() {
	path := flag.String("path", "", "path to the Ouroboros KV data directory")
	showKeys := flag.Bool("show-keys", false, "print relationship keys for manual inspection")
	prefix := flag.String("prefix", "parent", "relationship prefix to inspect (parent or child)")
	limit := flag.Int("limit", 20, "max number of keys to print when show-keys is enabled (0 = unlimited)")
	flag.Parse()

	if *path == "" {
		log.Fatal("-path is required")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := &ouroboroskv.Config{
		Paths:            []string{*path},
		MinimumFreeSpace: 0,
		Logger:           logger,
	}

	kv, err := ouroboroskv.Init(crypt.New(), cfg)
	if err != nil {
		log.Fatalf("failed to open store at %s: %v", *path, err)
	}
	defer kv.Close()

	parentCount, childCount, err := kv.InspectRelationshipCounts()
	if err != nil {
		log.Fatalf("failed to collect relationship counts: %v", err)
	}

	keys, err := kv.ListKeys()
	if err != nil {
		log.Fatalf("failed to list data keys: %v", err)
	}

	fmt.Printf("Store path: %s\n", *path)
	fmt.Printf("Data keys: %d\n", len(keys))
	fmt.Printf("Parent entries: %d\n", parentCount)
	fmt.Printf("Child entries: %d\n", childCount)

	if *showKeys {
		relationshipKeys, err := kv.InspectRelationshipKeys(*prefix, *limit)
		if err != nil {
			log.Fatalf("failed to list %s keys: %v", *prefix, err)
		}

		if *limit > 0 && len(relationshipKeys) == *limit {
			fmt.Printf("Listing first %d %s keys:\n", *limit, *prefix)
		} else {
			fmt.Printf("Listing %d %s keys:\n", len(relationshipKeys), *prefix)
		}

		if len(relationshipKeys) == 0 {
			fmt.Println("  (no entries)")
		}
		for _, key := range relationshipKeys {
			fmt.Printf("  %s\n", key)
		}
	}
}
