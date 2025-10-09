package ouroboroskv

import (
	"encoding/base64"
	"fmt"

	"github.com/i5heu/ouroboros-crypt/hash"
)

// ValidationResult captures the outcome of validating a single data entry.
type ValidationResult struct {
	Key       hash.Hash
	KeyBase64 string
	Err       error
}

// Passed reports whether the validation succeeded.
func (r ValidationResult) Passed() bool {
	return r.Err == nil
}

// ValidateKey verifies that the stored data associated with key still matches its hashes.
func (k *KV) ValidateKey(key hash.Hash) error {
	data, err := k.ReadData(key)
	if err != nil {
		return fmt.Errorf("failed to read data for validation: %w", err)
	}

	computed := hash.HashBytes(data.Content)
	if computed != key {
		return fmt.Errorf("content hash mismatch: expected %x, got %x", key, computed)
	}

	return nil
}

// ValidateAll iterates over all stored data and returns validation results per entry.
func (k *KV) ValidateAll() ([]ValidationResult, error) {
	keys, err := k.ListKeys()
	if err != nil {
		return nil, fmt.Errorf("failed to list keys for validation: %w", err)
	}

	results := make([]ValidationResult, 0, len(keys))
	for _, key := range keys {
		res := ValidationResult{
			Key:       key,
			KeyBase64: base64.StdEncoding.EncodeToString(key[:]),
		}
		if err := k.ValidateKey(key); err != nil {
			res.Err = err
		}
		results = append(results, res)
	}

	return results, nil
}
