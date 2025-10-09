package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCLIWorkflowEndToEnd(t *testing.T) {

	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	kv, err := initKV()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, kv.Close())
	})

	keyFile := filepath.Join(tempHome, ".ouroboros-kv", "ouroboros.key")
	if _, err := os.Stat(keyFile); err != nil {
		t.Fatalf("expected key file to be created: %v", err)
	}

	filePath := filepath.Join(tempHome, "sample.txt")
	originalContent := "integration cli payload " + time.Now().Format(time.RFC3339Nano)
	require.NoError(t, os.WriteFile(filePath, []byte(originalContent), 0o600))

	hashBase64, err := storeFile(kv, filePath)
	require.NoError(t, err)
	require.NotEmpty(t, hashBase64)

	resolved, err := resolveHashInput(kv, hashBase64[:8])
	require.NoError(t, err)
	exact, ok := decodeExactHash(hashBase64)
	require.True(t, ok)
	assert.Equal(t, resolved, exact)

	listOut, err := captureStdout(func() error { return listStoredData(kv) })
	require.NoError(t, err)
	assert.Contains(t, listOut, "sample.txt")
	assert.Contains(t, listOut, "Entries: 1")

	validateOut, err := captureStdout(func() error { return validateData(kv) })
	require.NoError(t, err)
	assert.Contains(t, validateOut, "Validated 1 entries")

	restoreOut, err := captureStdout(func() error { return restoreFile(kv, hashBase64) })
	require.NoError(t, err)
	assert.Equal(t, originalContent, strings.TrimSpace(restoreOut))

	require.NoError(t, deleteFile(kv, hashBase64))

	err = deleteFile(kv, hashBase64)
	assert.Error(t, err)

	emptyList, err := captureStdout(func() error { return listStoredData(kv) })
	require.NoError(t, err)
	assert.Contains(t, emptyList, "No data stored")

	assert.Error(t, deleteFile(kv, "not-a-hash"))

	assert.Equal(t, hashBase64[:8], shortenHash(hashBase64[:8]))
	shortened := shortenHash(hashBase64)
	assert.True(t, strings.HasSuffix(shortened, "..."))
	assert.LessOrEqual(t, len(shortened), 16)
	assert.Equal(t, "1.5 KB", formatBytes(1536))
	assert.Equal(t, "999 B", formatBytes(999))
}

func captureStdout(fn func() error) (string, error) {
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		return "", err
	}
	os.Stdout = w

	runErr := fn()
	_ = w.Close()
	os.Stdout = old

	var buf bytes.Buffer
	_, copyErr := io.Copy(&buf, r)
	_ = r.Close()

	if runErr != nil {
		return "", runErr
	}
	if copyErr != nil {
		return "", copyErr
	}

	return buf.String(), nil
}

func TestResolveHashInputErrors(t *testing.T) {

	tempHome := t.TempDir()
	t.Setenv("HOME", tempHome)

	kv, err := initKV()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, kv.Close()) })

	_, err = resolveHashInput(kv, "")
	assert.Error(t, err)

	fileA := filepath.Join(tempHome, "fileA.txt")
	fileB := filepath.Join(tempHome, "fileB.txt")
	require.NoError(t, os.WriteFile(fileA, []byte("A"), 0o600))
	require.NoError(t, os.WriteFile(fileB, []byte("B"), 0o600))

	hashA, err := storeFile(kv, fileA)
	require.NoError(t, err)
	hashB, err := storeFile(kv, fileB)
	require.NoError(t, err)

	prefix := hashA[:6]
	_, err = resolveHashInput(kv, prefix)
	assert.NoError(t, err)

	// Create ambiguous prefix by matching both hashes.
	ambiguous := commonPrefix(hashA, hashB)
	if len(ambiguous) >= 4 {
		_, err = resolveHashInput(kv, ambiguous)
		assert.Error(t, err)
	}
}

func commonPrefix(a, b string) string {
	max := len(a)
	if len(b) < max {
		max = len(b)
	}
	for i := 1; i <= max; i++ {
		if a[:i] != b[:i] {
			return a[:i-1]
		}
	}
	return a[:max]
}
