package ouroboroskv

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckConfigCreatesDirectory(t *testing.T) {
	base := t.TempDir()
	missing := filepath.Join(base, "new-config-path")

	cfg := &Config{Paths: []string{missing}, MinimumFreeSpace: 0}
	require.NoError(t, cfg.checkConfig())

	info, err := os.Stat(missing)
	require.NoError(t, err)
	require.True(t, info.IsDir())
}
