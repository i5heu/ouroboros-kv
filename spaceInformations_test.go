package ouroboroskv

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/shirou/gopsutil/disk"
)

func TestGetDeviceAndMountPoint_Success(t *testing.T) {
	partitions, err := disk.Partitions(true)
	if err != nil {
		t.Fatalf("disk.Partitions returned error: %v", err)
	}
	if len(partitions) == 0 {
		t.Skip("no partitions available on this system")
	}

	var chosen *disk.PartitionStat
	t.Log("Available partitions with mountpoints:")
	for i := range partitions {
		p := partitions[i]
		t.Log(p.Mountpoint)
		if p.Mountpoint != "" {
			chosen = &p
			break
		}
	}
	if chosen == nil {
		t.Skip("no partition with a mountpoint found")
	}

	path := chosen.Mountpoint
	mountPoint, device, err := getDeviceAndMountPoint(path)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if mountPoint != chosen.Mountpoint {
		t.Fatalf("expected mount point %q, got %q", chosen.Mountpoint, mountPoint)
	}
	if device != chosen.Device {
		t.Fatalf("expected device %q, got %q", chosen.Device, device)
	}
}

func TestGetDeviceAndMountPoint_NotFound(t *testing.T) {
	path := "/a987wgf9a8wgf/path/that/does/not/exist"
	_, _, err := getDeviceAndMountPoint(path)
	if err == nil {
		t.Fatalf("expected error for path %q, got nil", path)
	}
}

func TestGetDeviceAndMountPoint_RootHome(t *testing.T) {
	path := "/home"
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		t.Skipf("path %q does not exist on this system", path)
	} else if err != nil {
		t.Fatalf("failed to stat %q: %v", path, err)
	}

	_, _, err := getDeviceAndMountPoint(path)
	if err != nil {
		t.Fatalf("expected no error for path %q, got %v", path, err)
	}
}

func TestGetDeviceAndMountPoint_TempDir(t *testing.T) {
	temp := t.TempDir()

	partitions, err := disk.Partitions(true)
	if err != nil {
		t.Fatalf("disk.Partitions returned error: %v", err)
	}
	if len(partitions) == 0 {
		t.Skip("no partitions available on this system")
	}

	var expected *disk.PartitionStat
	for i := range partitions {
		p := partitions[i]
		if p.Mountpoint != "" && contains(temp, p.Mountpoint) {
			expected = &p
			break
		}
	}
	if expected == nil {
		t.Skipf("no partition found for temp dir %q", temp)
	}

	mountPoint, device, err := getDeviceAndMountPoint(temp)
	if err != nil {
		t.Fatalf("expected no error for temp dir %q, got: %v", temp, err)
	}
	if mountPoint != expected.Mountpoint {
		t.Fatalf("expected mount point %q, got %q", expected.Mountpoint, mountPoint)
	}
	if device != expected.Device {
		t.Fatalf("expected device %q, got %q", expected.Device, device)
	}
}

func TestGetDeviceAndMountPoint_TempDirNested(t *testing.T) {
	temp := t.TempDir()
	nested := filepath.Join(temp, "some", "nested", "path", "that", "does", "not", "exist")

	partitions, err := disk.Partitions(true)
	if err != nil {
		t.Fatalf("disk.Partitions returned error: %v", err)
	}
	if len(partitions) == 0 {
		t.Skip("no partitions available on this system")
	}

	var expected *disk.PartitionStat
	for i := range partitions {
		p := partitions[i]
		if p.Mountpoint != "" && contains(nested, p.Mountpoint) {
			expected = &p
			break
		}
	}
	if expected == nil {
		t.Skipf("no partition found for nested temp path %q", nested)
	}

	mountPoint, device, err := getDeviceAndMountPoint(nested)
	if err != nil {
		t.Fatalf("expected no error for nested temp path %q, got: %v", nested, err)
	}
	if mountPoint != expected.Mountpoint {
		t.Fatalf("expected mount point %q, got %q", expected.Mountpoint, mountPoint)
	}
	if device != expected.Device {
		t.Fatalf("expected device %q, got %q", expected.Device, device)
	}
}

func TestContainsEdgeCases(t *testing.T) {
	if contains("/alpha", "") {
		t.Fatalf("expected empty mountpoint to be false")
	}

	if !contains("/alpha", "/") {
		t.Fatalf("expected root mountpoint to match any path")
	}

	if !contains("/alpha", "/alpha/") {
		t.Fatalf("expected trailing slash mountpoint to match path")
	}

	if contains("/alpha", "/beta") {
		t.Fatalf("expected unrelated mountpoint to be false")
	}
}

func TestCalculateDirectorySizeMissingPath(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing", "subdir")
	if _, err := calculateDirectorySize(missing); err == nil {
		t.Fatalf("expected error when walking missing directory")
	}
}
